"""
Importer for Wegmans purchases/orders (in-store/online-instacart) registered to your Wegmans account. 

Expects a single file per purchase or order.

Purchases/orders can be obtained once logged in using the APIs:

Listing orders/purchases:
https://shop.wegmans.com/api/v2/orders
https://shop.wegmans.com/api/v2/purchases

Fetching individual orders/purchases:

https://shop.wegmans.com/api/v2/orders/orderID
https://shop.wegmans.com/api/v2/purchases/purchaseID

I haven't (yet) uploaded the download code I have for these, but contact me and I'll be happy to share.

CAVEATS:

Ths lumps every item under category. I initially had each item as a separate posting in the transaction,
but with grocery orders having potentially hundreds of items, this resulted in beancount-import becoming 
unusably slow (it seems like the matching algorithm is O(n^2) maybe O(n!) - as it looks through a lot of
permutations of postings trying to match). There may be a better way to handle the importer and still have
each item as a separate posting. For my needs, grouping by category works - I mainly wanted to split out
some simple things like Food/Alcohol/Household/Personal Care.

Kombucha is a specal case (keyword) because the category for Kombucha in Wegmans's system is "Dairy"...

No test cases!

The format of orders/purchases is CLOSE but not quite the same, so this code has some special-case handling.

At some point in 2019 the Wegmans system switched from storing sales tax in the "items sub total" to separating
it out, but there's no way to tell when that happened. So some orders in my history double-count the tax, don't
balance, and have to be manually adjusted.

Account names are hard-coded!
"""

from typing import List, Union, Optional, Set, Dict
import json
import datetime
import collections
import re
import os
import numbers
import decimal

import hashlib

from beancount.core.data import Transaction, Posting, Balance, Price, EMPTY_SET
from beancount.core.amount import Amount
from beancount.core.position import get_position, Position, Cost
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from collections import defaultdict

from . import link_based_source, description_based_source, Source
from . import ImportResult, SourceResults
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

date_format = '%Y-%m-%d'

def date( val: str ):
    if not val: return None
    try:
        return datetime.datetime.strptime(val, date_format).date()
    except Exception as e:
        raise RuntimeError('Invalid date: %r' % val) from e

def amt( val: str ):
    if not val: return None
    # str() to lock in the decimal places parsed from the JSON (double value),
    # otherwise, calling D() directly on the amount from the JSON results in
    # floating point precision issues
    return Amount( round(D(str(val)),2), "USD" )


def load_transactions(dir: str, currency: str = 'USD') -> List[Dict]:
    try:
        entries = []
        for filename in os.listdir(dir):
            if not filename.endswith('.json'): continue
            filename = os.path.join(dir, filename)

            filename = os.path.abspath(filename)
            with open(filename, 'r', encoding='utf-8', newline='') as f:
                data = json.load(f, parse_float=decimal.Decimal, parse_int=decimal.Decimal)
                data['filename'] = filename

                entries.append( data )

        return entries

    except Exception as e:
        raise RuntimeError('Wegmans JSON file has incorrect format', filename) from e

def get_info(raw_entry: Dict) -> dict:
    return dict(
        type='application/json',
        filename=raw_entry['filename'],
        line=raw_entry['line'],
    )

def _make_import_result(wegmans_entry: Dict) -> ImportResult:
    meta = collections.OrderedDict()
    meta['wegmans_order_id']         = wegmans_entry['id']
    meta['wegmans_timestamp']        = wegmans_entry['timestamp']
    meta['wegmans_fulfillment_date'] = wegmans_entry.get('fulfillment_date', None)
    meta['wegmans_store']            = wegmans_entry['store']['name']

    postings = []

    # TODO: a lot more "final_totals" to include if we want
    charge_meta: collections.OrderedDict[str,str] = collections.OrderedDict()
    if 'payment_instruments' in wegmans_entry:
        charge_meta['wegmans_last_four'] = ", ".join( [ p['last_four_digits'] for p in wegmans_entry['payment_instruments'] ] )
    charge_meta['wegmans_sales_tax']     = wegmans_entry['final_totals'].get('tax_total', D(0))
    charge_meta['wegmans_tip']           = wegmans_entry['final_totals'].get('tip_total', D(0))
    charge_meta['wegmans_refund']        = wegmans_entry['final_totals'].get('refund_total', D(0))
    charge_meta['wegmans_pre_discount_product_total'] = wegmans_entry['final_totals'].get('pre_discount_product_total', D(0))
    charge_meta['wegmans_product_total'] = wegmans_entry['final_totals'].get('product_total', D(0))
    charge_meta['wegmans_total']         = wegmans_entry['final_totals'].get('total', D(0))
    
    grouped = defaultdict(lambda: [])

    item_total = D(0)
    for item in wegmans_entry.get('items', wegmans_entry.get('order_items')):
        if 'status' in item and item['status'] == 'removed':
            if 'child_order_item' in item and item['child_order_item'] is not None:
                item = item['child_order_item']
            else:
                continue # no replacement

        item_total += item['sub_total']

        if 'Kombucha' in item['store_product']['name']:
            cathash = 'Kombucha'
        else:
            all_categories = ", ".join( [c['name'] for c in item['store_product']['categories'][0:2]] )
            cathash = hashlib.md5(all_categories.encode('utf-8')).hexdigest()
        grouped[ cathash ].append(item)

    charge_total = wegmans_entry['final_totals']['total']

    postings.append(
        Posting(
                account='Expenses:FIXME:Charge',
                # the "totals" gets the sales tax added a second time and isn't the amount charged
                # product_total seems to have had the sales tax added already
                units=Amount(-1*charge_total, 'USD'),
                cost=None,
                price=None,
                flag=None,
                meta=charge_meta,
            )
    )

    tax_total = wegmans_entry['final_totals'].get('tax_total', D(0))
    tax_meta = collections.OrderedDict()
    tax_meta['wegmans_sales_tax'] = tax_total
    postings.append(
        Posting(
                account='Expenses:FIXME:Tax',
                units=Amount(tax_total, 'USD'),
                cost=None,
                price=None,
                flag=None,
                meta=tax_meta,
        )
    )

    if (charge_total) != (item_total+tax_total):
        discrep_meta = collections.OrderedDict()
        discrep_meta['wegmans_comment'] = 'Unknown discrepancy'
        postings.append(
            Posting(
                    account='Expenses:Food:Groceries',
                    units=Amount(charge_total-(item_total+tax_total), 'USD'),
                    cost=None,
                    price=None,
                    flag=None,
                    meta=discrep_meta,
            )
        )


    for cathash in grouped.keys():
        items = grouped[ cathash ] 

        item_meta = collections.OrderedDict()

        cat_total = D(0)

        for i,item in enumerate(items):
            cat_total += item['sub_total']

            if i == 0:
                top_categories = ", ".join( [c['name'] for c in item['store_product']['categories'][0:2]] )
                item_meta['wegmans_category'] = top_categories

            all_categories = ", ".join( [c['name'] for c in item['store_product']['categories'] ] )
            item_meta['wegmans_item_%02d_product_name' % i ] = '%s: %s / %s | %s @ %s = %s' % (
                                                                  all_categories,
                                                                  item['store_product']['brand_name'],
                                                                  item['store_product']['name'],
                                                                  item.get('actual_quantity', item.get('quantity')),
                                                                  item['store_product']['base_price'],
                                                                  item['sub_total']
            )     


        postings.append( Posting(
                #account='Expenses:FIXME:Groceries' + ,
                account='Expenses:FIXME:%s' % cathash,
                units=Amount(cat_total, 'USD'),
                cost=None,
                price=None,
                flag=None,
                meta=item_meta
            )
        )


    # for item in wegmans_entry.get('items', wegmans_entry.get('order_items')):        
    #     item_meta = collections.OrderedDict()
    #     item_meta['wegmans_product_name'] = item['store_product']['name']
    #     item_meta['wegmans_brand_name'  ] = item['store_product']['brand_name']
    #     item_meta['wegmans_unit_price'  ] = item['store_product']['base_price']
    #     item_meta['wegmans_quantity'    ] = item.get('actual_quantity', item.get('quantity'))
    #     item_meta['wegmans_categories'  ] = ", ".join( [c['name'] for c in item['store_product']['categories']] )
    #     item_meta['wegmans_sub_total'   ] = item['sub_total']

    #     postings.append( Posting(
    #             #account='Expenses:FIXME:Groceries' + hashlib.md5(item['store_product']['categories'][0]['name'].encode('utf-8')).hexdigest(),
    #             account='Expenses:Food:Groceries',
    #             units=Amount(item['sub_total'], 'USD'),
    #             cost=None,
    #             price=None,
    #             flag=None,
    #             meta=item_meta
    #         )
    #     )

    date = wegmans_entry['timestamp']
    date = datetime.datetime.fromisoformat( date ).date()

    transaction = Transaction(
        meta      = meta,
        date      = date,
        flag      = FLAG_OKAY,
        payee     = "Wegmans",
        narration = None,
        tags      = EMPTY_SET,
        links     = EMPTY_SET,
        postings=postings
    )

    return ImportResult( date=date, info=None, entries=[transaction])


def _get_existing_transaction_ids( all_entries ):
    ret = set()

    for entry in all_entries:
        if not isinstance(entry, Transaction): continue

        tid = entry.meta.get('wegmans_order_id')
        if tid: ret.add(tid)

        for posting in entry.postings:
            tid = posting.meta.get('wegmans_order_id')
            if tid: ret.add(tid)
        
    return ret

class WegmansSource(link_based_source.LinkBasedSource, Source):
    def __init__(self,
                 dir: str,
                 account_mappings: Dict[str,str] = {},
                 **kwargs) -> None:
        super().__init__(link_prefix='wegmans.', **kwargs)

        self.log_status('Wegmans: loading all orders from %s' % dir)
        self.entries = load_transactions(dir=dir)
        self.log_status("Loaded %d entries from: %s" % ( len(self.entries), dir ) )

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        new_tids      = set([ e['id'] for e in self.entries])
        existing_tids = _get_existing_transaction_ids( journal.all_entries )

        for entry in self.entries:
            if entry.get('status') == 'cancelled':
                self.log_status("Skipping cancelled order: %s" % entry['id'])
                continue
            if entry['id'] in existing_tids: 
                self.log_status("Skipping existing order: %s" % entry['id'])
                continue
            self.log_status("Processing order: %s" % entry['id'])

            result = _make_import_result(entry)
            results.add_pending_entry(result)

        results.add_account("Liabilities:Wegmans")
        # for account in self.account_mappings.values():
        #     results.add_account(account)
        # for account in Wegmans_accounts.keys():
        #     results.add_account(account)

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting):
        ret = {}
        def maybe_add_key(key):
            x = posting.meta.get(key)
            if x is not None:
                ret[key] = x
        
        # maybe_add_key('name')
        # maybe_add_key('account_id')
        # maybe_add_key('categories')

        return ret

    def is_posting_cleared(self, posting: Posting):
        if not posting.meta: return False
        pending = posting.meta.get('pending', 0)
        return not pending

    @property
    def name(self):
        return 'Wegmans'

def load(spec, log_status):
    return WegmansSource(log_status=log_status, **spec)
