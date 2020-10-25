"""
Handles importing Fifth Third bank mortgage transactions.

I don't use this actively anymore (I have closed that particular account), so I'm not sure
it makes sense to merge into the main beancount-import branch, but someone else might benefit
from it.

I didn't build an automated importer, but it would be really easy to build one. The API
in question is:

https://onlinebanking.53.com/mobile/v1/services/account/transaction/history?accountId=YOUR-ACCOUNT-UUID&includeTransFilters=false

Which returns JSON like this:

{"status":"SUCCESS","transactions":[
    {"id":"UUID1","transactionDate":"2020-08-01","postDate":"2020-08-27","amount":123.00,"description":"ESC DISB - CUSTOMER","creditDebitType":"C","status":"POSTED","transactionCode":"5850","imageAvailable":false,"escrowAmount":123.00},
    {"id":"UUID2","transactionDate":"2020-08-01","postDate":"2020-08-27","description":"BALANCE AFTER","creditDebitType":"B","status":"POSTED","transactionCode":"9999","imageAvailable":false}
]}

This importer expects those transactions to be extracted and stored in JSONL files (one line per transactoin object). 

e.g. 

curl API | jq '.transactions[]' -c > data/fifththird/transactions.jsonl

transactions.jsonl:
{"id":"UUID1","transactionDate":"2020-08-01","postDate":"2020-08-27","amount":123.00,"description":"ESC DISB - CUSTOMER","creditDebitType":"C","status":"POSTED","transactionCode":"5850","imageAvailable":false,"escrowAmount":123.00},
{"id":"UUID2","transactionDate":"2020-08-01","postDate":"2020-08-27","description":"BALANCE AFTER","creditDebitType":"B","status":"POSTED","transactionCode":"9999","imageAvailable":false}

CAVEATS:

The above transaction IDs seem to be randomly generated each session, so you can't use "id" to de-dupe data. Some combination of other
fields (postDate+amount+transactionCode+description) should be fine for de-duping if you automate the download process, as I don't imagine
they ever change a record once it's posted to your account.

Account names are hard-coded in this source.
"""

from typing import List, Union, Optional, Set
import json
import datetime
import collections
import re
import os
import numbers

from beancount.core.data import Transaction, Posting, Balance, EMPTY_SET
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from . import link_based_source, description_based_source, Source
from . import ImportResult, SourceResults
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

# account may be either the mint_id or the journal account name
FifthThirdEntry = collections.namedtuple(
    'FifthThirdEntry',
    [
        'transaction_date',
        'posting_date',
        'transaction_id', 
        'amount',
        'description',
        'credit_debit_type',
        'transaction_code',
        'principal_amount',
        'escrow_amount',
        'interest_amount',
        'other_amount',
        'status',
        'additional_info',
        'filename',
        'line',
    ]
)

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


def load_transactions(filename: str, currency: str = 'USD') -> List[FifthThirdEntry]:
    try:
        entries = []
        filename = os.path.abspath(filename)
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            lno = 0
            for line in f:
                lno += 1
                data = json.loads(line)

                entries.append(
                    FifthThirdEntry(
                        transaction_date       = date( data.get('transactionDate') ),
                        posting_date           = date( data.get('postDate') ),
                        transaction_id         = data.get('id') ,
                        amount                 = amt(data.get('amount')),
                        description            = data.get('description'),
                        credit_debit_type      = data.get('creditDebitType'),
                        transaction_code       = data.get('transactionCode'),
                        principal_amount       = amt(data.get('principalAmount')),
                        escrow_amount          = amt(data.get('escrowAmount')),
                        interest_amount        = amt(data.get('interestAmount')),
                        other_amount           = amt(data.get('otherAmount')),
                        additional_info        = data.get('additionalInfo'),
                        status                 = data.get('status'),
                        line                   = lno,
                        filename               = filename
                    )
                )
        entries.reverse()
        entries.sort(key=lambda x: x.posting_date)  # sort by date
        return entries

    except Exception as e:
        print
        raise RuntimeError('FifthThird JSON-L file has incorrect format', filename) from e

def get_info(raw_entry: FifthThirdEntry) -> dict:
    return dict(
        type='application/json',
        filename=raw_entry.filename,
        line=raw_entry.line,
    )

def _make_import_result(fifththird_entry: FifthThirdEntry) -> ImportResult:
    meta = collections.OrderedDict()
    for field,value in fifththird_entry._asdict().items():
        if field == "filename": continue
        if field == "line": continue
        if value is not None:
            meta["fifththird_%s" % field] = value

    transaction_ids = [ 'fifththird.%s' % fifththird_entry.transaction_id ]

    # balance assertion
    if fifththird_entry.transaction_code == "9999":
        # set the date one day forward to account for the fact beancount
        # orders by date, only, so these balance assertions can happen before
        bal_date= ( fifththird_entry.posting_date+datetime.timedelta(days=1) )

        entries = []
        if fifththird_entry.principal_amount:
            entries.append(
                Balance(
                    account='Liabilities:Mortgage:FifthThird',
                    date=bal_date,
                    amount=Amount(-1*fifththird_entry.principal_amount.number, fifththird_entry.principal_amount.currency),
                    meta=meta,
                    tolerance=None,
                    diff_amount=None
                )
            )
        if fifththird_entry.escrow_amount:
            entries.append(
                Balance(
                    account='Assets:FifthThird:Escrow',
                    date=bal_date,
                    amount=fifththird_entry.escrow_amount,
                    meta=meta,
                    tolerance=None,
                    diff_amount=None
                )
            )


        return ImportResult(
            date=bal_date,
            info=get_info(fifththird_entry),
            entries=entries,
        )
   
    # escrow withdraw, payment to third party
    elif fifththird_entry.transaction_code == "5850":
        src_posting = Posting(
                account='Assets:FifthThird:Escrow',
                units=-fifththird_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=meta,
            )
        
        payee   = 'Escrow Payment - '
        account = 'Expenses:FIXME' 
        if 'HAZ INS' in fifththird_entry.description:
            payee   = payee + "Homeowner's Insurance"
            account = 'Expenses:House:Insurance'
        elif 'TAXES' in fifththird_entry.description:
            payee = payee + "Taxes"
            account = 'Expenses:House:Taxes'
        else:
            payee = payee + fifththird_entry.description

        dst_posting = Posting( account=account,  units=fifththird_entry.escrow_amount, price=None, cost=None, flag=None, meta=None )

        transaction = Transaction(
            meta      = None,
            date      = fifththird_entry.posting_date,
            flag      = FLAG_OKAY,
            payee     = payee,
            narration = fifththird_entry.description,
            tags      = EMPTY_SET,
            links     = EMPTY_SET,
            postings  = [ src_posting, dst_posting ]
        )

        return ImportResult(
            date=fifththird_entry.posting_date, info=get_info(fifththird_entry), entries=[transaction])

    
    else:
        src_posting = Posting(
                account='Assets:FifthThird:Payment',
                units=-fifththird_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=meta,
            )

        payee = 'Fifth Third Mortgage'
        dst_postings = []

        if fifththird_entry.principal_amount: dst_postings.append( Posting( account='Liabilities:Mortgage:FifthThird',  units=fifththird_entry.principal_amount, price=None, cost=None, flag=None, meta=None ))
        if fifththird_entry.interest_amount:  dst_postings.append( Posting( account='Expenses:House:Mortgage:Interest', units=fifththird_entry.interest_amount , price=None, cost=None, flag=None, meta=None ))
        if fifththird_entry.escrow_amount:    dst_postings.append( Posting( account='Assets:FifthThird:Escrow',         units=fifththird_entry.escrow_amount   , price=None, cost=None, flag=None, meta=None ))
        if fifththird_entry.other_amount:     dst_postings.append( Posting( account='Expenses:House:Mortgage:Other',    units=fifththird_entry.other_amount    , price=None, cost=None, flag=None, meta=None ))

        transaction = Transaction(
            meta      = None,
            date      = fifththird_entry.posting_date,
            flag      = FLAG_OKAY,
            payee     = payee,
            narration = fifththird_entry.description,
            tags      = EMPTY_SET,
            links     = EMPTY_SET,
            postings=[ src_posting ] + dst_postings
        )

        return ImportResult(
            date=fifththird_entry.posting_date, info=get_info(fifththird_entry), entries=[transaction])


# Previously, this would be based on the links concept, but I'm not sure what value links have
# over just using the metadata property. One definite disadvantage of links is they pollute the
# Fava web interface search/filter drop-down.
#
# Searches through all existing Transactions and pulls out the existing `transaction_id`
# values.
# 
# OLD approach: self.get_entries_with_link( all_entries=journal.all_entries, results=results, valid_links=transaction_ids) 
def _get_existing_transaction_ids( all_entries ):
    ret = set()

    for entry in all_entries:
        if not isinstance(entry, Transaction): continue

        tid = entry.meta.get('fifththird_transaction_id')
        if tid: 
            print("Entry has meta: %s" % tid)
            ret.add(tid)

        for posting in entry.postings:
            tid = posting.meta.get('fifththird_transaction_id')
            if tid: ret.add(tid)
        
    return ret

class FifthThirdSource(link_based_source.LinkBasedSource, Source):
    def __init__(self,
                 filename: str = None,
                 dir: str = None,
                 # TODO: include balances like Mint does? balances_directory: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(link_prefix='fifththird.', **kwargs)

        self.log_status('FifthThird: loading %s' % filename)
        if dir:
            self.entries = []
            for filename in os.listdir(dir):
                if not filename.endswith('.jsonl'): continue
                filename = os.path.join(dir, filename)

                entries = load_transactions(filename=filename)
                print("Loaded %d entries from: %s" % ( len(entries), filename ) )
                self.entries.extend( entries )
        elif filename:
            self.entries = load_transactions(filename=filename)
            print("Loaded %d entries from: %s" % ( len(self.entries), filename ) )
        else:
            raise RuntimeError("FifthThird module: Must specify either `dir` or `filename`")

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        new_tids      = set([ e.transaction_id for e in self.entries])
        existing_tids = _get_existing_transaction_ids( journal.all_entries )

        for entry in self.entries:
            if entry.transaction_id in existing_tids: 
                # print("Skipping existing transaction: %s" % entry.transaction_id)
                continue

            result = _make_import_result(entry)
            results.add_pending_entry(result)

        results.add_account('Assets:FifthThird:Payment')
        results.add_account('Liabilities:Mortgage:FifthThird')
        results.add_account('Expenses:House:Mortgage:Interest')
        results.add_account('Assets:FifthThird:Escrow')

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting):
        ret = {}
        def maybe_add_key(key):
            x = posting.meta.get(key)
            if x is not None:
                ret[key] = x
        
        maybe_add_key('name')
        maybe_add_key('account_id')
        maybe_add_key('categories')

        return ret

    def is_posting_cleared(self, posting: Posting):
        if not posting.meta: return False
        pending = posting.meta.get('pending', 0)
        return not pending

    @property
    def name(self):
        return 'fifththird'


def load(spec, log_status):
    return FifthThirdSource(log_status=log_status, **spec)
