"""
Handles importing investment records from Vanguard's website.

Data from:

https://personal.vanguard.com/rs/cfv/client-transaction-history-webservice-production-current/rs/transactions/account/######ACCT#######?poid={VG-CLIENT-POID}&years=10&nsccId=

Which is a single object with a "transaction" array:

e.g.

{"transaction":[
    {"CHKImageId":"","accountId":"/######ACCTID#######","accountNumber":"/######ACCTNUM#######"},
    [...]
]}

This importer expects the above data stored with one transaction object per line (JSONL format).

e.g.

{"CHKImageId":"","accountId":"/######ACCTID#######","accountNumber":"/######ACCTNUM#######"},
{"CHKImageId":"","accountId":"/######ACCTID#######","accountNumber":"/######ACCTNUM#######"},

curl -v 'url' | jq '.transaction[]' -c > vanguard.jsonl

CAVEATS:

This is "good enough" quality.

I have no clue what best practices for modeling investments are.  I welcome examples of how
my portfolio should be modeled and I'll adjust this at some point. Or help me make it amazing. :)

I've run into cases where I result in a negative cost basis for my accounts, which beancount does
not like at all. This is a known issue with beancount, but might also be an issue with how I'm generating
the postings.

There was a transition in handling of dividents and short-term / long-term gains - the codes
changed and the way these were represented changed. The new method is much better and clearer,
but I have data from before the change. This code handles both forms, but not horribly cleanly.

Accounts are hard-coded. Account structure (each commodity split into its own sub-account) is hard-coded.
"""

from typing import List, Union, Optional, Set, Dict
import json
import datetime
import collections
import re
import os
import numbers
import decimal

from beancount.core.data import Transaction, Posting, Balance, Price, EMPTY_SET
from beancount.core.amount import Amount
from beancount.core.position import get_position, Position, Cost
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from . import link_based_source, description_based_source, Source
from . import ImportResult, SourceResults
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

VanguardEntry = collections.namedtuple(
    'VanguardEntry',
    [
        "CHKImageId",
        "accountId",
        "accountNumber",
        "accountPositionID",
        "accountType",
        "accruedInterest",
        "assocBrkgAccountNumber",
        "assocExecutionPrice",
        "assocInvestmentName",
        "assocNetAmount",
        "assocRecordDate",
        "assocTAAccountNumber",
        "assocTxnFee",
        "assocTxnQuantity",
        "assocVGITransactionID",
        "assocVbaPrincipalAmount",
        "baseType",
        "cancelFlag",
        "chkNum",
        "commission",
        "cusip",
        "description",
        "exchangeCd",
        "fee",
        "fundAccountNumber",
        "grossAmount",
        "investmentName",
        "investmentType",
        "netAmount",
        "price",
        "principleAmount",
        "processDate",
        "quantity",
        "recordDate",
        "securitySubTypeCd",
        "securityTypeCd",
        "sequenceNumber",
        "settlementDate",
        "ticker",
        "tradeDate",
        "transactionAmount",
        "transactionCode",
        "transactionType",
        "txnAcctType",
        "vbaAssocAccountID",
        "vbaAssocAccountPositionID",
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


def load_transactions(filename: str, currency: str = 'USD') -> List[VanguardEntry]:
    try:
        entries = []
        filename = os.path.abspath(filename)
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            lno = 0
            for line in f:
                lno += 1
                data = json.loads(line, parse_float=decimal.Decimal, parse_int=decimal.Decimal)
                data['line'] = lno
                data['filename'] = filename

                # add in any missing fields
                for field in VanguardEntry._fields:
                    if not field in data:
                        data[field] = None
                    if field.endswith("Date"):
                        data[field] = date(data[field])
                        
                entries.append( VanguardEntry(**data) )
        entries.reverse()
        entries.sort(key=lambda x: x.sequenceNumber)
        return entries

    except Exception as e:
        print
        raise RuntimeError('Vanguard JSON-L file has incorrect format', filename) from e

def get_info(raw_entry: VanguardEntry) -> dict:
    return dict(
        type='application/json',
        filename=raw_entry.filename,
        line=raw_entry.line,
    )

def _make_import_result(vanguard_entry: VanguardEntry, vanguard_accounts, account_mappings) -> ImportResult:
    meta = collections.OrderedDict()
    for field,value in vanguard_entry._asdict().items():
        if field == "filename": continue
        if field == "line": continue
        if value is not None and value != 0 and value != "":        
            meta["vanguard_%s" % field] = value

    transaction_ids = [ 'vanguard.%s.%s' % ( vanguard_entry.accountId, vanguard_entry.sequenceNumber ) ]

    src_account = 'Expenses:FIXME'
    if not vanguard_entry.accountId in vanguard_accounts:
        raise RuntimeError("No existing account in the journal has a meta vanguard_account_id='%s'" % vanguard_entry.accountId)

    cash_account = '%s:%s' % ( vanguard_accounts[ vanguard_entry.accountId ], 'Cash' )
    vang_account = '%s:%s' % ( vanguard_accounts[ vanguard_entry.accountId ], vanguard_entry.ticker ) 

    dst_account = vang_account
    src_units = Amount(-1*D(vanguard_entry.principleAmount), 'USD')
    dst_units = Amount(D(vanguard_entry.quantity), vanguard_entry.ticker)
    dst_price = None
    dst_cost  = None
    if vanguard_entry.price:
        dst_cost  = Cost(D(vanguard_entry.price), "USD", vanguard_entry.tradeDate, "")
    elif vanguard_entry.netAmount != 0 and vanguard_entry.quantity > 0:
        dst_cost  = Cost(D(vanguard_entry.netAmount)/D(vanguard_entry.quantity), "USD", vanguard_entry.tradeDate, "")

    payee     = 'Vanguard'
    narration = "%s - %s - %s" % ( vanguard_entry.transactionType, vanguard_entry.ticker, vanguard_entry.investmentName )

    if vanguard_entry.transactionCode in ["5005", "7066", "7001", "BUY"]: # Buy 
        src_account = cash_account
    elif vanguard_entry.transactionCode in ("9558", "9555"): # Transfer 
        src_account = cash_account

    elif vanguard_entry.transactionCode == "DTRF": # Direct transfer
        src_account = 'Expenses:FIXME'

        if vanguard_entry.investmentName == "CASH":
            src_units = Amount(D(vanguard_entry.principleAmount), 'USD')
            dst_units = Amount(-1*D(vanguard_entry.principleAmount), 'USD')
            dst_account = cash_account
        else:
            src_units = Amount(-D(vanguard_entry.quantity), vanguard_entry.ticker)
            dst_units = Amount(D(vanguard_entry.quantity), vanguard_entry.ticker)

    elif vanguard_entry.transactionCode == "WOFF": # writeoff 
        src_units = Amount(D(vanguard_entry.principleAmount), 'USD')
        dst_units = Amount(-1*D(vanguard_entry.principleAmount), 'USD')
        dst_account = cash_account

    elif vanguard_entry.transactionCode in ["8037"]: # Capital gain (ST) direct reinvestment
        src_account = account_mappings['Income:GainST']
    elif vanguard_entry.transactionCode in ["8035"]: # Capital gain (LT) direct reinvestment
        src_account = account_mappings['Income:GainLT']
    elif vanguard_entry.transactionCode in ["5010", "8015", "8112"]: # Dividend - directly reinvestment
        src_account = account_mappings['Income:Dividend']

    elif vanguard_entry.transactionCode == "ROLL": # rollover check
        src_account = 'Assets:Retirement:OldAccount'
        src_units = Amount(D(vanguard_entry.principleAmount), 'USD')

        dst_account = cash_account
        dst_units = Amount(-1*D(vanguard_entry.principleAmount), 'USD')

    elif vanguard_entry.transactionCode in ["SCAP", "LCAP"]:  # Capital gain (ST/LT) to cash then RSCPed back in as reinvestment
        dst_account = account_mappings['Income:GainST']
        dst_units   = Amount(D(vanguard_entry.principleAmount), 'USD')
        dst_cost    = None
        src_account = 'Expenses:FIXME'
        narration = "Gain (LT)" if vanguard_entry.transactionCode == "LCAP" else "Gain (ST)"        

    elif vanguard_entry.transactionCode in ["RLCP", "RSCP"]: # Capital gain (ST/LT) reinvestment from cash
        src_account = 'Expenses:FIXME'
        dst_account = vang_account
        narration = "Gain (LT)" if vanguard_entry.transactionCode == "RLCP" else "Gain (ST)"        

    elif vanguard_entry.transactionCode == "DIV": # Dividend - to cash, then RDIVed back into the fund
        dst_account = account_mappings['Income:Dividend']
        dst_units   = Amount(D(vanguard_entry.principleAmount), 'USD')
        dst_cost    = None
        src_account = 'Expenses:FIXME'
        narration   = "Dividend"

    elif vanguard_entry.transactionCode in ["RDIV", "RDDV"]: # dividend re-investment back into the fund
        #src_account = cash_account
        src_account = 'Expenses:FIXME'
        dst_account = vang_account
        narration   = "Dividend"

    elif vanguard_entry.transactionCode in ["CNVO", "CNVI"]: # conversion outgoing / incoming
        src_account = 'Expenses:FIXME'
        dst_account = vang_account
        src_units = Amount(-1*D(vanguard_entry.principleAmount), 'USD')
        dst_units = Amount(D(vanguard_entry.quantity), vanguard_entry.ticker)

        if vanguard_entry.transactionCode == "CNVO":
            dst_price = Amount(D(vanguard_entry.netAmount)/D(vanguard_entry.quantity), "USD")
            dst_cost  = None
        else:
            dst_cost  = Cost(D(vanguard_entry.netAmount)/D(vanguard_entry.quantity), "USD", vanguard_entry.tradeDate, "")
            dst_price = None

    elif vanguard_entry.transactionCode in ["SELE"]: # sell for exchange
        src_account = 'Expenses:FIXME'
        dst_account = vang_account
        dst_price   = Amount(number=D(vanguard_entry.price), currency="USD")
        dst_cost    = None

    src_posting = Posting(
            account=src_account,
            units=src_units,
            cost=None,
            price=None,
            flag=None,
            meta=None,
        )

    dst_postings = []

    dst_postings.append( Posting( 
        account=dst_account,  
        units=dst_units, 
        cost=dst_cost,
        price=dst_price,
        flag=None,
        meta=meta 
    ) )

    transaction = Transaction(
        meta      = None,
        date      = vanguard_entry.recordDate,
        flag      = FLAG_OKAY,
        payee     = payee,
        narration = narration,
        tags      = EMPTY_SET,
        links     = EMPTY_SET,
        postings=[ src_posting ] + dst_postings
    )

    return ImportResult( date=vanguard_entry.recordDate, info=get_info(vanguard_entry), entries=[transaction])


def _get_existing_transaction_ids( all_entries ):
    ret = set()

    for entry in all_entries:
        if not isinstance(entry, Transaction): continue

        tid = entry.meta.get('vanguard_sequenceNumber')
        if tid: ret.add(tid)

        for posting in entry.postings:
            tid = posting.meta.get('vanguard_sequenceNumber')
            if tid: ret.add(tid)
        
    return ret

class VanguardSource(link_based_source.LinkBasedSource, Source):
    def __init__(self,
                 filename: str = None,
                 dir: str = None,
                 account_mappings: Dict[str,str] = {},
                 **kwargs) -> None:
        super().__init__(link_prefix='Vanguard.', **kwargs)

        self.account_mappings = account_mappings
        missing_keys = set(['Assets:Cash', 'Income:Dividend', 'Income:GainST', 'Income:GainLT'])
        missing_keys = missing_keys -  set( account_mappings.keys() ) 
        if len(missing_keys) > 0:
            raise RuntimeError("Vanguard module: Configuration account_mappings is missing mappings for: %s" % " ".join(missing_keys))

        self.log_status('Vanguard: loading %s' % filename)
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
            raise RuntimeError("Vanguard module: Must specify either `dir` or `filename`")

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        new_tids      = set([ e.sequenceNumber for e in self.entries])
        existing_tids = _get_existing_transaction_ids( journal.all_entries )

        vanguard_accounts = find_vanguard_accounts(journal);

        for entry in self.entries:
            if entry.sequenceNumber in existing_tids: 
                # print("Skipping existing transaction: %s" % entry.sequenceNumber)
                continue

            result = _make_import_result(entry, vanguard_accounts, self.account_mappings)
            results.add_pending_entry(result)

        for account in self.account_mappings.values():
            results.add_account(account)
        for account in vanguard_accounts.keys():
            results.add_account(account)

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
        return 'Vanguard'

def find_vanguard_accounts(journal: JournalEditor) -> Dict[str, str]:
    accounts = dict()
    for entry in journal.accounts.values():
        if entry.meta and 'vanguard_account_id' in entry.meta:
            accounts[ entry.meta['vanguard_account_id'] ] = entry.account
    return accounts


def load(spec, log_status):
    return VanguardSource(log_status=log_status, **spec)
