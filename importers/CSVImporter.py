"""csv importer.
"""
__copyright__ = "Copyright (C) 2021 Shangyan Zhou"
__license__ = "MIT"

import sys
import csv
import datetime
import enum
import io
import re
import os
import logging
import collections
from typing import Union, Dict, Callable, Optional

import dateutil.parser

from beancount.core import data
from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.ingest import importer
from beancount.ingest import cache
from beancount.utils.date_utils import parse_date_liberally

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class Col(enum.Enum):
    """The set of interpretable columns."""

    # The settlement date, the date we should create the posting at.
    DATE = "[DATE]"

    # The date at which the transaction took place.
    TXN_DATE = "[TXN_DATE]"

    # The time at which the transaction took place.
    # Beancount does not support time field -- just add it to metadata.
    TXN_TIME = "[TXN_TIME]"

    # The payee field.
    PAYEE = "[PAYEE]"

    # The narration fields. Use multiple fields to combine them together.
    NARRATION = "[NARRATION]"

    # The amount being posted.
    AMOUNT = "[AMOUNT]"

    # Debits and credits being posted in separate, dedicated columns.
    AMOUNT_DEBIT = "[DEBIT]"
    AMOUNT_CREDIT = "[CREDIT]"

    # Transcation status.
    STATUS = "[STATUS]"

    # Transcatin type.
    TYPE = "[TYPE]"

    # The balance amount, after the row has posted.
    BALANCE = "[BALANCE]"

    # A column which says DEBIT or CREDIT (generally ignored).
    DRCR = "[DRCR]"

    # An account name.
    ACCOUNT = "[ACCOUNT]"


class Drcr(enum.Enum):
    DEBIT = "[DEBIT]"

    CREDIT = "[CREDIT]"

    # For asset transfer and the like
    UNCERTAINTY = "[UNCERTAINTY]"


def cast_to_decimal(amount: str):
    """Cast the amount to either an instance of Decimal or None.

    Args:
        amount: A string of amount. The format may be '¥1,000.00', '5.20', '200'
    Returns:
        The corresponding Decimal of amount.
    """
    if amount is None:
        return None
    amount = "".join(amount.split(","))
    numbers = re.findall(r"\d+\.?\d*", amount)
    assert len(numbers) == 1
    return D(numbers[0])


def strip_blank(contents):
    """ 
    strip the redundant blank in file contents.
    """
    with io.StringIO(contents) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        rows = []
        for row in csvreader:
            rows.append(",".join(['"{}"'.format(x.strip()) for x in row]))
        return "\n".join(rows)


def get_amounts(
    iconfig: Dict[Col, str], row, drcr: Drcr, allow_zero_amounts: bool = False
):
    """Get the amount columns of a row.

    Args:
        iconfig: A dict of Col to row index.
        row: A row array containing the values of the given row.
        allow_zero_amounts: Is a transaction with amount D('0.00') okay? If not,
            return (None, None).
    Returns:
        A pair of (debit-amount, credit-amount), both of which are either an
        instance of Decimal or None, or not available.
    """
    debit, credit = None, None
    if Col.AMOUNT in iconfig:
        amount = row[iconfig[Col.AMOUNT]]
        # Distinguish debit or credit
        if drcr == Drcr.CREDIT:
            credit = amount
        else:
            debit = amount
    else:
        debit, credit = [
            row[iconfig[col]] if col in iconfig else None
            for col in [Col.AMOUNT_DEBIT, Col.AMOUNT_CREDIT]
        ]

    # If zero amounts aren't allowed, return null value.
    is_zero_amount = (credit is not None and cast_to_decimal(credit) == ZERO) and (
        debit is not None and cast_to_decimal(debit) == ZERO
    )
    if not allow_zero_amounts and is_zero_amount:
        return (None, None)

    return (
        -cast_to_decimal(debit) if debit else None,
        cast_to_decimal(credit) if credit else None,
    )


def get_DRCR_status(iconfig: [Col, str], row, drcr_dict):
    """Get the status which says DEBIT or CREDIT of a row.
    """

    try:
        if Col.DRCR in iconfig and len(row[iconfig[Col.DRCR]]):
            return drcr_dict[row[iconfig[Col.DRCR]]]
        elif Col.STATUS in iconfig:
            return drcr_dict[row[iconfig[Col.STATUS]]]
        else:
            if Col.AMOUNT_CREDIT in iconfig and row[iconfig[Col.AMOUNT_CREDIT]]:
                return Drcr.CREDIT
            elif Col.AMOUNT_DEBIT in iconfig and row[iconfig[Col.AMOUNT_DEBIT]]:
                return Drcr.DEBIT
            else:
                return Drcr.UNCERTAINTY
    except KeyError:
        return Drcr.UNCERTAINTY


class Importer(importer.ImporterProtocol):
    """Importer for csv files."""

    def __init__(
        self,
        config: Dict[Col, str],
        default_account: str,
        currency: str,
        file_name_prefix: str,
        skip_lines: int = 0,
        drcr_dict: Optional[Dict] = None,
        refund_keyword=None,
        account_map: Dict = {},
    ):
        """Constructor.

        Args:
          config: A dict of Col enum types to the names or indexes of the columns.
          default_account: An account string, the default account to post this to.
          currency: A currency string, the currenty of this account.
          file_name_prefix: Used for identification.
          skip_lines: Skip first x (garbage) lines of file.
          drcr_dict: A dict to determine whether a transcation is credit or debit.
          refund_keyword: The keyword to determine whether a transaction is a refund.
          account_map: A dict to find the account corresponding to the transactions.
        """

        assert isinstance(config, dict), "Invalid type: {}".format(config)
        self.config = config

        self.currency = currency
        assert isinstance(skip_lines, int)
        self.skip_lines = skip_lines
        self.drcr_dict = drcr_dict
        self.refund_keyword = refund_keyword
        self.account_map = account_map
        self.file_name_prefix = file_name_prefix

    def file_date(self, file):
        "Get the maximum date from the file."
        iconfig, has_header = normalize_config(
            self.config, file.contents(), self.skip_lines
        )
        if Col.DATE in iconfig:
            reader = csv.reader(open(io.StringIO(strip_blank(file.contents()))))
            for _ in range(self.skip_lines):
                next(reader)
            if has_header:
                next(reader)
            max_date = None
            for row in reader:
                if not row:
                    continue
                if row[0].startswith("#"):
                    continue
                date_str = row[iconfig[Col.DATE]]
                date = parse_date_liberally(date_str, self.dateutil_kwds)
                if max_date is None or date > max_date:
                    max_date = date
            return max_date

    def identify(self, file: cache._FileMemo):
        if file.mimetype() != "text/csv":
            return False
        if not os.path.basename(file.name).startswith(self.file_name_prefix):
            return False

        iconfig, _ = normalize_config(self.config, file.contents(), self.skip_lines)
        return len(iconfig) == len(self.config)

    def extract(self, file, existing_entries=None):
        entries = []

        # Normalize the configuration to fetch by index.
        iconfig, has_header = normalize_config(
            self.config, file.contents(), self.skip_lines
        )

        reader = csv.reader(io.StringIO(strip_blank(file.contents())))

        # Skip garbage lines
        for _ in range(self.skip_lines):
            next(reader)

        # Skip header, if one was detected.
        if has_header:
            next(reader)

        def get(row, ftype):
            return row[iconfig[ftype]] if ftype in iconfig else None

        # Parse all the transactions.
        first_row = last_row = None
        for index, row in enumerate(reader, 1):
            if not row:
                continue
            if row[0].startswith("#"):
                continue
            if row[0].startswith("-----------"):
                break

            if first_row is None:
                first_row = row
            last_row = row

            # Extract the data we need from the row, based on the configuration.
            status = get(row, Col.STATUS)
            date = get(row, Col.DATE)
            txn_date = get(row, Col.TXN_DATE)
            txn_time = get(row, Col.TXN_TIME)
            account = get(row, Col.ACCOUNT)
            tx_type = get(row, Col.TYPE)
            tx_type = tx_type or ""

            payee = get(row, Col.PAYEE)
            if payee:
                payee = payee.strip()

            narration = get(row, Col.NARRATION)
            if narration:
                narration = narration.strip()

            # Create a transaction
            meta = data.new_metadata(file.name, index)
            if txn_date is not None:
                meta["date"] = parse_date_liberally(txn_date)
            if txn_time is not None:
                meta["time"] = str(dateutil.parser.parse(txn_time).time())
            date = parse_date_liberally(date)
            txn = data.Transaction(
                meta,
                date,
                self.FLAG,
                payee,
                narration,
                data.EMPTY_SET,
                data.EMPTY_SET,
                [],
            )

            # Attach one posting to the transaction
            drcr = get_DRCR_status(iconfig, row, self.drcr_dict)
            amount_debit, amount_credit = get_amounts(iconfig, row, drcr)

            # Skip empty transactions
            if amount_debit is None and amount_credit is None:
                continue

            for amount in [amount_debit, amount_credit]:
                if amount is None:
                    continue
                units = Amount(amount, self.currency)

                if drcr == Drcr.UNCERTAINTY:
                    if account and len(account.split("-")) == 2:
                        accounts = account.split("-")
                        primary_account = mapping_account(
                            self.account_map["assets"], accounts[1]
                        )
                        secondary_account = mapping_account(
                            self.account_map["assets"], accounts[0]
                        )
                        txn.postings.append(
                            data.Posting(
                                primary_account, -units, None, None, None, None
                            )
                        )
                        txn.postings.append(
                            data.Posting(
                                secondary_account, None, None, None, None, None
                            )
                        )
                    else:
                        txn.postings.append(
                            data.Posting(
                                self.account_map["assets"]["DEFAULT"],
                                units,
                                None,
                                None,
                                None,
                                None,
                            )
                        )
                else:
                    primary_account = mapping_account(
                        self.account_map["assets"], account
                    )
                    txn.postings.append(
                        data.Posting(primary_account, units, None, None, None, None)
                    )

                    payee_narration = payee + narration
                    account_map = self.account_map[
                        "credit"
                        if drcr == Drcr.CREDIT
                        and not (
                            self.refund_keyword
                            and payee_narration.find(self.refund_keyword) != -1
                        )
                        else "debit"
                    ]

                    secondary_account = mapping_account(
                        account_map, payee_narration + tx_type
                    )
                    txn.postings.append(
                        data.Posting(secondary_account, None, None, None, None, None)
                    )

            # Add the transaction to the output list
            logging.debug(txn)
            entries.append(txn)

        # Figure out if the file is in ascending or descending order.
        first_date = parse_date_liberally(get(first_row, Col.DATE))
        last_date = parse_date_liberally(get(last_row, Col.DATE))
        is_ascending = first_date < last_date

        # Reverse the list if the file is in descending order
        if not is_ascending:
            entries = list(reversed(entries))

        # Add a balance entry if possible
        if Col.BALANCE in iconfig and entries:
            entry = entries[-1]
            date = entry.date + datetime.timedelta(days=1)
            balance = entry.meta.get("balance", None)
            if balance is not None:
                meta = data.new_metadata(file.name, index)
                entries.append(
                    data.Balance(
                        meta, date, account, Amount(balance, self.currency), None, None,
                    )
                )

        # Remove the 'balance' metadata.
        for entry in entries:
            entry.meta.pop("balance", None)

        return entries


def normalize_config(config, head, skip_lines: int = 0):
    """Using the header line, convert the configuration field name lookups to int indexes.

    Args:
      config: A dict of Col types to string or indexes.
      head: A string, some decent number of bytes of the head of the file.
      dialect: A dialect definition to parse the header
      skip_lines: Skip first x (garbage) lines of file.
    Returns:
      A pair of
        A dict of Col types to integer indexes of the fields, and
        a boolean, true if the file has a header.
    Raises:
      ValueError: If there is no header and the configuration does not consist
        entirely of integer indexes.
    """
    # Skip garbage lines before sniffing the header
    assert isinstance(skip_lines, int)
    assert skip_lines >= 0
    for _ in range(skip_lines):
        head = head[head.find("\n") + 1 :]

    head = strip_blank(head)
    has_header = csv.Sniffer().has_header(head)
    if has_header:
        header = next(csv.reader(io.StringIO(head)))
        field_map = {
            field_name.strip(): index for index, field_name in enumerate(header)
        }
        index_config = {}
        for field_type, field in config.items():
            if isinstance(field, str):
                field = field_map[field]
            index_config[field_type] = field
    else:
        if any(not isinstance(field, int) for field_type, field in config.items()):
            raise ValueError(
                "csv config without header has non-index fields: " "{}".format(config)
            )
        index_config = config
    return index_config, has_header


def mapping_account(account_map, keyword):
    """Finding which key of account_map contains the keyword, return the corresponding value.

    Args:
      account_map: A dict of account keywords string (each keyword separated by "|") to account name.
      keyword: A keyword string.
    Return:
      An account name string.
    Raises:
      KeyError: If "DEFAULT" keyword is not in account_map.
    """
    if "DEFAULT" not in account_map:
        raise KeyError("DEFAULT is not in " + account_map.__str__)
    account_name = account_map["DEFAULT"]
    for account_keywords in account_map.keys():
        if account_keywords == "DEFAULT":
            continue
        if re.search(account_keywords, keyword):
            account_name = account_map[account_keywords]
            break
    return account_name
