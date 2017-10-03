import time
import datetime
import singer
import pendulum
import attr
from requests.exceptions import HTTPError
from singer.utils import strftime
from singer import metrics
import backoff
from .xero import XeroClient
from . import credentials
from . import transform

LOGGER = singer.get_logger()


def _request_with_timer(tap_stream_id, xero, options):
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            resp = xero.filter(tap_stream_id, **options)
            timer.tags[metrics.Tag.http_status_code] = 200
            return resp
        except HTTPError as e:
            timer.tags[metrics.Tag.http_status_code] = e.response.status_code
            raise


class RateLimitException(Exception):
    pass


class Puller(object):
    bookmark_property = "UpdatedDateUTC"

    def __init__(self, config, state, tap_stream_id):
        self.config = config
        self.state = state
        self.tap_stream_id = tap_stream_id
        self.xero = XeroClient(config)

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def _make_request(self, options={}):
        try:
            return _request_with_timer(self.tap_stream_id, self.xero, options)
        except HTTPError as e:
            if e.response.status_code == 401:
                credentials.refresh(self.config)
            elif e.response.status_code == 503:
                raise RateLimitException()
            else:
                raise

    @property
    def _order_by(self):
        return self.bookmark_property + " ASC"

    @property
    def _bookmark(self):
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        if self.tap_stream_id not in self.state["bookmarks"]:
            self.state["bookmarks"][self.tap_stream_id] = {}
        return self.state["bookmarks"][self.tap_stream_id]

    @property
    def _offset(self):
        if "offset" not in self._bookmark:
            self._bookmark["offset"] = {}
        return self._bookmark["offset"]

    def _set_last_updated(self, updated_at):
        if isinstance(updated_at, datetime.datetime):
            updated_at = updated_at.isoformat()
        self._bookmark[self.bookmark_property] = updated_at

    def _update_start_state(self):
        if not self._bookmark.get(self.bookmark_property):
            self._set_last_updated(self.config["start_date"])
        return pendulum.parse(self._bookmark[self.bookmark_property])

    FULL_PAGE_SIZE = 100

    def _paginate(self,
                  options={},
                  first_page_num=1,
                  pagination_key="page",
                  get_next_page_num=lambda curr_page_num, _: curr_page_num + 1):
        num_pages_yielded = 0
        curr_page_num = first_page_num
        while True:
            if num_pages_yielded > 1e6:
                raise Exception("1 million pages doesn't seem realistic")
            options[pagination_key] = curr_page_num
            page = self._make_request(options)
            if not page:
                break
            next_page_num = get_next_page_num(curr_page_num, page)
            yield page, next_page_num
            curr_page_num = next_page_num
            num_pages_yielded += 1
            if len(page) < self.FULL_PAGE_SIZE:
                break

    def yield_pages(self):
        raise NotImplemented()


class IncrementingPull(Puller):
    def yield_pages(self):
        start = self._update_start_state()
        page = self._make_request(dict(since=start))
        if page:
            self._set_last_updated(page[-1][self.bookmark_property])
            yield page


class BankTransfersPull(IncrementingPull):
    bookmark_property = "CreatedDateUTC"


class PaginatedPull(Puller):
    def yield_pages(self):
        start = self._update_start_state()
        first_num = self._offset.get("page") or 1
        options = dict(since=start, order=self._order_by)
        for page, next_num in self._paginate(options, first_page_num=first_num):
            self._offset["page"] = next_num
            self._set_last_updated(page[-1][self.bookmark_property])
            yield page
        self._offset.pop("page", None)


class JournalPull(Puller):
    """The Journals endpoint is a special case. It has its own way of ordering
    and paging the data. See
    https://developer.xero.com/documentation/api/journals"""
    def yield_pages(self):
        first_num = self._bookmark.get("JournalNumber") or 0
        self._bookmark["JournalNumber"] = first_num
        next_page_fn = lambda _, page: page[-1]["JournalNumber"]
        for page, next_num in self._paginate(first_page_num=first_num,
                                             pagination_key="offset",
                                             get_next_page_num=next_page_fn):
            self._bookmark["JournalNumber"] = next_num
            yield page


class LinkedTransactionsPull(Puller):
    """The Linked Transactions endpoint is a special case. It supports
    pagination, but not the Modified At header, but the objects returned have
    the UpdatedDateUTC timestamp in them. Therefore we must always iterate over
    all of the data, but we can manually omit records based on the
    UpdatedDateUTC property."""
    def yield_pages(self):
        start = self._update_start_state()
        first_num = self._offset.get("page") or 1
        for page, next_page_num in self._paginate(first_page_num=first_num):
            self._offset["page"] = next_page_num
            self._set_last_updated(page[-1][self.bookmark_property])
            yield [x for x in page if x["UpdatedDateUTC"] >= strftime(start)]
        self._offset.pop("page", None)


class CreditNotes(PaginatedPull):
    def yield_pages(self):
        for credit_notes in super().yield_pages():
            transform.format_credit_notes(credit_notes)
            yield credit_notes


class ContactGroups(Puller):
    def yield_pages(self):
        contact_groups = self._make_request()
        transform.format_contact_groups(contact_groups)
        yield contact_groups


class Contacts(PaginatedPull):
    def yield_pages(self):
        for contacts in super().yield_pages():
            for contact in contacts:
                transform.format_contact_groups(contact["ContactGroups"])
            yield contacts


class EverythingPull(Puller):
    def yield_pages(self):
        yield self._make_request()


@attr.attributes
class Stream(object):
    tap_stream_id = attr.attr()
    pk_fields = attr.attr()
    puller = attr.attr()

all_streams = [
    # PAGINATED STREAMS
    # These endpoints have all the best properties: they return the
    # UpdatedDateUTC property and support the Modified After, order, and page
    # parameters
    Stream("bank_transactions", ["BankTransactionID"], PaginatedPull),
    Stream("contacts", ["ContactID"], Contacts),
    Stream("credit_notes", ["CreditNoteID"], CreditNotes),
    Stream("invoices", ["InvoiceID"], PaginatedPull),
    Stream("manual_journals", ["ManualJournalID"], PaginatedPull),
    Stream("overpayments", ["OverpaymentID"], PaginatedPull),
    Stream("prepayments", ["PrepaymentID"], PaginatedPull),
    Stream("purchase_orders", ["PurchaseOrderID"], PaginatedPull),

    # JOURNALS STREAM
    # This endpoint is paginated, but in its own special snowflake way.
    Stream("journals", ["JournalID"], JournalPull),

    # NON-PAGINATED STREAMS
    # These endpoints do not support pagination, but do support the Modified At
    # header.
    Stream("accounts", ["AccountID"], IncrementingPull),
    Stream("bank_transfers", ["BankTransferID"], BankTransfersPull),
    Stream("employees", ["EmployeeID"], IncrementingPull),
    Stream("expense_claims", ["ExpenseClaimID"], IncrementingPull),
    Stream("items", ["ItemID"], IncrementingPull),
    Stream("payments", ["PaymentID"], IncrementingPull),
    Stream("receipts", ["ReceiptID"], IncrementingPull),
    Stream("users", ["UserID"], IncrementingPull),

    # PULL EVERYTHING STREAMS
    # These endpoints do not support the Modified After header (or paging), so
    # we must pull all the data each time.
    Stream("branding_themes", ["BrandingThemeID"], EverythingPull),
    Stream("contact_groups", ["ContactGroupID"], ContactGroups),
    Stream("currencies", ["Code"], EverythingPull),
    Stream("organisations", ["OrganisationID"], EverythingPull),
    Stream("repeating_invoices", ["RepeatingInvoiceID"], EverythingPull),
    Stream("tax_rates", ["TaxType"], EverythingPull),
    Stream("tracking_categories", ["TrackingCategoryID"], EverythingPull),

    # LINKED TRANSACTIONS STREAM
    # This endpoint is not paginated, but can do some manual filtering
    Stream("linked_transactions", ["LinkedTransactionID"], LinkedTransactionsPull),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
