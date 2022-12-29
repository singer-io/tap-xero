from requests.exceptions import HTTPError
import singer
from singer import metadata, metrics, Transformer
from singer.utils import strptime_with_tz
import backoff
from . import transform

LOGGER = singer.get_logger()
FULL_PAGE_SIZE = 100


def _request_with_timer(tap_stream_id, xero, filter_options):
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            resp = xero.filter(tap_stream_id, **filter_options)
            timer.tags[metrics.Tag.http_status_code] = 200
            return resp
        except HTTPError as e:
            timer.tags[metrics.Tag.http_status_code] = e.response.status_code
            raise


class RateLimitException(Exception):
    pass


@backoff.on_exception(backoff.expo,
                      RateLimitException,
                      max_tries=10,
                      factor=2)
def _make_request(ctx, tap_stream_id, filter_options=None, attempts=0):
    filter_options = filter_options or {}
    try:
        return _request_with_timer(tap_stream_id, ctx.client, filter_options)
    except HTTPError as e:
        if e.response.status_code == 401:
            if attempts == 1:
                raise Exception("Received Not Authorized response after credential refresh.") from e
            ctx.refresh_credentials()
            return _make_request(ctx, tap_stream_id, filter_options, attempts + 1)

        if e.response.status_code == 503:
            raise RateLimitException() from e

        raise
    assert False


class Stream():
    def __init__(self, tap_stream_id, pk_fields, bookmark_key="UpdatedDateUTC", format_fn=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.format_fn = format_fn or (lambda x: x)
        self.bookmark_key = bookmark_key
        self.replication_method = "INCREMENTAL"
        self.filter_options = {}

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records, ctx):
        stream = ctx.catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        mdata = stream.metadata
        for rec in records:
            with Transformer() as transformer:
                rec = transformer.transform(rec, schema, metadata.to_map(mdata))
                singer.write_record(self.tap_stream_id, rec)
        self.metrics(records)


class BookmarkedStream(Stream):
    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        start = ctx.update_start_date_bookmark(bookmark)
        records = _make_request(ctx, self.tap_stream_id, dict(since=start))
        if records:
            self.format_fn(records)
            self.write_records(records, ctx)
            max_bookmark_value = max([record[self.bookmark_key] for record in records]) # pylint: disable=consider-using-generator
            ctx.set_bookmark(bookmark, max_bookmark_value)
            ctx.write_state()


class PaginatedStream(Stream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        offset = [self.tap_stream_id, "page"]
        start = ctx.update_start_date_bookmark(bookmark)
        curr_page_num = ctx.get_offset(offset) or 1

        self.filter_options.update({"since": start})

        # Xero bug causes all manual_journal records to be returned instead of
        # 100 per page when `order` is specified. `UpdatedDateUTC ASC` is the
        # default so we can safely exclude it until the bug is fixed.
        if self.tap_stream_id != "manual_journals":
            self.filter_options.update({"order": "UpdatedDateUTC ASC"})

        max_updated = start
        while True:
            ctx.set_offset(offset, curr_page_num)
            ctx.write_state()
            self.filter_options["page"] = curr_page_num
            records = _make_request(ctx, self.tap_stream_id, self.filter_options)
            if records:
                self.format_fn(records)
                self.write_records(records, ctx)
                max_updated = records[-1][self.bookmark_key]
            if not records or len(records) < FULL_PAGE_SIZE:
                break
            curr_page_num += 1
        ctx.clear_offsets(self.tap_stream_id)
        ctx.set_bookmark(bookmark, max_updated)
        ctx.write_state()


class Contacts(PaginatedStream):
    def __init__(self, *args, **kwargs):
        super().__init__("contacts", ["ContactID"], format_fn=transform.format_contacts, *args, **kwargs)

    def sync(self, ctx):
        # Parameter to collect archived contacts from the Xero platform
        if ctx.config.get("include_archived_contacts") in ["true", True]:
            self.filter_options.update({'includeArchived': "true"})

        super().sync(ctx)


class Journals(Stream):
    """The Journals endpoint is a special case. It has its own way of ordering
    and paging the data. See
    https://developer.xero.com/documentation/api/journals"""
    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        journal_number = ctx.get_bookmark(bookmark) or 0
        while True:
            filter_options = {"offset": journal_number}
            records = _make_request(ctx, self.tap_stream_id, filter_options)
            if records:
                self.format_fn(records)
                self.write_records(records, ctx)
                journal_number = max((record[self.bookmark_key] for record in records))
                ctx.set_bookmark(bookmark, journal_number)
                ctx.write_state()
            if not records or len(records) < FULL_PAGE_SIZE:
                break


class LinkedTransactions(Stream):
    """The Linked Transactions endpoint is a special case. It supports
    pagination, but not the Modified At header, but the objects returned have
    the UpdatedDateUTC timestamp in them. Therefore we must always iterate over
    all of the data, but we can manually omit records based on the
    UpdatedDateUTC property."""
    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        offset = [self.tap_stream_id, "page"]
        start = ctx.update_start_date_bookmark(bookmark)
        curr_page_num = ctx.get_offset(offset) or 1
        max_updated = start
        while True:
            ctx.set_offset(offset, curr_page_num)
            ctx.write_state()
            filter_options = {"page": curr_page_num}
            raw_records = _make_request(ctx, self.tap_stream_id, filter_options)
            records = [x for x in raw_records
                       if strptime_with_tz(x[self.bookmark_key]) >= strptime_with_tz(start)]
            if records:
                self.write_records(records, ctx)
                max_updated = records[-1][self.bookmark_key]
            if not records or len(records) < FULL_PAGE_SIZE:
                break
            curr_page_num += 1
        ctx.clear_offsets(self.tap_stream_id)
        ctx.set_bookmark(bookmark, max_updated)
        ctx.write_state()


class Everything(Stream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bookmark_key = None
        self.replication_method = "FULL_TABLE"

    def sync(self, ctx):
        records = _make_request(ctx, self.tap_stream_id)
        self.format_fn(records)
        self.write_records(records, ctx)


all_streams = [
    # PAGINATED STREAMS
    # These endpoints have all the best properties: they return the
    # UpdatedDateUTC property and support the Modified After, order, and page
    # parameters
    PaginatedStream("bank_transactions", ["BankTransactionID"]),
    Contacts(),
    PaginatedStream("quotes", ["QuoteID"]),
    PaginatedStream("credit_notes", ["CreditNoteID"], format_fn=transform.format_credit_notes),
    PaginatedStream("invoices", ["InvoiceID"], format_fn=transform.format_invoices),
    PaginatedStream("manual_journals", ["ManualJournalID"]),
    PaginatedStream("overpayments", ["OverpaymentID"], format_fn=transform.format_over_pre_payments),
    PaginatedStream("payments", ["PaymentID"], format_fn=transform.format_payments),
    PaginatedStream("prepayments", ["PrepaymentID"], format_fn=transform.format_over_pre_payments),
    PaginatedStream("purchase_orders", ["PurchaseOrderID"]),

    # JOURNALS STREAM
    # This endpoint is paginated, but in its own special snowflake way.
    Journals("journals", ["JournalID"], bookmark_key="JournalNumber", format_fn=transform.format_journals),

    # NON-PAGINATED STREAMS
    # These endpoints do not support pagination, but do support the Modified At
    # header.
    BookmarkedStream("accounts", ["AccountID"]),
    BookmarkedStream("bank_transfers", ["BankTransferID"], bookmark_key="CreatedDateUTC"),
    BookmarkedStream("employees", ["EmployeeID"]),
    BookmarkedStream("expense_claims", ["ExpenseClaimID"]),
    BookmarkedStream("items", ["ItemID"]),
    BookmarkedStream("receipts", ["ReceiptID"], format_fn=transform.format_receipts),
    BookmarkedStream("users", ["UserID"], format_fn=transform.format_users),

    # PULL EVERYTHING STREAMS
    # These endpoints do not support the Modified After header (or paging), so
    # we must pull all the data each time.
    Everything("branding_themes", ["BrandingThemeID"]),
    Everything("contact_groups", ["ContactGroupID"], format_fn=transform.format_contact_groups),
    Everything("currencies", ["Code"]),
    Everything("organisations", ["OrganisationID"]),
    Everything("repeating_invoices", ["RepeatingInvoiceID"]),
    Everything("tax_rates", ["TaxType"]),
    Everything("tracking_categories", ["TrackingCategoryID"]),

    # LINKED TRANSACTIONS STREAM
    # This endpoint is not paginated, but can do some manual filtering
    LinkedTransactions("linked_transactions", ["LinkedTransactionID"], bookmark_key="UpdatedDateUTC"),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
