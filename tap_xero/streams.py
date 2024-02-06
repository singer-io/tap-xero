from requests.exceptions import HTTPError
import singer
import logging
from datetime import datetime, timedelta
from singer import metadata, metrics, Transformer
from singer.utils import strptime_with_tz
import backoff
from . import transform
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


LOGGER = singer.get_logger()
FULL_PAGE_SIZE = 100

logging.getLogger('backoff').setLevel(logging.CRITICAL)


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

        if e.response.status_code in [429, 503]:
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
            max_bookmark_value = max([record[self.bookmark_key] for record in records])
            ctx.set_bookmark(bookmark, max_bookmark_value)
            ctx.write_state()


class ReportStream(Stream):
    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        start = ctx.update_start_date_bookmark(bookmark)

        start_dt = parse(start)
        start_dt = datetime(start_dt.year, start_dt.month, 1)
        while True:
            from_date = start_dt.strftime("%Y-%m-01")
            to_date = start_dt + relativedelta(months=1) - timedelta(1)
            to_date = to_date.strftime("%Y-%m-%d")
            self.filter_options.update(dict(fromDate=from_date, toDate=to_date))
            records = _make_request(ctx, self.tap_stream_id, self.filter_options)

            records = records["Reports"]

            report_rows = []
            for row in records[0]["Rows"]:
                if row["RowType"]=="Section":
                    for r in row["Rows"]:
                        if r["RowType"]=="Row":
                            record = {}
                            record["from_date"] = from_date
                            record["to_date"] = to_date
                            record["account"] = r["Cells"][0]["Value"]
                            record["value"] = r["Cells"][1]["Value"]
                            report_rows.append(record)
            if report_rows:
                self.format_fn(report_rows)
                self.write_records(report_rows, ctx)
            start_dt = start_dt + relativedelta(months=1)
            if start_dt >= datetime.utcnow():
                break
        ctx.set_bookmark(bookmark, (start_dt - relativedelta(months=1)).isoformat())
        ctx.write_state()


class PaginatedStream(Stream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def sync(self, ctx):
        bookmark = [self.tap_stream_id, self.bookmark_key]
        offset = [self.tap_stream_id, "page"]
        start = ctx.update_start_date_bookmark(bookmark)
        curr_page_num = ctx.get_offset(offset) or 1

        self.filter_options.update(dict(since=start, order="UpdatedDateUTC ASC"))

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
            logging.info("Got {} records: {}".format(
                len(records), records
            ))
            if records:
                self.format_fn(records)
                self.write_records(records, ctx)
                journal_number = max((record[self.bookmark_key] for record in records))
                ctx.set_bookmark(bookmark, journal_number)
                ctx.write_state()
            if not records or len(records) < FULL_PAGE_SIZE:
                break

    def write_records(self, records, ctx):
        """"Custom implementation from the write records method available in Stream class"""
        stream = ctx.catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        lines_schema = schema["properties"].get("JournalLines", {}).get("items")
        lines_stream_id = "{}_lines".format(self.tap_stream_id)
        mdata = stream.metadata
        try:
            line_mdata = [i for i in mdata if "JournalLines" in i.get("breadcrumb", [])]
        except IndexError:
            line_mdata = None

        if line_mdata:
            singer.write_schema(
                lines_stream_id,
                lines_schema,
                ["JournalLineID"]
            )

        if line_mdata is None:
            line_mdata = []

        for rec in records:
            with Transformer() as transformer:
                rec = transformer.transform(rec, schema, metadata.to_map(mdata))
                singer.write_record(self.tap_stream_id, rec)
            if "JournalLines" in rec and len(line_mdata) > 0 and ctx.config.get("journal_lines_stream") in ["true", True]:
                for line in rec["JournalLines"]:
                    with Transformer() as transformer:
                        line = transformer.transform(line, lines_schema, metadata.to_map(line_mdata))
                        singer.write_record(lines_stream_id, line)
        self.metrics(records)


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

    # REPORTS STREAM
    ReportStream("reports_profit_and_loss", ["from_date"], bookmark_key="to_date"),
    ReportStream("reports_balance_sheet", ["from_date"], bookmark_key="to_date")
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
