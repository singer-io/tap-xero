#!/usr/bin/env python3
import os
import singer
from singer import metrics, utils
from collections import namedtuple
from .pull import (
    CREDENTIALS_KEYS,
    IncrementingPull,
    PaginatedPull,
    JournalPull,
    LinkedTransactionsPull,
    EverythingPull,
)
from .format import (
    format_datetimes,
    format_dates,
)
import json
import attr

REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

LOGGER = singer.get_logger()


@attr.attributes
class Stream(object):
    tap_stream_id = attr.attr()
    pk_fields = attr.attr()
    puller = attr.attr()
    datetime_fields = attr.attr(default=[])
    date_fields = attr.attr(default=[])

STREAMS = [
    # PAGINATED STREAMS
    # These endpoints have all the best properties: they return the
    # UpdatedDateUTC property and support the Modified After, order, and page
    # parameters
    Stream("bank_transactions", ["BankTransactionID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date", "DateString"]),
    Stream("contacts", ["ContactID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"]),
    Stream("credit_notes", ["CreditNoteID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date",
                        "DateString",
                        "FullyPaidOnDate",
                        ["Allocations", "*", "Date"]]),
    Stream("invoices", ["InvoiceID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date",
                        "DateString",
                        "DueDate",
                        "DueDateString",
                        "ExpectedPaymentDate",
                        "PlannedPaymentDate",
                        "FullyPaidOnDate",
                        ["CreditNotes", "*", "Date"],
                        ["CreditNotes", "*", "DateString"],
                        ["Payments", "*", "Date"]]),
    Stream("manual_journals", ["ManualJournalID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date"]),
    Stream("overpayments", ["OverpaymentID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date", "DateString"]),
    Stream("prepayments", ["PrepaymentID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date", "DateString"]),
    Stream("purchase_orders", ["PurchaseOrderID"], PaginatedPull,
           datetime_fields=["UpdatedDateUTC",
                            ["Contact", "UpdatedDateUTC"]],
           date_fields=["Date",
                        "DateString",
                        "DeliveryDate",
                        "DeliveryDateString",
                        "ExpectedArrivalDate",
                        "ExpectedArrivalDateString"]),

    # JOURNALS STREAM
    # This endpoint is paginated, but in its own special snowflake way.
    Stream("journals", ["JournalID"], JournalPull,
           datetime_fields=["CreatedDateUTC"],
           date_fields=["JournalDate"]),

    # NON-PAGINATED STREAMS
    # These endpoints do not support pagination, but do support the Modified At
    # header.
    Stream("accounts", ["AccountID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"]),
    Stream("bank_transfers", ["BankTransferID"], IncrementingPull,
           datetime_fields=["CreatedDateUTC", "CreatedDateUTCString"],
           date_fields=["Date", "DateString"]),
    Stream("employees", ["EmployeeID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"]),
    Stream("expense_claims", ["ExpenseClaimID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC", ["User", "UpdatedDateUTC"]],
           date_fields=["PaymentDueDate", "ReportingDate"]),
    Stream("items", ["ItemID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"]),
    Stream("payments", ["PaymentID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date"]),
    Stream("receipts", ["ReceiptID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"],
           date_fields=["Date"]),
    Stream("users", ["UserID"], IncrementingPull,
           datetime_fields=["UpdatedDateUTC"]),

    # PULL EVERYTHING STREAMS
    # These endpoints do not support the Modified After header (or paging), so
    # we must pull all the data each time.
    Stream("branding_themes", ["BrandingThemeID"], EverythingPull,
           datetime_fields=["CreatedDateUTC"]),
    Stream("contact_groups", ["ContactGroupID"], EverythingPull),
    Stream("currencies", ["Code"], EverythingPull),
    Stream("organisations", ["OrganisationID"], EverythingPull,
           datetime_fields=["CreatedDateUTC"],
           date_fields=["PeriodLockDate", "EndOfYearLockDate"]),
    Stream("repeating_invoices", ["RepeatingInvoiceID"], EverythingPull,
           date_fields=[["Schedule", "StartDate"],
                        ["Schedule", "EndDate"],
                        ["Schedule", "NextScheduledDate"]]),
    Stream("tax_rates", ["TaxType"], EverythingPull),
    Stream("tracking_categories", ["TrackingCategoryID"], EverythingPull),

    # LINKED TRANSACTIONS STREAM
    # This endpoint is not paginated, but can do some manual filtering
    Stream("linked_transactions", ["LinkedTransactionID"], LinkedTransactionsPull,
           datetime_fields=["UpdatedDateUTC"]),
]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    if "definitions" not in schema:
        schema["definitions"] = {}
    for sub_stream_id in list(schema["definitions"].keys()):
        sub_schema = load_schema(sub_stream_id)
        schema["definitions"][sub_stream_id] = sub_schema
        if "definitions" in sub_schema:
            schema["definitions"].update(sub_schema.pop("definitions"))
    return schema


def discover():
    result = {"streams": []}
    for stream in STREAMS:
        result["streams"].append(
            dict(stream=stream.tap_stream_id,
                 tap_stream_id=stream.tap_stream_id,
                 key_properties=stream.pk_fields,
                 schema=load_schema(stream.tap_stream_id))
        )
    return result


def do_discover():
    print(json.dumps(discover(), indent=4))


def run_stream(config, state, stream):
    state["currently_syncing"] = stream.tap_stream_id
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)
    puller = stream.puller(config, state, stream.tap_stream_id)
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for page in puller.yield_pages():
            counter.increment(len(page))
            for item in page:
                format_datetimes(stream.datetime_fields, item)
                format_dates(stream.date_fields, item)
            singer.write_records(stream.tap_stream_id, page)
            singer.write_state(state)
    singer.write_state(state)


def sync(config, state, catalog):
    currently_syncing = state.get("currently_syncing")
    start_idx = [e.tap_stream_id for e in STREAMS].index(currently_syncing) \
        if currently_syncing \
        else 0
    stream_ids_to_sync = [c["tap_stream_id"] for c in catalog["streams"]]
    for stream in STREAMS[start_idx:]:
        if stream.tap_stream_id not in stream_ids_to_sync:
            continue
        run_stream(config, state, stream)
    state["currently_syncing"] = None
    singer.write_state(state)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    state = args.state
    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else discover()
        sync(config, state, catalog)

if __name__ == "__main__":
    main()
