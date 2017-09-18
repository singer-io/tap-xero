#!/usr/bin/env python3
import os
import singer
from singer import metrics, utils
from singer.catalog import Catalog
from .pull import (
    IncrementingPull,
    BankTransfersPull,
    PaginatedPull,
    JournalPull,
    LinkedTransactionsPull,
    EverythingPull,
)
from . import credentials
import json
import attr

CREDENTIALS_KEYS = ["consumer_key",
                    "consumer_secret",
                    "rsa_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

LOGGER = singer.get_logger()

BAD_CREDS_MESSAGE = (
    "Failed to refresh OAuth token using the credentials from the connection. "
    "The token might need to be reauthorized from the integration's properties "
    "or there could be another authentication issue. Please attempt to reauthorize "
    "the integration."
)


class BadCredsException(Exception):
    pass


@attr.attributes
class Stream(object):
    tap_stream_id = attr.attr()
    pk_fields = attr.attr()
    puller = attr.attr()

STREAMS = [
    # PAGINATED STREAMS
    # These endpoints have all the best properties: they return the
    # UpdatedDateUTC property and support the Modified After, order, and page
    # parameters
    Stream("bank_transactions", ["BankTransactionID"], PaginatedPull),
    Stream("contacts", ["ContactID"], PaginatedPull),
    Stream("credit_notes", ["CreditNoteID"], PaginatedPull),
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
    Stream("contact_groups", ["ContactGroupID"], EverythingPull),
    Stream("currencies", ["Code"], EverythingPull),
    Stream("organisations", ["OrganisationID"], EverythingPull),
    Stream("repeating_invoices", ["RepeatingInvoiceID"], EverythingPull),
    Stream("tax_rates", ["TaxType"], EverythingPull),
    Stream("tracking_categories", ["TrackingCategoryID"], EverythingPull),

    # LINKED TRANSACTIONS STREAM
    # This endpoint is not paginated, but can do some manual filtering
    Stream("linked_transactions", ["LinkedTransactionID"], LinkedTransactionsPull),
]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def discover():
    result = {"streams": []}
    for stream in STREAMS:
        schema = load_schema(stream.tap_stream_id)
        schema["selected"] = False
        result["streams"].append(
            dict(stream=stream.tap_stream_id,
                 tap_stream_id=stream.tap_stream_id,
                 key_properties=stream.pk_fields,
                 schema=schema)
        )
    return Catalog.from_dict(result)


def run_stream(config, state, stream):
    state["currently_syncing"] = stream.tap_stream_id
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)
    puller = stream.puller(config, state, stream.tap_stream_id)
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for page in puller.yield_pages():
            counter.increment(len(page))
            singer.write_records(stream.tap_stream_id, page)
            singer.write_state(state)
    singer.write_state(state)


def init_credentials(config):
    if credentials.can_use_s3(config):
        creds = credentials.download_from_s3(config)
        if creds:
            config.update(creds)
        else:
            # no creds means we have to try to use what's in the config
            # to refresh the token
            try:
                config = credentials.refresh(config)
            except Exception as ex:
                raise BadCredsException(BAD_CREDS_MESSAGE) from ex

    return config


def sync(config, state, catalog):
    init_credentials(config)
    currently_syncing = state.get("currently_syncing")
    start_idx = [e.tap_stream_id for e in STREAMS].index(currently_syncing) \
        if currently_syncing \
        else 0
    stream_ids_to_sync = [c.tap_stream_id for c in catalog.streams
                          if c.is_selected()]
    for stream in STREAMS[start_idx:]:
        if stream.tap_stream_id not in stream_ids_to_sync:
            continue
        run_stream(config, state, stream)
    state["currently_syncing"] = None
    singer.write_state(state)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover().dump()
        print()
    else:
        catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover()
        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
