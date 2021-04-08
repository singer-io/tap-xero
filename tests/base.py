import os
from datetime import datetime, timezone
import unittest
from singer import utils

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie


def preserve_refresh_token(existing_conns, payload):
    if not existing_conns:
        return payload
    conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
    payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
    return payload

class XeroScenarioBase(unittest.TestCase):
    start_dt = datetime(2001, 1, 1, tzinfo=timezone.utc)
    start_date = utils.strftime(start_dt)

    def setUp(self):
        required_creds = {
            "client_id": 'TAP_XERO_CLIENT_ID',
            "client_secret": 'TAP_XERO_CLIENT_SECRET',
            "refresh_token": 'TAP_XERO_REFRESH_TOKEN',
        }
        required_props = {
            "tenant_id": 'TAP_XERO_TENANT_ID',
            "xero_user_id": 'TAP_XERO_USER_ID'
        }
        missing_creds = [v for v in required_creds.values() if not os.getenv(v)]
        missing_props = [v for v in required_props.values() if not os.getenv(v)]
        if missing_creds or missing_props:
            missing_envs = missing_creds + missing_props
            raise Exception("set " + ", ".join(missing_envs))
        self._credentials = {k: os.getenv(v) for k, v in required_creds.items()}
        self.conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)

    def get_type(self):
        return "platform.xero"

    def get_credentials(self):
        self._credentials["client_secret"] = os.getenv('TAP_XERO_CLIENT_SECRET')
        self._credentials["client_id"] = os.getenv('TAP_XERO_CLIENT_ID')
        self._credentials["refresh_token"] = os.getenv('TAP_XERO_REFRESH_TOKEN')
        self._credentials["access_token"] = "access_token"
        return self._credentials

    @property
    def expected_pks(self):
        return {
            "bank_transactions": ["BankTransactionID"],
            "contacts": ["ContactID"],
            "credit_notes": ["CreditNoteID"],
            "invoices": ["InvoiceID"],
            "manual_journals": ["ManualJournalID"],
            "overpayments": ["OverpaymentID"],
            "prepayments": ["PrepaymentID"],
            "purchase_orders": ["PurchaseOrderID"],
            "journals": ["JournalID"],
            "accounts": ["AccountID"],
            "bank_transfers": ["BankTransferID"],
            "employees": ["EmployeeID"],
            "expense_claims": ["ExpenseClaimID"],
            "items": ["ItemID"],
            "payments": ["PaymentID"],
            "receipts": ["ReceiptID"],
            "users": ["UserID"],
            "branding_themes": ["BrandingThemeID"],
            "contact_groups": ["ContactGroupID"],
            "currencies": ["Code"],
            "organisations": ["OrganisationID"],
            "repeating_invoices": ["RepeatingInvoiceID"],
            "tax_rates": ["TaxType"],
            "tracking_categories": ["TrackingCategoryID"],
            "linked_transactions": ["LinkedTransactionID"],
        }

    @property
    def expected_streams(self):
        return set(self.expected_pks)

    @property
    def expected_bookmarks(self):
        return {
            "bank_transactions": ["UpdatedDateUTC"],
            "contacts": ["UpdatedDateUTC"],
            "credit_notes": ["UpdatedDateUTC"],
            "invoices": ["UpdatedDateUTC"],
            "manual_journals": ["UpdatedDateUTC"],
            "overpayments": ["UpdatedDateUTC"],
            "prepayments": ["UpdatedDateUTC"],
            "purchase_orders": ["UpdatedDateUTC"],
            "journals": ["JournalNumber"],
            "accounts": ["UpdatedDateUTC"],
            "bank_transfers": ["CreatedDateUTC"],
            "employees": ["UpdatedDateUTC"],
            "expense_claims": ["UpdatedDateUTC"],
            "items": ["UpdatedDateUTC"],
            "payments": ["UpdatedDateUTC"],
            "receipts": ["UpdatedDateUTC"],
            "users": ["UpdatedDateUTC"],
            "linked_transactions": ["UpdatedDateUTC"],
        }

    @property
    def expected_offsets(self):
        return {
            "bank_transactions": {},
            "contacts": {},
            "credit_notes": {},
            "invoices": {},
            "manual_journals": {},
            "overpayments": {},
            "prepayments": {},
            "purchase_orders": {},
            "linked_transactions": {},
        }

    def record_to_bk_value(self, stream, record):
        if stream == "journals":
            return record.get("JournalNumber")
        if stream == "bank_transfers":
            return record.get("CreatedDateUTC")
        return record.get("UpdatedDateUTC")

    def tap_name(self):
        return "tap-xero"

    def get_properties(self):
        return {
            "start_date" : self.start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tenant_id": os.getenv('TAP_XERO_TENANT_ID'),
            "xero_user_id": os.getenv('TAP_XERO_USER_ID'),
        }

    def get_bookmark_default(self, stream):
        if stream == "journals":
            return 0
        return self.get_properties()["start_date"]

    def typify_bookmark(self, stream, bookmark_name, bookmark_val):
        try:
            if stream == "journals":
                return int(bookmark_val)
            return utils.strptime_with_tz(bookmark_val)
        except Exception as e:
            raise Exception("Couldn't cast (stream:{}) (bk_name:{}) (bk_val:{})"
                            .format(stream, bookmark_name, bookmark_val)) from e

    def check_all_streams_in_catalogs(self, found_catalogs):
        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        diff = self.expected_streams.symmetric_difference(found_catalog_names)
        self.assertEqual(
            len(diff), 0,
            msg="discovered schemas do not match: {}".format(diff)
        )
        print("discovered schemas are kosher")

    def select_found_catalogs(self, found_catalogs):
        # selected = [menagerie.select_catalog(self.conn_id, c) for c in found_catalogs]
        # menagerie.post_annotated_catalogs(self.conn_id, selected)
        for catalog in found_catalogs:
            schema = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
            non_selected_properties = []
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                self.conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )


    def select_specific_catalog(self, found_catalogs, catalog_to_select):
        for catalog in found_catalogs:
            if catalog['tap_stream_id'] != catalog_to_select:
                continue

            schema = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
            non_selected_properties = []
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                self.conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
            )
            break


    def look_for_unexpected_bookmarks(self, bookmarks):
        diff = set(bookmarks).difference(self.expected_bookmarks)
        self.assertEqual(
            len(diff), 0,
            msg=("Unexpected bookmarks: {} Expected: {} Actual: {}"
                 .format(diff, self.expected_bookmarks, bookmarks))
        )
