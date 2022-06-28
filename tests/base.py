import os
from datetime import datetime, timezone
import unittest
from singer import utils

import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from datetime import datetime as dt
from datetime import timedelta


def preserve_refresh_token(existing_conns, payload):
    if not existing_conns:
        return payload
    conn_with_creds = connections.fetch_existing_connection_with_creds(existing_conns[0]['id'])
    payload['properties']['refresh_token'] = conn_with_creds['credentials']['refresh_token']
    return payload

class XeroScenarioBase(unittest.TestCase):
    start_date = "2011-06-14T00:00:00Z"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_KEYS = "valid-replication-keys"
    REPLICATION_METHOD = "forced-replication-method"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

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

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            "bank_transactions": {
                self.PRIMARY_KEYS:{"BankTransactionID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "contacts": {
                self.PRIMARY_KEYS:{"ContactID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "credit_notes": {
                self.PRIMARY_KEYS:{"CreditNoteID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "invoices": {
                self.PRIMARY_KEYS:{"InvoiceID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "manual_journals": {
                self.PRIMARY_KEYS:{"ManualJournalID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "overpayments": {
                self.PRIMARY_KEYS:{"OverpaymentID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "prepayments": {
                self.PRIMARY_KEYS:{"PrepaymentID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "purchase_orders": {
                self.PRIMARY_KEYS:{"PurchaseOrderID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "journals": {
                self.PRIMARY_KEYS:{"JournalID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"JournalNumber"}
            },
            "accounts": {
                self.PRIMARY_KEYS:{"AccountID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "bank_transfers": {
                self.PRIMARY_KEYS:{"BankTransferID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"CreatedDateUTC"}
            },
            "employees": {
                self.PRIMARY_KEYS:{"EmployeeID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "expense_claims": {
                self.PRIMARY_KEYS:{"ExpenseClaimID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "items": {
                self.PRIMARY_KEYS:{"ItemID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "payments": {
                self.PRIMARY_KEYS:{"PaymentID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "receipts": {
                self.PRIMARY_KEYS:{"ReceiptID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "users": {
                self.PRIMARY_KEYS:{"UserID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "branding_themes": {
                self.PRIMARY_KEYS:{"BrandingThemeID"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "contact_groups": {
                self.PRIMARY_KEYS:{"ContactGroupID"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "currencies": {
                self.PRIMARY_KEYS:{"Code"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "organisations": {
                self.PRIMARY_KEYS:{"OrganisationID"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "repeating_invoices": {
                self.PRIMARY_KEYS:{"RepeatingInvoiceID"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "tax_rates": {
                self.PRIMARY_KEYS:{"TaxType"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "tracking_categories": {
                self.PRIMARY_KEYS:{"TrackingCategoryID"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "linked_transactions": {
                self.PRIMARY_KEYS:{"LinkedTransactionID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
            "quotes": {
                self.PRIMARY_KEYS:{"QuoteID"},
                self.REPLICATION_METHOD: self.INCREMENTAL, 
                self.REPLICATION_KEYS: {"UpdatedDateUTC"}
            },
        }

    @property
    def expected_pks(self):
        return {table: properties.get(self.PRIMARY_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    @property
    def expected_streams(self):
        return set(self.expected_metadata().keys())

    @property
    def expected_bookmarks(self):
        return {table: properties.get(self.REPLICATION_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

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
            "start_date" : self.start_date,
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

    def expected_replication_method(self):
        """Return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """Return a dictionary with key of table name and set of value of automatic(primary key and bookmark field) fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) |  v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        print(found_catalog_names)
        self.assertSetEqual(self.expected_streams, found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs
    
    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_streams,
                                                              self.expected_pks)

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count
    
    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))
            
    def parse_date(self, date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(
            "Tests do not account for dates of this format: {}".format(date_value))