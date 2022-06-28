from tap_tester import menagerie, runner
from base import XeroScenarioBase

# Map of fields that need to be skipped as they are not appearing in the response
FIELDS_TO_SKIP = {
    'invoices': {
        'TotalDiscount', 
        'PlannedPaymentDateString',
        'ExpectedPaymentDate',
        'ExpectedPaymentDateString',
        'Url',
        'PlannedPaymentDate',
        'CISDeduction'
    },
    'purchase_orders': {
        'TotalDiscount',
        'SentToContact'
    },
    'quotes': {
        'Summary',
        'ExpiryDate',
        'Reference',
        'Terms',
        'Title',
        'TrackingCategory'
    },
    'prepayments': {
        'AppliedAmount',
        'CurrencyRate',
        'Reference'
    },
    'bank_transactions': {
        'Url',
        'CurrencyRate',
        'ExternalLinkProviderName',
        'BatchPayment'
    },
    'credit_notes': {
        'AppliedAmount',
        'SentToContact',
        'DueDate',
        'DueDateString',
        'CISDeduction'
    },
    'users': {'ValidationErrors'},
    'payments': {
        'CreditNote',
        'Overpayment',
        'Prepayments'
    },
    'organisations': {
        'RegistrationNumber',
        'LineOfBusiness',
        'EndOfYearLockDate',
        'APIKey',
        'EmployerIdentificationNumber'
    },
    'contact_groups': {
        'Contacts'
    },
    'tracking_categories': {
        'TrackingOptionName',
        'TrackingCategoryName',
        'TrackingOptionID',
        'Option'
    },
    'repeating_invoices': {'TotalDiscount'},
    'manual_journals': {'Url'},
    'overpayments': {
        'AppliedAmount',
        'CurrencyRate',
        'ID',
        'Reference'
    },
    'receipts': {'Url'},
    # For `contacts` stream these fields are not retrieved in response.
    'contacts': {
        'PurchasesDefaultAccountCode',
        'ValidationErrors',
        'MergedToContactID ',
        'PurchasesDefaultLineAmountType',
        'Website',
        'Discount',
        'PurchasesTrackingCategories',
        'SalesDefaultAccountCode',
        'PaymentTerms',
        'SalesTrackingCategories',
        'XeroNetworkKey',
        'TrackingCategoryName',
        'CompanyNumber',
        'Attachments',
        'SalesDefaultLineAmountType',
        'AccountNumber',
        'TrackingCategoryOption'
    },
    'tax_rates': {'ElementsForTaxComponents'},
}

class TestXeroAllFields(XeroScenarioBase):

    """Ensure running the tap with all streams and fields selected results in the replication of all fields."""

    def name(self):
        return "tap_tester_xero_all_fields"
    
    def test_all_fields_run(self):

        """
        Assert given conditions:
        • verify no unexpected streams are replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • Verify all fields for each streams are replicated

        """
        # Streams to verify all fields tests
        # As we don't have any records for given steams we are skipping them
        expected_streams = self.expected_streams - { 'linked_transactions' }
        
        expected_automatic_fields = self.expected_automatic_fields()

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)
        
        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get("stream_name") in expected_streams]

        # perform table and field selections
        self.perform_and_verify_table_and_field_selection(self.conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table and field selection to set expections
        # used for asserting all fields are replicated
        stream_to_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(self.conn_id, stream_id)
            fields_from_field_level_md = [metadata["breadcrumb"][1]
                                        for metadata in catalog_entry["metadata"]
                                        if metadata["breadcrumb"] != []]
            stream_to_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run sync mode
        stream_to_record_count = self.run_and_verify_sync(self.conn_id)

        # get records from target output
        synced_recs = runner.get_records_from_target_output()
        
        # verify no unexpected streams are replicated
        synced_streams_names = set(stream_to_record_count.keys())
        self.assertSetEqual(expected_streams,synced_streams_names)

        for stream in synced_streams_names:
            with self.subTest(stream=stream):

                # expected values for stream
                expected_all_keys = stream_to_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields[stream]

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                # actual values
                actual_all_keys = set()
                for message in synced_recs.get(stream).get("messages"):
                    if message["action"] == "upsert":
                        actual_all_keys = actual_all_keys.union(
                            set(message["data"].keys())
                        )
                
                # verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys - FIELDS_TO_SKIP.get(stream,set()),actual_all_keys,
                                    msg=f"Expected and actual all keys are not same for {stream} stream")