import tap_tester.runner as runner
from base import XeroScenarioBase


class XeroPagination(XeroScenarioBase):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """
    API_LIMIT = 100
    def name(self):
        return "tap_tester_xero_pagination_test"

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """

        # Streams to verify pagination tests
        pagination_supported_streams = {"bank_transactions","contacts","quotes","credit_notes","invoices",
                                        "manual_journals","overpayments","payments","prepayments", "purchase_orders"}

        # We are not able to generate enough data to test pagination for the
        # `prepayments`, `payments`, `overpayments` streams.
        # Hence, removing it from expected_streams set.
        expected_streams = pagination_supported_streams - {'prepayments', 'payments', 'overpayments'}

        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(self.conn_id, test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(self.conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_pks[stream]
         
                # verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync, self.API_LIMIT,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in synced_records.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                primary_keys_list_1 = primary_keys_list[:self.API_LIMIT]
                primary_keys_list_2 = primary_keys_list[self.API_LIMIT:2*self.API_LIMIT]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    primary_keys_page_1.isdisjoint(primary_keys_page_2))