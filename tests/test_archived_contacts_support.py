import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections

from base import XeroScenarioBase, preserve_refresh_token

class TestArchivedContacts(XeroScenarioBase):

    def setUp(self):
        self.include_archived_contacts = None
        super().setUp()

    def name(self):
        return "tap_tester_xero_common_connection"

    def get_properties(self):
        properties = super().get_properties()

        # include_archived_contacts is an optional property for configuration
        if self.include_archived_contacts:
            properties["include_archived_contacts"] = self.include_archived_contacts

        return properties

    def get_records_from_xero_platform(self):
        only_active_contacts = runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.check_all_streams_in_catalogs(found_catalogs)
        # Replicating the contacts stream
        self.select_specific_catalog(found_catalogs, "contacts")

        # clear state and run the actual sync
        menagerie.set_state(self.conn_id, {})
        runner.run_sync_job_and_check_status(self)
        records = runner.get_upserts_from_target_output()

        return records


    def test_get_archived_contacts(self):
        # Tap-Xero be default will collect only active records
        only_active_records = self.get_records_from_xero_platform()
        contacts_status_1 = [record["ContactStatus"] for record in only_active_records]
        number_of_contacts_received_1 = len(only_active_records)

        # Verifying that no ARCHIVED contacts are returned
        self.assertEqual(True, "ARCHIVED" not in contacts_status_1)
        self.assertSetEqual({"ACTIVE"}, set(contacts_status_1))

        # Configuring the tap to collect Archived records as well
        self.include_archived_contacts = "true"
        self.conn_id = connections.ensure_connection(self, payload_hook=preserve_refresh_token)
        # Tap-Xero be default should now collect Active and archived records
        active_and_archived_records = self.get_records_from_xero_platform()
        contacts_status_2 = [record["ContactStatus"] for record in active_and_archived_records]
        number_of_contacts_received_2 = len(active_and_archived_records)

        # Verifying that ARCHIVED and ACTIVE contacts are returned
        self.assertSetEqual({"ACTIVE", "ARCHIVED"}, set(contacts_status_2))

        # Verifying that second sync provided more contacts then the first sync
        self.assertGreater(number_of_contacts_received_2, number_of_contacts_received_1)
