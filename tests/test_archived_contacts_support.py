import json

import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import XeroScenarioBase

class TestArchivedContacts(XeroScenarioBase):

    def name(self):
        return "tap_tester_xero_common_connection"

    def get_properties(self):
        properties = super().get_properties()
        properties["includeArchivedContacts"] = "true"

        return properties


    def test_get_archived_contacts(self):
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.check_all_streams_in_catalogs(found_catalogs)
        # Replicating the contacts stream
        self.select_specific_catalog(found_catalogs, "contacts")

        # clear state and run the actual sync
        menagerie.set_state(self.conn_id, {})
        runner.run_sync_job_and_check_status(self)
        records = runner.get_upserts_from_target_output()

        contacts_status = [record["ContactStatus"] for record in records]
        self.assertIn("ARCHIVED", contacts_status)
