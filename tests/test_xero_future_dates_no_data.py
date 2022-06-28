from datetime import datetime, timezone
from singer import utils

import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import XeroScenarioBase

class XeroFutureDatesNoData(XeroScenarioBase):
    def name(self):
        return "tap_tester_xero_common_connection"

    @property
    def state(self):
        future_dt = datetime(2050, 1, 1, tzinfo=timezone.utc)
        future_date = utils.strftime(future_dt)
        return {
            "currently_syncing": None,
            "bookmarks": {
                "bank_transactions": {"UpdatedDateUTC": future_date},
                "contacts": {"UpdatedDateUTC": future_date},
                "credit_notes": {"UpdatedDateUTC": future_date},
                "invoices": {"UpdatedDateUTC": future_date},
                "manual_journals": {"UpdatedDateUTC": future_date},
                "overpayments": {"UpdatedDateUTC": future_date},
                "prepayments": {"UpdatedDateUTC": future_date},
                "purchase_orders": {"UpdatedDateUTC": future_date},
                "journals": {"JournalNumber": 10e10},
                "accounts": {"UpdatedDateUTC": future_date},
                "bank_transfers": {"CreatedDateUTC": future_date},
                "employees": {"UpdatedDateUTC": future_date},
                "expense_claims": {"UpdatedDateUTC": future_date},
                "items": {"UpdatedDateUTC": future_date},
                "payments": {"UpdatedDateUTC": future_date},
                "receipts": {"UpdatedDateUTC": future_date},
                "users": {"UpdatedDateUTC": future_date},
                "linked_transactions": {"UpdatedDateUTC": future_date},
                "quotes": {"UpdatedDateUTC": future_date}
            }
        }

    def test_run(self):
        runner.run_check_job_and_check_status(self)
        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.select_found_catalogs(found_catalogs)

        # clear state and run the actual sync
        menagerie.set_state(self.conn_id, self.state)
        runner.run_sync_job_and_check_status(self)

        counts_by_stream = runner.examine_target_output_file(
            self, self.conn_id, self.expected_streams, self.expected_pks)
        for stream in self.state["bookmarks"]:
            if stream == 'journals':
                # Seems like this endpoint used to return nothing if the
                # offset was high enough, but that is not the case anymore
                # and we get a page of journals back
                continue
            record_count = counts_by_stream.get(stream, 0)
            self.assertEqual(
                record_count, 0,
                msg=("Stream {} had {} rows instead of 0"
                     .format(stream, record_count))
            )
