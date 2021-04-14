import json

import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

from base import XeroScenarioBase

class XeroBookmarks(XeroScenarioBase):
    def name(self):
        return "tap_tester_xero_common_connection"

    def check_output_record_counts(self):
        counts_by_stream = runner.examine_target_output_file(
            self, self.conn_id, self.expected_streams, self.expected_pks)
        replicated_row_count = sum(counts_by_stream.values())
        self.assertGreater(
            replicated_row_count, 0,
            msg="failed to replicate any data: {}".format(counts_by_stream)
        )
        print("total replicated row count: {}".format(replicated_row_count))

    def check_bookmarks(self, bookmarks, max_bookmarks_from_records):
        """Checks that the bookmarks in the state match the maximum values
        found in the records themselves."""
        # if we didn't replicate data, the bookmark should be the default
        for stream in self.expected_bookmarks:
            if stream not in max_bookmarks_from_records:
                max_bookmarks_from_records[stream] = self.get_bookmark_default(stream)
        for stream, bk_names in self.expected_bookmarks.items():
            for bk_name in bk_names:
                bk_value = bookmarks.get(stream, {}).get(bk_name)
                self.assertIsNotNone(
                    bk_value,
                    msg="stream '{}' had no bookmark '{}' bookmarks: {}"
                    .format(stream, bk_name, json.dumps(bookmarks, indent=2))
                )
                max_bk_found = max_bookmarks_from_records[stream]
                self.assertEqual(
                    self.typify_bookmark(stream, bk_name, bk_value),
                    self.typify_bookmark(stream, bk_name, max_bk_found),
                    "Bookmark {} for stream {} should have been updated to {}"
                    .format(bk_value, stream, max_bk_found)
                )
                print("bookmark {}({}) updated to {}"
                      .format(stream, bk_name, bk_value))

    def check_offsets(self, bookmarks):
        for stream, offset in self.expected_offsets.items():
            self.assertEqual(
                bookmarks.get(stream, {}).get("offset"), offset,
                msg=("unexpected offset found for stream {} {}. bookmarks: {}"
                     .format(stream, offset, bookmarks))
            )
            print("offsets {} cleared".format(stream))

    def test_run(self):
        runner.run_check_job_and_check_status(self)

        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.check_all_streams_in_catalogs(found_catalogs)
        self.select_found_catalogs(found_catalogs)

        # clear state and run the actual sync
        menagerie.set_state(self.conn_id, {})
        runner.run_sync_job_and_check_status(self)
        self.check_output_record_counts()

        max_bookmarks_from_records = runner.get_max_bookmarks_from_target(self)
        state = menagerie.get_state(self.conn_id)
        bookmarks = state.get("bookmarks", {})
        self.check_bookmarks(bookmarks, max_bookmarks_from_records)
        self.check_offsets(bookmarks)
        self.look_for_unexpected_bookmarks(bookmarks)
        self.assertIsNone(state.get("currently_syncing"))
