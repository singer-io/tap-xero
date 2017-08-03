import unittest
import singer
from singer import metrics
import mock
import tap_xero
from tap_xero.pull import (
    IncrementingPull,
    PaginatedPull,
    JournalPull,
    LinkedTransactionsPull,
)
from tap_xero.tests import utils

LOGGER = singer.get_logger()
metrics.log = utils.metric_log


class TestPaginatedBookmarks(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        config = utils.get_test_config()
        self.state = {"bookmarks": {"bank_transactions": {}}}
        self.puller = PaginatedPull(config, self.state, "bank_transactions")

    def _get_updated_at(self):
        return self.state["bookmarks"]["bank_transactions"]["updated_at"]

    def _get_page_state(self):
        return self.state["bookmarks"]["bank_transactions"]["page"]

    def test_bookmarks(self):
        num_pages = 0
        num_records = 0
        last_updated_at = None
        for page in self.puller.yield_pages():
            num_pages += 1
            num_records += len(page)
            self.assertEqual(self._get_page_state(), num_pages + 1)
            self.assertNotEqual(self._get_updated_at(), last_updated_at)
            last_updated_at = self._get_updated_at()
        self.assertIsNone(self._get_page_state())
        self.assertIsNotNone(last_updated_at)
        self.assertGreater(num_records, 0)

        # Now, if we sync again we should not have any more records
        num_records = 0
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertEqual(num_records, 0)

    def test_page_in_initial_state(self):
        self.state["bookmarks"]["bank_transactions"]["page"] = 2
        num_records = 0
        last_updated_at = None
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertIsNotNone(self._get_updated_at())
        self.assertEqual(num_records, 0)


class TestIncrementalBookmarks(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        config = utils.get_test_config()
        self.state = {}
        self.puller = IncrementingPull(config, self.state, "accounts")

    def _get_updated_at(self):
        return self.state["bookmarks"]["accounts"]["updated_at"]

    def test_bookmarks(self):
        num_pages = 0
        num_records = 0
        for page in self.puller.yield_pages():
            num_pages += 1
            num_records += len(page)
        self.assertIsNotNone(self._get_updated_at())
        self.assertEqual(num_pages, 1)
        self.assertGreater(num_records, 0)

        # Now, if we sync again we should not have any more records
        num_records = 0
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertEqual(num_records, 0)


class TestJournalBookmarks(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        config = utils.get_test_config()
        self.state = {"bookmarks": {"journals": {}}}
        self.puller = JournalPull(config, self.state, "journals")

    def _get_updated_at(self):
        return self.state["bookmarks"]["journals"].get("updated_at")

    def _get_offset_state(self):
        return self.state["bookmarks"]["journals"]["journal_number"]

    def test_bookmarks(self):
        num_pages = 0
        num_records = 0
        current_offset = 0
        for page in self.puller.yield_pages():
            num_pages += 1
            num_records += len(page)
            self.assertGreater(self._get_offset_state(), current_offset)
            self.assertIsNone(self._get_updated_at())
        self.assertGreater(num_pages, 1)
        self.assertGreater(num_records, 100)
        self.assertIsNotNone(self._get_offset_state())
        self.assertIsNone(self._get_updated_at())

        # Now, if we sync again we should not have any more records
        num_records = 0
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertEqual(num_records, 0)


class TestLinkedTransactionsBookmarks(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        config = utils.get_test_config()
        self.state = {"bookmarks": {"linked_transactions": {}}}
        self.puller = LinkedTransactionsPull(config, self.state, "linked_transactions")

    def _get_updated_at(self):
        return self.state["bookmarks"]["linked_transactions"]["updated_at"]

    def test_bookmarks(self):
        num_pages = 0
        num_records = 0
        for page in self.puller.yield_pages():
            num_pages += 1
            num_records += len(page)
        self.assertIsNotNone(self._get_updated_at())
        self.assertEqual(num_pages, 1)
        self.assertGreater(num_records, 0)

        # Now, if we sync again we should not have any more records
        num_records = 0
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertEqual(num_records, 0)

    def test_page_in_initial_state(self):
        self.state["bookmarks"]["linked_transactions"]["page"] = 2
        num_records = 0
        last_updated_at = None
        for page in self.puller.yield_pages():
            num_records += len(page)
        self.assertIsNotNone(self._get_updated_at())
        self.assertEqual(num_records, 0)
