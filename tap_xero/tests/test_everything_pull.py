import unittest
import singer
from singer import metrics
import mock
import tap_xero
from tap_xero.pull import EverythingPull
from tap_xero.tests import utils

LOGGER = singer.get_logger()
metrics.log = utils.metric_log


class TestEverythingPull(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        config = utils.get_test_config()
        self.state = {}
        self.puller = EverythingPull(config, self.state, "repeating_invoices")

    def test_everything_pull(self):
        num_pages = 0
        num_records = 0
        for page in self.puller.yield_pages():
            num_pages += 1
            num_records += len(page)
        self.assertEqual(num_pages, 1)
        self.assertGreater(num_records, 0)

        # Now, if we sync again we should get the same records
        num_records_2 = 0
        for page in self.puller.yield_pages():
            num_records_2 += len(page)
        self.assertEqual(num_records_2, num_records)
