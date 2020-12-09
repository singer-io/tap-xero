from tap_xero.client import parse_date
from singer.utils import strptime_to_utc
import unittest
import datetime

class TestDatetimeParsing(unittest.TestCase):

    def test_normal_datetimes(self):
        dates = [
            '2020-10-20T12:30:00Z',
            '2020-01-02T16:30:00Z',
            '2020-01-01T12:30:00+0',
        ]

        parsed_dates = [parse_date(x) for x in dates]

        expected_dates = [
            strptime_to_utc('2020-10-20T12:30:00Z'),
            strptime_to_utc('2020-01-02T16:30:00Z'),
            strptime_to_utc('2020-01-01T12:30:00+0'),
        ]

        self.assertEquals(parsed_dates, expected_dates)

    def test_epoch_datetimes(self):
        dates = [
            '/Date(1603895333000+0000)/',
            '/Date(814890533000+0000)/',
            '/Date(1130509733000+0000)/',
            '/Date(-1565568000000+0000)/',
        ]

        parsed_dates = [parse_date(x) for x in dates]

        expected_dates = [
            datetime.datetime(2020, 10, 28, 14, 28, 53),
            datetime.datetime(1995, 10, 28, 14, 28, 53),
            datetime.datetime(2005, 10, 28, 14, 28, 53),
            datetime.datetime(1920, 5, 23, 00, 00, 00),
        ]

        self.assertEquals(parsed_dates, expected_dates)

    def test_not_datetimes(self):
        dates = [
            '1023',
            'abcsdf',
            '0020',
            '0023'
        ]

        parsed_dates = [parse_date(x) for x in dates]

        expected_dates = [None, None, None, None]

        self.assertEquals(parsed_dates, expected_dates)
