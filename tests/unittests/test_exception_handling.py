import tap_xero.client as client_
import unittest
import requests
from unittest import mock
import decimal
import json

json_decode_error_str = '{\'key\': \'value\'}'

def mocked_session(*args, **kwargs):
    class Mocksession:
        def __init__(self, json_data, status_code):
            self.text = json_data
            self.status_code = status_code
        
        def raise_for_status(self):
            return self.status_code
        
    return Mocksession(args[0], 200)

def mocked_request(*args, **kwargs):
    class Mockresponse:
        def __init__(self, resp):
            self.json_data = resp

        def prepare(self):
            return self.json_data

    return Mockresponse(json_decode_error_str)

class TestFilterFunExceptionHandling(unittest.TestCase):
    """
    Test cases to verify if the exceptions are handled as expected while communicating with Xero Environment 
    """

    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_request)
    def test_json_decode_exception(self, mocked_session, mocked_request):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"
        try:
            filter_func_exec = xero_client.filter(tap_stream_id)
        except json.decoder.JSONDecodeError as e:
            pass

        self.assertEqual(mocked_request.call_count, 3)
        self.assertEqual(mocked_session.call_count, 3)

    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_request)
    def test_normal_filter_execution(self, mocked_session, mocked_request):
        config = {}
        tap_stream_id = "contacts"

        global json_decode_error_str
        json_decode_error_str = '{"Contacts": "value"}'

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"
        try:
            filter_func_exec = xero_client.filter(tap_stream_id)
        except json.decoder.JSONDecodeError as e:
            pass

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(mocked_session.call_count, 1)


