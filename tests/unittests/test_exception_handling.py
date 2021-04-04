import tap_xero.client as client_
import unittest
import requests
from unittest import mock
import decimal
import json


def mocked_session(*args, **kwargs):
    class Mocksession:
        def __init__(self, json_data, status_code, content, headers, raise_error):
            self.text = json_data
            self.status_code = status_code
            self.raise_error = raise_error
            if headers:
                self.headers = headers

        def raise_for_status(self):
            if not raise_error:
                return self.status_code

            raise requests.HTTPError("sample message")

    arguments_to_session = args[0]

    json_data = arguments_to_session[0]
    status_code = arguments_to_session[1]
    content = arguments_to_session[2]
    headers = arguments_to_session[3]
    raise_error = arguments_to_session[4]
    return Mocksession(json_data, status_code, content, headers, raise_error)


class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)


def mocked_failed_429_request(*args, **kwargs):
    json_decode_str = ''
    headers = {"Retry-After": 10}
    return Mockresponse(json_decode_str, 429, headers=headers, raise_error=True)


class TestFilterFunExceptionHandling(unittest.TestCase):
    """
    Test cases to verify if the exceptions are handled as expected while communicating with Xero Environment 
    """

    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_custom_exception(self, mocked_session, mocked_failed_429_request):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            # Verifying if the custom exception 'XeroTooManyError' is raised on receiving status code 429
            self.assertRaises(client_.XeroTooManyError, xero_client.filter(tap_stream_id))
        except (requests.HTTPError, client_.XeroTooManyError) as e:
            expected_error_message = "Error: Too Many Requests. Please retry after 10 seconds"
            
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_backoff_behavior(self, mocked_session, mocked_failed_429_request):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"
        try:
            filter_func_exec = xero_client.filter(tap_stream_id)
        except (requests.HTTPError, client_.XeroTooManyError) as e:
            pass

        self.assertEqual(mocked_failed_429_request.call_count, 3)
        self.assertEqual(mocked_session.call_count, 3)