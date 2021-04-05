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

        def json(self):
            return self.text

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


def mocked_forbidden_403_exception(*args, **kwargs):
    json_decode_str = {"Title": "Forbidden", "Detail": "AuthenticationUnsuccessful"}

    return Mockresponse(json_decode_str, 403, raise_error=True)


def mocked_badrequest_400_error(*args, **kwargs):
    json_decode_str = {"Message": "Bad Request Error"}

    return Mockresponse(json_decode_str, 400, raise_error=True)


def mocked_unauthorized_401_error(*args, **kwargs):
    json_decode_str = {"Title": "Unauthorized", "Detail": "AuthenticationUnsuccessful"}

    return Mockresponse(json_decode_str, 401, raise_error=True)


def mocked_notfound_404_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 404, raise_error=True)


def mocked_failed_429_request(*args, **kwargs):
    json_decode_str = ''
    headers = {"Retry-After": 10}
    return Mockresponse(json_decode_str, 429, headers=headers, raise_error=True)


def mocked_internalservererror_500_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 500, raise_error=True)


def mocked_notimplemented_501_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 501, raise_error=True)

class TestFilterFunExceptionHandling(unittest.TestCase):
    """
    Test cases to verify if the exceptions are handled as expected while communicating with Xero Environment 
    """

    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_badrequest_400_error)
    def test_badrequest_400_error(self, mocked_session, mocked_badrequest_400_error):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_unauthorized_401_error)
    def test_unauthorized_401_error(self, mocked_session, mocked_unauthorized_401_error):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_forbidden_403_exception)
    def test_forbidden_403_exception(self, mocked_session, mocked_forbidden_403_exception):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_notfound_404_error)
    def test_notfound_404_error(self, mocked_session, mocked_notfound_404_error):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_internalservererror_500_error)
    def test_internalservererror_500_error(self, mocked_session, mocked_internalservererror_500_error):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroInternalError as e:
            expected_error_message = "HTTP-error-code: 500, Error: An unhandled error with the Xero API. Contact the Xero API team if problems persist."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_notimplemented_501_error)
    def test_notimplemented_501_error(self, mocked_session, mocked_notimplemented_501_error):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            xero_client.filter(tap_stream_id)
        except client_.XeroNotImplementedError as e:
            expected_error_message = "HTTP-error-code: 501, Error: The method you have called has not been implemented."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_429_error(self, mocked_session, mocked_failed_429_request):
        config = {}
        tap_stream_id = "contacts"

        xero_client = client_.XeroClient(config)
        xero_client.access_token = "123"
        xero_client.tenant_id = "123"

        try:
            # Verifying if the custom exception 'XeroTooManyError' is raised on receiving status code 429
            filter_func_exec = xero_client.filter(tap_stream_id)
        except client_.XeroTooManyError as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded. Please retry after 10 seconds"
            
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.Session.send', side_effect=mocked_session)
    @mock.patch('requests.Request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_429_backoff_behavior(self, mocked_session, mocked_failed_429_request):
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