from tap_xero.client import XeroClient
import unittest
from unittest import mock
from unittest.case import TestCase
from requests.exceptions import Timeout, ConnectTimeout
import datetime

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_xero.client.requests.Request')
    @mock.patch('tap_xero.client.requests.Session.send')
    @mock.patch('tap_xero.client.requests.Session.post')
    def test_backoff_check_platform_access_timeout_error(self, mock_post, mock_send, mock_request):
        """
        Check whether the request backoffs properly  for 60 seconds in case of Timeout error.
        """
        mock_send.side_effect = Timeout
        mock_post.side_effect = Timeout
        before_time = datetime.datetime.now()
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
            client = XeroClient(config)   
            client.check_platform_access(config, "dummy_path")
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 120)

    @mock.patch('tap_xero.client.requests.Request')
    @mock.patch('tap_xero.client.requests.Session.send')
    @mock.patch('tap_xero.client.requests.Session.post')
    def test_backoff_check_platform_access_connect_timeout_error(self, mock_post, mock_send, mock_request):
        """
        Check whether the request backoffs properly for 60 seconds in case of ConnectTimeout error.
        """
        mock_send.side_effect = ConnectTimeout
        mock_post.side_effect = ConnectTimeout
        before_time = datetime.datetime.now()
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
            client = XeroClient(config)   
            client.check_platform_access(config, "dummy_path")
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 120)

    @mock.patch('tap_xero.client.requests.Session.post')
    def test_backoff_refresh_credentials_timeout_error(self, mock_post):
        """
        Check whether the request backoffs properly for 60 seconds in case of Timeout error.
        """
        mock_post.side_effect = Timeout
        before_time = datetime.datetime.now()
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
            client = XeroClient(config)   
            client.refresh_credentials(config, "dummy_path")
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)

    @mock.patch('tap_xero.client.requests.Session.post')
    def test_backoff_refresh_credentials_connect_timeout_error(self, mock_post):
        """
        Check whether the request backoffs properly for 60 seconds in case of ConnectTimeout error.
        """
        mock_post.side_effect = ConnectTimeout
        before_time = datetime.datetime.now()
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
            client = XeroClient(config)   
            client.refresh_credentials(config, "dummy_path")
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)

    @mock.patch('tap_xero.client.requests.Session.send')
    @mock.patch('tap_xero.client.requests.Request')
    def test_backoff_filter_timeout_error(self, mock_request, mock_send):
        """
        Check whether the request backoffs properly for 5 times in case of Timeout error.
        """
        mock_send.side_effect = Timeout
        before_time = datetime.datetime.now()
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
            client = XeroClient(config)
            client.access_token = "dummy_token"
            client.filter(tap_stream_id='dummy_stream')
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)

class MockResponse():
    '''
    Mock response  object for the requests call 
    '''
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def json(self):
        return self.text

class MockRequest():
    '''
    Mock Request object for mocking the Requests()
    '''
    def __init__(self):
        pass

    def prepare(self):
        pass

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('tap_xero.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_xero.client.requests.Request', return_value = MockRequest())
    @mock.patch('tap_xero.client.XeroClient.refresh_credentials')
    def test_config_provided_request_timeout(self, mock_refresh_creds, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt", "request_timeout": 100}
        client = XeroClient(config)
        client.access_token = "dummy_access_token"
        client.check_platform_access("GET", "dummy_path")
        mock_send.assert_called_with(MockRequest().prepare(), timeout=100.0)

    @mock.patch('tap_xero.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_xero.client.requests.Request', return_value = MockRequest())
    @mock.patch('tap_xero.client.XeroClient.refresh_credentials')
    def test_default_value_request_timeout(self, mock_refresh_creds, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt"}
        client = XeroClient(config)
        client.access_token = "dummy_access_token"
        client.check_platform_access("GET", "dummy_path")
        mock_send.assert_called_with(MockRequest().prepare(), timeout=300.0)

    @mock.patch('tap_xero.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_xero.client.requests.Request', return_value = MockRequest())
    @mock.patch('tap_xero.client.XeroClient.refresh_credentials')
    def test_config_provided_empty_request_timeout(self, mock_refresh_creds, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt", "request_timeout": ""}
        client = XeroClient(config)
        client.access_token = "dummy_access_token"
        client.check_platform_access("GET", "dummy_path")
        mock_send.assert_called_with(MockRequest().prepare(), timeout=300.0)
        
    @mock.patch('tap_xero.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_xero.client.requests.Request', return_value = MockRequest())
    @mock.patch('tap_xero.client.XeroClient.refresh_credentials')
    def test_config_provided_string_request_timeout(self, mock_refresh_creds, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt", "request_timeout": "100"}
        client = XeroClient(config)
        client.access_token = "dummy_access_token"
        client.check_platform_access("GET", "dummy_path")
        mock_send.assert_called_with(MockRequest().prepare(), timeout=100.0)

    @mock.patch('tap_xero.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_xero.client.requests.Request', return_value = MockRequest())
    @mock.patch('tap_xero.client.XeroClient.refresh_credentials')
    def test_config_provided_float_request_timeout(self, mock_refresh_creds, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"start_date": "dummy_st", "client_id": "dummy_ci", "client_secret": "dummy_cs", "tenant_id": "dummy_ti", "refresh_token": "dummy_rt", "request_timeout": 100.8}
        client = XeroClient(config)
        client.access_token = "dummy_access_token"
        client.check_platform_access("GET", "dummy_path")
        mock_send.assert_called_with(MockRequest().prepare(), timeout=100.8)