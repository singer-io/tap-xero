import tap_xero.client as client_
import unittest
from unittest import mock
from test_exception_handling import mocked_session, mock_successful_request, mock_successful_session_post

@mock.patch('requests.Session.send', side_effect=mocked_session)
class TestCheckPlatformAccessRequest(unittest.TestCase):

    @mock.patch('requests.Session.post', side_effect=mock_successful_session_post)
    @mock.patch('tap_xero.client.update_config_file')
    @mock.patch('requests.Request', side_effect=mock_successful_request)
    def test_check_platform_access_request(self, mocked_request, mocked_update_config_file, mocked_post, mocked_send):
        '''
            Verify that check_platform_access called with expected endpoint and paramater for validating credentials
        '''
        config = {
            "client_id": "123",
            "client_secret": "123",
            "refresh_token": "123",
            "tenant_id": "123"
        }

        # Initialize XeroClient and call check_platform_access function
        xero_client = client_.XeroClient(config)
        xero_client.check_platform_access(config, "")
        
        # Verify requests.request is called with expected endpoint(Invoice) and parmeter in check_platform_access
        mocked_request.assert_called_with('GET',
                                       'https://api.xero.com/api.xro/2.0/Invoices',
                                        headers={'Authorization': 'Bearer 123', 'Xero-Tenant-Id': '123', 'Content-Type': 'application/json'},
                                        params={'page': 1})
