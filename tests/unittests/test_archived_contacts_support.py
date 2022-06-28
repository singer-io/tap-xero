import tap_xero.streams as stream_
import unittest
import requests
from unittest import mock

class MockConfig:
    def __init__(self):
        self.config = {
            "include_archived_contacts": "true"
        }

    def update_start_date_bookmark(self,bookmark):
        return "2021-04-01"

    def get_offset(self, offset):
        return None

    def set_offset(self, offset, curr_page_num):
        return curr_page_num

    def write_state(self):
        return ""

    def clear_offsets(self, tap_stream_id):
        return ""

    def set_bookmark(self, bookmark, max_updated):
        return ""


class TestSupportArchivedContacts(unittest.TestCase):
    """
    Test cases to verify the support of Archived contacts from the Xero API
    """

    @mock.patch("tap_xero.streams._make_request")
    def test_archived_contacts_selected_string(self, mocked_make_request_method):
        mocked_make_request_method.return_value = []
        tap_stream_id = "contacts"
        contacts_stream_execution = stream_.Contacts()

        # ArchivedContacts parameter set to true in the MockConfig class
        ctx = MockConfig()
        sync_resp = contacts_stream_execution.sync(ctx)

        expected_filter_options = dict(since="2021-04-01", order="UpdatedDateUTC ASC", includeArchived="true", page=1)

        # Verifying the parameters send to the _make_request method which is responsible for collecting data from the Xero platform
        mocked_make_request_method.assert_called_with(ctx, tap_stream_id, expected_filter_options)


    @mock.patch("tap_xero.streams._make_request")
    def test_archived_contacts_selected_boolean(self, mocked_make_request_method):
        mocked_make_request_method.return_value = []
        tap_stream_id = "contacts"
        contacts_stream_execution = stream_.Contacts()

        # ArchivedContacts parameter set to true in the MockConfig class
        ctx = MockConfig()

        # Customer may also pass a boolean parameter for include_archived_contacts
        ctx.config["include_archived_contacts"] = True
        sync_resp = contacts_stream_execution.sync(ctx)

        expected_filter_options = dict(since="2021-04-01", order="UpdatedDateUTC ASC", includeArchived="true", page=1)

        # Verifying the parameters send to the _make_request method which is responsible for collecting data from the Xero platform
        mocked_make_request_method.assert_called_with(ctx, tap_stream_id, expected_filter_options)


    @mock.patch("tap_xero.streams._make_request")
    def test_archived_contacts_not_selected(self, mocked_make_request_method):
        mocked_make_request_method.return_value = []
        tap_stream_id = "contacts"
        contacts_stream_execution = stream_.Contacts()

        # ArchivedContacts parameter set to true in the MockConfig class
        ctx = MockConfig()
        # Setting the archived contacts parameter value to "false" to get only active contacts result
        ctx.config["include_archived_contacts"] = "false"
        sync_resp = contacts_stream_execution.sync(ctx)

        expected_filter_options = dict(since="2021-04-01", order="UpdatedDateUTC ASC", page=1)

        # Verifying the parameters send to the _make_request method which is responsible for collecting data from the Xero platform
        mocked_make_request_method.assert_called_with(ctx, tap_stream_id, expected_filter_options)

    @mock.patch("tap_xero.streams._make_request")
    def test_archived_contacts_parameter_with_other_streams(self, mocked_make_request_method):
        mocked_make_request_method.return_value = []
        tap_stream_id = "accounts"
        pk_fields = ["AccountID"]
        contacts_stream_execution = stream_.PaginatedStream(tap_stream_id, pk_fields)

        # ArchivedContacts parameter set to true in the MockConfig class. However, for other streams, this parameter should not be passed
        # while contacting Xero platform
        ctx = MockConfig()
        sync_resp = contacts_stream_execution.sync(ctx)

        expected_filter_options = dict(since="2021-04-01", order="UpdatedDateUTC ASC", page=1)

        # Verifying the parameters send to the _make_request method which is responsible for collecting data from the Xero platform
        mocked_make_request_method.assert_called_with(ctx, tap_stream_id, expected_filter_options)

    @mock.patch("tap_xero.streams._make_request")
    def test_archived_contacts_option_not_passed_in_config(self, mocked_make_request_method):
        mocked_make_request_method.return_value = []
        tap_stream_id = "contacts"
        contacts_stream_execution = stream_.Contacts()

        # ArchivedContacts parameter set to true in the MockConfig class
        ctx = MockConfig()

        # Deleting the 'include_archived_contacts' option from the config dictionary
        del ctx.config["include_archived_contacts"]
        sync_resp = contacts_stream_execution.sync(ctx)

        expected_filter_options = dict(since="2021-04-01", order="UpdatedDateUTC ASC", page=1)

        # Verifying the parameters send to the _make_request method which is responsible for collecting data from the Xero platform
        mocked_make_request_method.assert_called_with(ctx, tap_stream_id, expected_filter_options)
