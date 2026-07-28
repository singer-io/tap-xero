"""
Unit tests covering the discovery changes introduced to tap-xero:
  - Stream.check_access()  (tap_xero/streams.py)
  - discover()             (tap_xero/__init__.py)
  - load_schema()          (tap_xero/__init__.py)
  - load_metadata()        (tap_xero/__init__.py)
"""
import unittest
from unittest import mock

import tap_xero.streams as streams_
from tap_xero import discover, load_metadata, load_schema
from tap_xero.client import XeroForbiddenError, XeroUnauthorizedError

# ---------------------------------------------------------------------------
# Minimal test doubles
# ---------------------------------------------------------------------------

class MockClient:
    """Minimal stand-in for XeroClient – only the filter() call-site matters."""

    def filter(self, tap_stream_id, filter_options=None):
        return []


class MockContext:
    """Minimal stand-in for Context used throughout discovery."""

    def __init__(self):
        self.config = {"start_date": "2021-01-01T00:00:00Z"}
        self.client = MockClient()
        self.state = {}

    def refresh_credentials(self):
        pass

    def check_platform_access(self):
        pass


# ---------------------------------------------------------------------------
# Helper: return a plain function suitable for patching Stream.check_access
# ---------------------------------------------------------------------------

def _always_accessible(stream_self, ctx):
    return True


def _never_accessible(stream_self, ctx):
    return False


# ---------------------------------------------------------------------------
# Tests: Stream.check_access()
# ---------------------------------------------------------------------------

class TestCheckAccess(unittest.TestCase):
    """Unit tests for Stream.check_access()."""

    def setUp(self):
        self.ctx = MockContext()

    # --- return value tests ------------------------------------------------

    def test_returns_true_when_filter_succeeds(self):
        """check_access returns True when client.filter completes without error."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]):
            stream = streams_.BookmarkedStream("accounts", ["AccountID"])
            self.assertTrue(stream.check_access(self.ctx))

    def test_returns_false_on_forbidden_error(self):
        """check_access returns False (not raise) on XeroForbiddenError."""
        with mock.patch.object(self.ctx.client, "filter",
                                side_effect=XeroForbiddenError("403")):
            stream = streams_.BookmarkedStream("accounts", ["AccountID"])
            self.assertFalse(stream.check_access(self.ctx))

    def test_returns_false_on_unauthorized_error(self):
        """check_access returns False (not raise) on XeroUnauthorizedError."""
        with mock.patch.object(self.ctx.client, "filter",
                                side_effect=XeroUnauthorizedError("401")):
            stream = streams_.BookmarkedStream("accounts", ["AccountID"])
            self.assertFalse(stream.check_access(self.ctx))

    # --- call-order / call-arguments tests ---------------------------------

    def test_refresh_credentials_called_before_filter(self):
        """check_access calls ctx.refresh_credentials() before ctx.client.filter()."""
        call_order = []

        with mock.patch.object(self.ctx, "refresh_credentials",
                                side_effect=lambda: call_order.append("refresh")), \
             mock.patch.object(self.ctx.client, "filter",
                                side_effect=lambda *a, **kw: call_order.append("filter")):
            stream = streams_.BookmarkedStream("accounts", ["AccountID"])
            stream.check_access(self.ctx)

        self.assertEqual(call_order, ["refresh", "filter"])

    def test_filter_receives_stream_id_and_empty_probe_options(self):
        """Default Stream.check_access passes tap_stream_id with no extra args."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]) as mock_filter:
            stream = streams_.BookmarkedStream("accounts", ["AccountID"])
            stream.check_access(self.ctx)
            mock_filter.assert_called_once_with("accounts")

    def test_paginated_stream_passes_empty_probe_options(self):
        """PaginatedStream.check_access passes empty probe_filter_options."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]) as mock_filter:
            stream = streams_.PaginatedStream("invoices", ["InvoiceID"])
            stream.check_access(self.ctx)
            mock_filter.assert_called_once_with("invoices")

    def test_everything_stream_passes_empty_probe_options(self):
        """Everything.check_access passes empty probe_filter_options."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]) as mock_filter:
            stream = streams_.Everything("currencies", ["Code"])
            stream.check_access(self.ctx)
            mock_filter.assert_called_once_with("currencies")

    # --- stream-specific probe_filter_options tests -----------------------

    def test_journals_class_attribute_probe_options(self):
        """Journals defines probe_filter_options={'offset': 0} at the class level."""
        self.assertEqual(streams_.Journals.probe_filter_options, {"offset": 0})

    def test_linked_transactions_class_attribute_probe_options(self):
        """LinkedTransactions defines probe_filter_options={'page': 1} at class level."""
        self.assertEqual(streams_.LinkedTransactions.probe_filter_options, {"page": 1})

    def test_journals_instance_check_access_uses_probe_options(self):
        """Journals instances probe with offset=0 from class-level probe_filter_options."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]) as mock_filter:
            stream = streams_.Journals(
                "journals", ["JournalID"], bookmark_key="JournalNumber"
            )
            stream.check_access(self.ctx)
            mock_filter.assert_called_once_with("journals", offset=0)

    def test_linked_transactions_instance_check_access_uses_probe_options(self):
        """LinkedTransactions instances probe with page=1 from class-level probe_filter_options."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]) as mock_filter:
            stream = streams_.LinkedTransactions(
                "linked_transactions", ["LinkedTransactionID"]
            )
            stream.check_access(self.ctx)
            mock_filter.assert_called_once_with("linked_transactions", page=1)

    # --- Contacts (subclass) tests ----------------------------------------

    def test_contacts_returns_true_when_accessible(self):
        """Contacts.check_access returns True when filter succeeds."""
        with mock.patch.object(self.ctx.client, "filter", return_value=[]):
            self.assertTrue(streams_.Contacts().check_access(self.ctx))

    def test_contacts_returns_false_on_403(self):
        """Contacts.check_access returns False on XeroForbiddenError."""
        with mock.patch.object(self.ctx.client, "filter",
                                side_effect=XeroForbiddenError("403")):
            self.assertFalse(streams_.Contacts().check_access(self.ctx))

    def test_contacts_returns_false_on_401(self):
        """Contacts.check_access returns False on XeroUnauthorizedError."""
        with mock.patch.object(self.ctx.client, "filter",
                                side_effect=XeroUnauthorizedError("401")):
            self.assertFalse(streams_.Contacts().check_access(self.ctx))


# ---------------------------------------------------------------------------
# Tests: discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Unit tests for the discover() function in tap_xero/__init__.py."""

    def setUp(self):
        self.ctx = MockContext()

    # --- platform-access gate ---------------------------------------------

    def test_calls_check_platform_access_once(self):
        """discover() must call ctx.check_platform_access() exactly once."""
        with mock.patch.object(streams_.Stream, "check_access", _always_accessible), \
             mock.patch.object(self.ctx, "check_platform_access") as mock_check:
            discover(self.ctx)
        mock_check.assert_called_once()

    # --- full-access catalog tests ----------------------------------------

    def test_all_accessible_catalog_length_equals_all_streams(self):
        """discover() returns one CatalogEntry per stream when all are accessible."""
        with mock.patch.object(streams_.Stream, "check_access", _always_accessible):
            catalog = discover(self.ctx)
        self.assertEqual(len(catalog.streams), len(streams_.all_streams))

    def test_catalog_stream_ids_order_matches_all_stream_ids(self):
        """Catalog preserves the order defined in all_stream_ids."""
        with mock.patch.object(streams_.Stream, "check_access", _always_accessible):
            catalog = discover(self.ctx)
        catalog_ids = [s.tap_stream_id for s in catalog.streams]
        self.assertEqual(catalog_ids, streams_.all_stream_ids)

    def test_catalog_entry_stream_field_matches_tap_stream_id(self):
        """Each CatalogEntry has stream == tap_stream_id."""
        with mock.patch.object(streams_.Stream, "check_access", _always_accessible):
            catalog = discover(self.ctx)
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(entry.stream, entry.tap_stream_id)

    def test_catalog_entries_have_correct_key_properties(self):
        """Each CatalogEntry's key_properties match the stream's pk_fields."""
        with mock.patch.object(streams_.Stream, "check_access", _always_accessible):
            catalog = discover(self.ctx)
        pk_map = {s.tap_stream_id: s.pk_fields for s in streams_.all_streams}
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(entry.key_properties, pk_map[entry.tap_stream_id])

    # --- access-filtering tests -------------------------------------------

    def test_inaccessible_stream_excluded_from_catalog(self):
        """discover() skips a stream whose check_access returns False."""
        target = streams_.all_streams[0].tap_stream_id

        def selective(s, ctx):
            return s.tap_stream_id != target

        with mock.patch.object(streams_.Stream, "check_access", selective):
            catalog = discover(self.ctx)

        catalog_ids = [s.tap_stream_id for s in catalog.streams]
        self.assertNotIn(target, catalog_ids)
        self.assertEqual(len(catalog.streams), len(streams_.all_streams) - 1)

    def test_partial_access_returns_only_accessible_streams(self):
        """discover() includes exactly the streams for which check_access is True."""
        accessible = {"accounts", "invoices", "currencies"}

        def selective(s, ctx):
            return s.tap_stream_id in accessible

        with mock.patch.object(streams_.Stream, "check_access", selective):
            catalog = discover(self.ctx)

        self.assertEqual(
            {e.tap_stream_id for e in catalog.streams},
            accessible,
        )

    def test_raises_forbidden_error_when_no_streams_accessible(self):
        """discover() raises XeroForbiddenError when every check_access returns False."""
        with mock.patch.object(streams_.Stream, "check_access", _never_accessible):
            with self.assertRaises(XeroForbiddenError):
                discover(self.ctx)

    def test_check_access_called_for_every_stream(self):
        """discover() calls check_access() once for every stream in all_streams."""
        checked = []

        def tracking(s, ctx):
            checked.append(s.tap_stream_id)
            return True

        with mock.patch.object(streams_.Stream, "check_access", tracking):
            discover(self.ctx)

        self.assertEqual(len(checked), len(streams_.all_streams))
        self.assertEqual(set(checked), set(streams_.all_stream_ids))

    def test_last_accessible_stream_kept(self):
        """discover() does not raise when exactly one stream passes check_access."""
        only_stream = streams_.all_streams[-1].tap_stream_id

        def one_left(s, ctx):
            return s.tap_stream_id == only_stream

        with mock.patch.object(streams_.Stream, "check_access", one_left):
            catalog = discover(self.ctx)

        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].tap_stream_id, only_stream)


# ---------------------------------------------------------------------------
# Tests: load_schema()
# ---------------------------------------------------------------------------

class TestLoadSchema(unittest.TestCase):
    """Unit tests for load_schema()."""

    def test_all_stream_schemas_load_without_error(self):
        """load_schema() succeeds for every tap_stream_id in all_streams."""
        for stream in streams_.all_streams:
            with self.subTest(stream=stream.tap_stream_id):
                schema = load_schema(stream.tap_stream_id)
                self.assertIn("properties", schema)

    def test_schema_is_dict_with_properties_key(self):
        """load_schema('accounts') returns a dict that has a 'properties' key."""
        schema = load_schema("accounts")
        self.assertIsInstance(schema, dict)
        self.assertIn("properties", schema)

    def test_schema_dependencies_stripped_from_output(self):
        """tap_schema_dependencies is removed from the resolved schema."""
        # invoices.json references nested schemas via tap_schema_dependencies
        schema = load_schema("invoices")
        self.assertNotIn("tap_schema_dependencies", schema)

    def test_schema_dependency_refs_are_inlined(self):
        """Schemas with dependencies have their $ref definitions resolved inline."""
        # invoices depend on line_items; after resolution line_items properties
        # should be present in the LineItems array items
        schema = load_schema("invoices")
        line_items_def = (
            schema.get("properties", {})
                  .get("LineItems", {})
                  .get("items", {})
        )
        self.assertIn("properties", line_items_def)


# ---------------------------------------------------------------------------
# Tests: load_metadata()
# ---------------------------------------------------------------------------

class TestLoadMetadata(unittest.TestCase):
    """Unit tests for load_metadata()."""

    # --- helpers -----------------------------------------------------------

    def _root_meta(self, mdata_list):
        """Return the root-level metadata dict (empty breadcrumb tuple)."""
        for entry in mdata_list:
            if not entry["breadcrumb"]:
                return entry["metadata"]
        return {}

    def _field_meta(self, mdata_list, field_name):
        """Return the metadata dict for a specific property field."""
        for entry in mdata_list:
            bc = entry["breadcrumb"]
            if len(bc) == 2 and bc[0] == "properties" and bc[1] == field_name:
                return entry["metadata"]
        return None

    # --- root-level metadata keys -----------------------------------------

    def test_table_key_properties_matches_pk_fields(self):
        """table-key-properties is set to the stream's pk_fields."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        mdata = load_metadata(stream, load_schema("accounts"))
        self.assertEqual(self._root_meta(mdata)["table-key-properties"], ["AccountID"])

    def test_incremental_stream_replication_method(self):
        """Incremental streams carry forced-replication-method='INCREMENTAL'."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        mdata = load_metadata(stream, load_schema("accounts"))
        self.assertEqual(
            self._root_meta(mdata)["forced-replication-method"], "INCREMENTAL"
        )

    def test_full_table_stream_replication_method(self):
        """Full-table streams carry forced-replication-method='FULL_TABLE'."""
        stream = streams_.Everything("currencies", ["Code"])
        mdata = load_metadata(stream, load_schema("currencies"))
        self.assertEqual(
            self._root_meta(mdata)["forced-replication-method"], "FULL_TABLE"
        )

    def test_bookmarked_stream_valid_replication_keys_present(self):
        """Bookmarked streams list their bookmark_key in valid-replication-keys."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        mdata = load_metadata(stream, load_schema("accounts"))
        root = self._root_meta(mdata)
        self.assertIn("valid-replication-keys", root)
        self.assertIn("UpdatedDateUTC", root["valid-replication-keys"])

    def test_full_table_stream_has_no_valid_replication_keys(self):
        """Full-table streams (bookmark_key=None) omit valid-replication-keys."""
        stream = streams_.Everything("currencies", ["Code"])
        mdata = load_metadata(stream, load_schema("currencies"))
        self.assertNotIn("valid-replication-keys", self._root_meta(mdata))

    def test_journals_bookmark_key_is_journal_number(self):
        """Journals stream uses JournalNumber as bookmark key."""
        stream = next(s for s in streams_.all_streams if s.tap_stream_id == "journals")
        mdata = load_metadata(stream, load_schema("journals"))
        root = self._root_meta(mdata)
        self.assertIn("valid-replication-keys", root)
        self.assertIn("JournalNumber", root["valid-replication-keys"])

    def test_bank_transfers_bookmark_key_is_created_date_utc(self):
        """bank_transfers uses CreatedDateUTC as its bookmark key."""
        stream = next(
            s for s in streams_.all_streams if s.tap_stream_id == "bank_transfers"
        )
        mdata = load_metadata(stream, load_schema("bank_transfers"))
        root = self._root_meta(mdata)
        self.assertIn("valid-replication-keys", root)
        self.assertIn("CreatedDateUTC", root["valid-replication-keys"])

    # --- property-level inclusion -----------------------------------------

    def test_pk_field_has_automatic_inclusion(self):
        """Primary key field metadata has inclusion='automatic'."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        mdata = load_metadata(stream, load_schema("accounts"))
        self.assertEqual(
            self._field_meta(mdata, "AccountID")["inclusion"], "automatic"
        )

    def test_bookmark_key_field_has_automatic_inclusion(self):
        """The bookmark key field has inclusion='automatic'."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        mdata = load_metadata(stream, load_schema("accounts"))
        self.assertEqual(
            self._field_meta(mdata, "UpdatedDateUTC")["inclusion"], "automatic"
        )

    def test_non_key_non_bookmark_fields_have_available_inclusion(self):
        """All non-PK, non-bookmark fields have inclusion='available'."""
        stream = streams_.BookmarkedStream("accounts", ["AccountID"])
        schema = load_schema("accounts")
        mdata = load_metadata(stream, schema)
        auto_fields = {"AccountID", "UpdatedDateUTC"}
        for entry in mdata:
            bc = entry["breadcrumb"]
            if len(bc) == 2 and bc[0] == "properties" and bc[1] not in auto_fields:
                with self.subTest(field=bc[1]):
                    self.assertEqual(entry["metadata"]["inclusion"], "available")

    def test_multi_key_stream_all_pk_fields_are_automatic(self):
        """Streams with multiple PK fields mark every PK as automatic."""
        # bank_transactions has a single PK, but we can test with a custom stream
        stream = streams_.PaginatedStream(
            "bank_transactions", ["BankTransactionID", "Type"]
        )
        schema = load_schema("bank_transactions")
        mdata = load_metadata(stream, schema)
        for pk in ["BankTransactionID", "Type"]:
            with self.subTest(pk=pk):
                field_meta = self._field_meta(mdata, pk)
                if field_meta is not None:  # only if property exists in schema
                    self.assertEqual(field_meta["inclusion"], "automatic")
