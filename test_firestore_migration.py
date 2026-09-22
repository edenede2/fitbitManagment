import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from services.health_client_factory import HealthClientFactory
from utils.data_backend import DataBackendConfig
from utils.firestore_schema import (
    MIGRATION_SPECS,
    build_migration_plan,
    canonical_documents_hash,
    stable_document_id,
)
from utils.firestore_store import GoogleFirestoreStore


def spec_for(sheet):
    return next(spec for spec in MIGRATION_SPECS if spec.source_sheet == sheet)


class FakeSnapshot:
    def __init__(self, document_id, value):
        self.id = document_id
        self._value = value

    def to_dict(self):
        return dict(self._value)


class FakeDocument:
    def __init__(self, client, collection, document_id):
        self.client = client
        self.collection = collection
        self.document_id = document_id

    def set(self, value):
        self.client.data.setdefault(self.collection, {})[self.document_id] = dict(value)


class FakeCollection:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def document(self, document_id):
        return FakeDocument(self.client, self.name, document_id)

    def stream(self):
        return [
            FakeSnapshot(document_id, value)
            for document_id, value in self.client.data.get(self.name, {}).items()
        ]


class FakeBatch:
    def __init__(self):
        self.operations = []

    def set(self, document, value):
        self.operations.append(("set", document, dict(value)))

    def delete(self, document):
        self.operations.append(("delete", document, None))

    def commit(self):
        for operation, document, value in self.operations:
            collection = document.client.data.setdefault(document.collection, {})
            if operation == "set":
                collection[document.document_id] = value
            else:
                collection.pop(document.document_id, None)


class FakeFirestoreClient:
    def __init__(self):
        self.data = {}

    def collection(self, name):
        return FakeCollection(self, name)

    def batch(self):
        return FakeBatch()


class FirestoreMigrationTests(unittest.TestCase):
    def test_secret_values_are_omitted_from_device_documents(self):
        plan = build_migration_plan(
            spec_for("fitbit"),
            [
                {
                    "project": "Yoga",
                    "name": "YN4",
                    "token": "sensitive-token",
                    "token_secret_ref": "projects/admontracker/secrets/device/versions/latest",
                }
            ],
            migrated_at="2026-09-22T00:00:00+00:00",
        )
        document = next(iter(plan.documents.values()))
        self.assertNotIn("token", document)
        self.assertIn("token_secret_ref", document)
        self.assertEqual(plan.redacted_nonempty_fields, 1)

    def test_oauth_tokens_compact_to_latest_active_row(self):
        plan = build_migration_plan(
            spec_for("health_oauth_tokens"),
            [
                {
                    "watchName": "YN4",
                    "provider": "google_health",
                    "is_active": "TRUE",
                    "updated_at": "old",
                    "access_token": "old-secret",
                },
                {
                    "watchName": "YN4",
                    "provider": "google_health",
                    "is_active": "TRUE",
                    "updated_at": "new",
                    "token_secret_ref": "projects/admontracker/secrets/yn4/versions/latest",
                    "access_token": "new-secret",
                },
                {
                    "watchName": "YN4",
                    "provider": "google_health",
                    "is_active": "FALSE",
                    "updated_at": "revoked",
                },
            ],
            migrated_at="2026-09-22T00:00:00+00:00",
        )
        self.assertEqual(len(plan.documents), 1)
        self.assertEqual(plan.excluded_rows, 1)
        self.assertEqual(plan.compacted_rows, 1)
        document = next(iter(plan.documents.values()))
        self.assertEqual(document["updated_at"], "new")
        self.assertNotIn("access_token", document)

    def test_document_ids_do_not_expose_identifiers(self):
        spec = spec_for("user")
        row = {"email": "participant@example.org", "role": "Manager"}
        document_id = stable_document_id(spec, row, 2)
        self.assertEqual(len(document_id), 64)
        self.assertNotIn("participant", document_id)
        self.assertNotIn("@", document_id)

    def test_reauth_queue_compacts_repeated_alerts_per_watch(self):
        plan = build_migration_plan(
            spec_for("health_reauth_queue"),
            [
                {
                    "queue_id": "first",
                    "watchName": "YN4",
                    "provider": "google_health",
                    "detected_at": "old",
                    "status": "open",
                },
                {
                    "queue_id": "second",
                    "watchName": "YN4",
                    "provider": "google_health",
                    "detected_at": "new",
                    "status": "open",
                },
            ],
            migrated_at="2026-09-22T00:00:00+00:00",
        )
        self.assertEqual(len(plan.documents), 1)
        self.assertEqual(plan.compacted_rows, 1)
        self.assertEqual(next(iter(plan.documents.values()))["queue_id"], "second")

    def test_hash_ignores_migration_timestamp(self):
        spec = spec_for("fitbit")
        first = build_migration_plan(
            spec,
            [{"project": "Yoga", "name": "YN4"}],
            migrated_at="2026-09-22T00:00:00+00:00",
        )
        second = build_migration_plan(
            spec,
            [{"project": "Yoga", "name": "YN4"}],
            migrated_at="2026-09-23T00:00:00+00:00",
        )
        self.assertEqual(
            canonical_documents_hash(first.documents),
            canonical_documents_hash(second.documents),
        )

    def test_sheets_remains_default_backend(self):
        with patch.dict(os.environ, {}, clear=True):
            config = DataBackendConfig.from_environment()
        self.assertEqual(config.backend, "sheets")
        self.assertFalse(config.firestore_shadow_write)
        self.assertTrue(config.sheets_read_fallback)

    def test_firestore_backend_requires_project(self):
        with patch.dict(os.environ, {"DATA_BACKEND": "firestore"}, clear=True):
            with self.assertRaisesRegex(ValueError, "FIRESTORE_PROJECT_ID"):
                DataBackendConfig.from_environment()

    def test_runtime_store_omits_plaintext_tokens(self):
        client = FakeFirestoreClient()
        store = GoogleFirestoreStore(client=client, project_id="admontracker")
        store.upsert_sheet_rows(
            "fitbit",
            [
                {
                    "project": "Yoga",
                    "name": "YN4",
                    "token": "do-not-store",
                    "token_secret_ref": "projects/admontracker/secrets/yn4/versions/latest",
                }
            ],
        )
        rows = store.read_sheet_rows("fitbit")
        self.assertEqual(len(rows), 1)
        self.assertNotIn("token", rows[0])
        self.assertIn("token_secret_ref", rows[0])

    def test_staff_access_and_project_registry_fields_are_not_duplicated(self):
        user_plan = build_migration_plan(
            spec_for("user"),
            [
                {
                    "name": "Participant 1",
                    "email": "participant@example.org",
                    "project": "Yoga",
                    "role": "Manager",
                    "projects": "Yoga,Fibro",
                }
            ],
        )
        user = next(iter(user_plan.documents.values()))
        self.assertEqual(user["project"], "Yoga")
        self.assertNotIn("role", user)
        self.assertNotIn("projects", user)
        self.assertIsNone(
            next(
                (spec for spec in MIGRATION_SPECS if spec.source_sheet == "project"),
                None,
            )
        )

    def test_device_project_does_not_require_project_collection(self):
        plan = build_migration_plan(
            spec_for("fitbit"),
            [{"project": "NotInProjectTab", "name": "YN4", "isActive": "TRUE"}],
        )
        device = next(iter(plan.documents.values()))
        self.assertEqual(device["project"], "NotInProjectTab")

    def test_runtime_store_updates_and_deletes_by_business_key(self):
        client = FakeFirestoreClient()
        store = GoogleFirestoreStore(client=client, project_id="admontracker")
        store.upsert_sheet_rows(
            "fitbit",
            [{"project": "Yoga", "name": "YN4", "auth_status": "connected"}],
        )
        self.assertEqual(
            store.update_sheet_rows(
                "fitbit",
                keys={"project": "Yoga", "name": "YN4"},
                updates={"auth_status": "disconnected"},
            ),
            1,
        )
        self.assertEqual(store.read_sheet_rows("fitbit")[0]["auth_status"], "disconnected")
        self.assertEqual(
            store.delete_sheet_rows(
                "fitbit",
                keys={"project": "Yoga", "name": "YN4"},
            ),
            1,
        )
        self.assertEqual(store.read_sheet_rows("fitbit"), [])

    def test_spreadsheet_routes_primary_backend_to_firestore(self):
        with patch.dict(
            os.environ,
            {"DATA_BACKEND": "firestore", "FIRESTORE_PROJECT_ID": "admontracker"},
            clear=True,
        ), patch("entity.Sheet.get_secrets", return_value={"spreadsheet_key": "main"}):
            spreadsheet = Spreadsheet(name="main", api_key="main")
            secondary = Spreadsheet(name="other", api_key="other")
        self.assertEqual(spreadsheet.source_kind, "firestore")
        self.assertEqual(secondary.source_kind, "production")

    def test_firestore_adapter_reads_without_sheets(self):
        store = Mock()
        store.read_sheet_rows.return_value = [
            {"project": "Yoga", "name": "YN4"},
            {"project": "Other", "name": "N1"},
        ]
        spreadsheet = Spreadsheet(name="main", api_key="main", source_kind="firestore")
        with patch.object(GoogleSheetsAdapter, "_firestore_store", return_value=store):
            rows = GoogleSheetsAdapter.get_rows(
                spreadsheet,
                "fitbit",
                "project",
                project="Yoga",
            )
        self.assertEqual(rows, [{"project": "Yoga", "name": "YN4"}])
        store.read_sheet_rows.assert_called_once_with("fitbit")

    def test_firestore_missing_match_can_fall_back_to_sheets(self):
        store = Mock()
        store.read_sheet_rows.return_value = [{"project": "Other", "name": "N1"}]
        worksheet = Mock()
        worksheet.get_all_records.return_value = [{"project": "Yoga", "name": "YN4"}]
        workbook = Mock()
        workbook.worksheet.return_value = worksheet
        sheets_api = Mock()
        sheets_api.open_spreadsheet.return_value = workbook
        spreadsheet = Spreadsheet(name="main", api_key="main", source_kind="firestore")
        with patch.object(
            GoogleSheetsAdapter,
            "_firestore_store",
            return_value=store,
        ), patch.object(
            GoogleSheetsAdapter,
            "_sheets_read_fallback_allowed",
            return_value=True,
        ), patch(
            "entity.Sheet.SheetsAPI.get_instance",
            return_value=sheets_api,
        ):
            rows = GoogleSheetsAdapter.get_rows(
                spreadsheet,
                "fitbit",
                "project",
                project="Yoga",
            )
        self.assertEqual(rows, [{"project": "Yoga", "name": "YN4"}])
        sheets_api.open_spreadsheet.assert_called_once_with("main")

    def test_unmapped_tab_does_not_use_firestore(self):
        spreadsheet = Spreadsheet(name="main", api_key="main", source_kind="firestore")
        self.assertFalse(
            GoogleSheetsAdapter._is_firestore_sheet(spreadsheet, "legacy_unmapped_tab")
        )

    def test_google_health_client_resolves_token_once_per_refresh(self):
        config = SimpleNamespace()
        with patch(
            "services.health_client_factory.get_oauth_client_config",
            return_value=config,
        ), patch(
            "services.health_client_factory.get_valid_access_token",
            return_value="access-token",
        ) as get_token:
            client = HealthClientFactory.from_watch_row(
                object(),
                {
                    "name": "YN4",
                    "provider": "google_health",
                    "oauth_client_key": "production",
                },
            )
            self.assertEqual(client.access_token_provider(), "access-token")
            self.assertEqual(client.access_token_provider(), "access-token")
        get_token.assert_called_once()


if __name__ == "__main__":
    unittest.main()
