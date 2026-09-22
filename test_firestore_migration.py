import os
import unittest
from unittest.mock import patch

from utils.data_backend import DataBackendConfig
from utils.firestore_schema import (
    MIGRATION_SPECS,
    build_migration_plan,
    canonical_documents_hash,
    stable_document_id,
)


def spec_for(sheet):
    return next(spec for spec in MIGRATION_SPECS if spec.source_sheet == sheet)


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

    def test_hash_ignores_migration_timestamp(self):
        spec = spec_for("project")
        first = build_migration_plan(
            spec,
            [{"name": "Yoga"}],
            migrated_at="2026-09-22T00:00:00+00:00",
        )
        second = build_migration_plan(
            spec,
            [{"name": "Yoga"}],
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


if __name__ == "__main__":
    unittest.main()
