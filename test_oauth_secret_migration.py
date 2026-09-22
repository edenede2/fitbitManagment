import unittest
from unittest.mock import patch

from scripts.migrate_oauth_secrets import (
    _is_fitbit_active,
    _is_health_active,
    _is_registry_active,
    _migrate_fitbit_registry,
    _migrate_grouped_sheet,
    _migration_secret_ref,
    _verified_store,
)


class FakeWorksheet:
    def __init__(self, headers, records):
        self.headers = list(headers)
        self.records = list(records)
        self.updates = []

    def row_values(self, row):
        return list(self.headers) if row == 1 else []

    def get_all_records(self):
        return list(self.records)

    def resize(self, *, cols):
        return None

    def update(self, range_name, values):
        if range_name == "1:1":
            self.headers = list(values[0])

    def batch_update(self, data, raw=True):
        self.updates.extend(data)


class FakeWorkbook:
    def __init__(self, worksheet):
        self._worksheet = worksheet

    def worksheet(self, name):
        if isinstance(self._worksheet, dict):
            return self._worksheet[name]
        return self._worksheet


def update_map(worksheet):
    return {
        item["range"]: item["values"][0][0]
        for item in worksheet.updates
    }


class OAuthSecretMigrationTests(unittest.TestCase):
    def setUp(self):
        self.headers = [
            "watchName",
            "token_secret_ref",
            "access_token",
            "refresh_token",
            "status",
        ]
        self.records = [
            {
                "watchName": "P-001",
                "access_token": "old-access",
                "refresh_token": "old-refresh",
                "status": "connected",
            },
            {
                "watchName": "P-001",
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "status": "connected",
            },
            {
                "watchName": "P-002",
                "access_token": "revoked-access",
                "refresh_token": "revoked-refresh",
                "status": "revoked",
            },
            {
                "watchName": "",
                "access_token": "unidentified-access",
                "refresh_token": "unidentified-refresh",
                "status": "connected",
            },
        ]

    def migrate(self, *, apply, clear_plaintext):
        worksheet = FakeWorksheet(self.headers, self.records)
        workbook = FakeWorkbook(worksheet)
        with patch(
            "scripts.migrate_oauth_secrets._verified_store",
            return_value="projects/p/secrets/fitbit-p001",
        ) as store:
            result = _migrate_grouped_sheet(
                workbook,
                tab="fitbit_oauth_tokens",
                secret_kind="participant-oauth",
                identity_fields=["watchName"],
                required_identity_fields=["watchName"],
                identity_prefix=["fitbit"],
                secret_fields=["access_token", "refresh_token"],
                ref_field="token_secret_ref",
                active_predicate=_is_fitbit_active,
                apply=apply,
                clear_plaintext=clear_plaintext,
            )
        return result, worksheet, store

    def test_dry_run_counts_groups_without_writes(self):
        result, worksheet, store = self.migrate(apply=False, clear_plaintext=False)
        self.assertEqual(result.candidates, 4)
        self.assertEqual(result.groups, 2)
        self.assertEqual(result.inactive, 1)
        self.assertEqual(result.unresolved, 1)
        self.assertEqual(worksheet.updates, [])
        store.assert_not_called()

    def test_boolean_false_is_not_treated_as_active(self):
        self.assertFalse(_is_health_active({"is_active": False}))
        self.assertFalse(_is_registry_active({"isActive": False}))

    def test_only_latest_active_payload_is_stored_and_history_is_cleared(self):
        result, worksheet, store = self.migrate(apply=True, clear_plaintext=True)
        store.assert_called_once_with(
            "participant-oauth",
            ["fitbit", "P-001"],
            {"access_token": "new-access", "refresh_token": "new-refresh"},
            existing_ref="",
        )
        updates = update_map(worksheet)
        # Rows 2 and 3 are P-001 history; row 4 is revoked and can be erased.
        self.assertEqual(updates["C2"], "")
        self.assertEqual(updates["D2"], "")
        self.assertEqual(updates["C3"], "")
        self.assertEqual(updates["D3"], "")
        self.assertEqual(updates["B3"], "projects/p/secrets/fitbit-p001")
        self.assertEqual(updates["C4"], "")
        self.assertEqual(updates["D4"], "")
        # The unidentified row is deliberately not modified.
        self.assertNotIn("C5", updates)
        self.assertNotIn("D5", updates)
        self.assertEqual(result.migrated, 1)
        self.assertEqual(result.cleared, 3)

    def test_matching_existing_secret_does_not_create_another_version(self):
        payload = {"access_token": "a", "refresh_token": "r"}
        with patch(
            "scripts.migrate_oauth_secrets.load_json_secret",
            return_value=payload,
        ), patch("scripts.migrate_oauth_secrets.store_json_secret") as store:
            ref = _verified_store(
                "participant-oauth",
                ["fitbit", "P-001"],
                payload,
                existing_ref="projects/p/secrets/existing",
            )
        self.assertEqual(ref, "projects/p/secrets/existing")
        store.assert_not_called()

    def test_malformed_reference_is_not_sent_to_secret_manager(self):
        count = type("Count", (), {"invalid_refs": 0})()
        self.assertEqual(_migration_secret_ref("legacy", count), "")
        self.assertEqual(count.invalid_refs, 1)

        payload = {"access_token": "a", "refresh_token": "r"}
        with patch(
            "scripts.migrate_oauth_secrets.load_json_secret",
            return_value=payload,
        ) as load, patch(
            "scripts.migrate_oauth_secrets.store_json_secret",
            return_value="projects/p/secrets/recovered",
        ) as store:
            ref = _verified_store(
                "participant-oauth",
                ["fitbit", "P-001"],
                payload,
                existing_ref="legacy",
            )
        self.assertEqual(ref, "projects/p/secrets/recovered")
        load.assert_called_once_with("projects/p/secrets/recovered")
        store.assert_called_once_with(
            "participant-oauth",
            ["fitbit", "P-001"],
            payload,
            existing_ref="",
        )

    def test_wrong_project_reference_is_rejected_when_project_is_configured(self):
        with patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "admontracker"}):
            self.assertEqual(
                _migration_secret_ref("projects/other-project/secrets/token"),
                "",
            )

    def test_registry_reuses_oauth_ref_and_only_stores_legacy_only_watch(self):
        oauth = FakeWorksheet(
            ["watchName", "token_secret_ref", "status"],
            [
                {
                    "watchName": "P-001",
                    "token_secret_ref": "projects/p/secrets/oauth-p001",
                    "status": "connected",
                }
            ],
        )
        registry = FakeWorksheet(
            ["name", "token", "token_secret_ref", "isActive"],
            [
                {"name": "P-001", "token": "oauth-copy", "isActive": True},
                {"name": "P-002", "token": "legacy-only", "isActive": True},
                {"name": "P-003", "token": "inactive", "isActive": False},
            ],
        )
        workbook = FakeWorkbook(
            {"fitbit_oauth_tokens": oauth, "fitbit": registry}
        )
        with patch(
            "scripts.migrate_oauth_secrets._verified_store",
            return_value="projects/p/secrets/legacy-p002",
        ) as store:
            result = _migrate_fitbit_registry(
                workbook,
                apply=True,
                clear_plaintext=True,
            )
        store.assert_called_once_with(
            "participant-oauth",
            ["fitbit", "P-002"],
            {"access_token": "legacy-only", "refresh_token": ""},
            existing_ref="",
        )
        updates = update_map(registry)
        self.assertEqual(updates["C2"], "projects/p/secrets/oauth-p001")
        self.assertEqual(updates["C3"], "projects/p/secrets/legacy-p002")
        self.assertEqual(updates["B2"], "")
        self.assertEqual(updates["B3"], "")
        self.assertEqual(updates["B4"], "")
        self.assertEqual(result.migrated, 2)
        self.assertEqual(result.cleared, 3)
        self.assertEqual(result.inactive, 1)


if __name__ == "__main__":
    unittest.main()
