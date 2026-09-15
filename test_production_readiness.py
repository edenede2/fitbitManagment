import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from run_drive_archive_collection import _zip_tree, period_windows
from services.drive_archive import safe_drive_segment
from services.google_health_client import GoogleHealthClient
from utils.compliance import (
    approved_disclosure_ready,
    participant_authorization_url,
    research_documents,
)
from utils.health_connect_links import create_health_connect_link
from utils.health_token_store import (
    acknowledge_oauth_state,
    assert_state_authorized_for_callback,
    save_google_health_tokens_for_watch,
)
from utils.secret_store import safe_secret_id


APPROVED_ENV = {
    "PARTICIPANT_DISCLOSURE_ENFORCED": "true",
    "PARTICIPANT_DISCLOSURE_VERSION": "EC-385-23-GH-v1",
    "RESEARCH_RETENTION_TEXT_EN": "Approved English retention wording.",
    "RESEARCH_RETENTION_TEXT_HE": "נוסח שמירה מאושר.",
    "RESEARCH_DELETION_TEXT_EN": "Approved English deletion wording.",
    "RESEARCH_DELETION_TEXT_HE": "נוסח מחיקה מאושר.",
    "APP_BASE_URL": "https://app.admontracker.online",
    "APPROVED_GOOGLE_HEALTH_ADDENDUM_PATH": __file__,
}


class ComplianceGateTests(unittest.TestCase):
    def test_public_evidence_assets_are_deployment_safe_pdfs(self):
        documents = {document.key: document for document in research_documents()}
        for key in ("study_approval", "institutional_accreditation", "pilot_consent"):
            with self.subTest(key=key):
                document = documents[key]
                self.assertTrue(document.exists)
                self.assertEqual(document.mime_type, "application/pdf")
                self.assertTrue(document.path.read_bytes().startswith(b"%PDF"))

    def test_draft_cannot_be_marked_ready(self):
        with patch.dict("os.environ", {}, clear=True):
            ready, missing = approved_disclosure_ready()
        self.assertFalse(ready)
        self.assertIn("an ethics-approved disclosure version", missing)

    def test_approved_bilingual_wording_is_ready(self):
        with patch.dict("os.environ", APPROVED_ENV, clear=True):
            ready, missing = approved_disclosure_ready()
        self.assertTrue(ready)
        self.assertEqual(missing, [])

    def test_public_participant_url_uses_production_domain(self):
        with patch.dict("os.environ", APPROVED_ENV, clear=True):
            url = participant_authorization_url("opaque state")
        self.assertEqual(
            url,
            "https://app.admontracker.online/Participant_Authorization?state=opaque+state",
        )

    def test_enforced_callback_requires_staff_adult_and_participant_records(self):
        valid = {
            "staff_consent_verified": "TRUE",
            "adult_verified": "TRUE",
            "participant_acknowledged_at": "2026-09-14T00:00:00+00:00",
            "participant_disclosure_version": "EC-385-23-GH-v1",
        }
        with patch.dict("os.environ", APPROVED_ENV, clear=True):
            assert_state_authorized_for_callback(valid)
            for field in (
                "staff_consent_verified",
                "adult_verified",
                "participant_acknowledged_at",
            ):
                invalid = {**valid, field: ""}
                with self.assertRaises(ValueError):
                    assert_state_authorized_for_callback(invalid)

    def test_link_routes_through_disclosure_when_enforced(self):
        with patch.dict("os.environ", APPROVED_ENV, clear=True), patch(
            "utils.health_connect_links.uuid4", return_value="state-1"
        ), patch("utils.health_connect_links.save_oauth_state") as save_state:
            url = create_health_connect_link(
                Mock(),
                watchName="P-001",
                project="study-a",
                provider="google_health",
                oauth_client_key="prod-health",
                created_by="staff@example.invalid",
                staff_consent_verified=True,
                adult_verified=True,
            )
        self.assertEqual(
            url,
            "https://app.admontracker.online/Participant_Authorization?state=state-1",
        )
        save_state.assert_called_once()

    def test_acknowledgement_is_append_only_and_pseudonymous(self):
        state = {
            "state": "state-1",
            "provider": "google_health",
            "watchName": "P-001",
            "project": "study-a",
            "purpose": "connect",
            "staff_consent_verified": "TRUE",
            "staff_consent_verified_at": "2026-09-14T00:00:00+00:00",
            "adult_verified": "TRUE",
        }
        with patch.dict("os.environ", APPROVED_ENV, clear=True), patch(
            "utils.health_token_store._update_row_by_keys", return_value=True
        ), patch("utils.health_token_store._append") as append:
            updated = acknowledge_oauth_state(Mock(), state_row=state, language="he")
        self.assertEqual(updated["participant_language"], "he")
        consent_values = append.call_args.args[3]
        self.assertNotIn("email", consent_values)
        self.assertNotIn("name", consent_values)
        self.assertEqual(consent_values["watchName"], "P-001")


class SecretStorageTests(unittest.TestCase):
    def test_secret_ids_are_safe_stable_and_pseudonymous(self):
        first = safe_secret_id("participant-oauth", "google_health", "Study A", "P/001")
        second = safe_secret_id("participant-oauth", "google_health", "Study A", "P/001")
        self.assertEqual(first, second)
        self.assertNotIn("/", first)
        self.assertLessEqual(len(first), 255)

    def test_google_tokens_leave_sheet_cells_blank_when_secret_manager_enabled(self):
        captured = {}
        with patch.dict("os.environ", {"SECRET_MANAGER_ENABLED": "true"}, clear=True), patch(
            "utils.health_token_store.get_active_token_row", return_value=None
        ), patch(
            "utils.health_token_store.store_json_secret",
            return_value="projects/p/secrets/token",
        ), patch(
            "utils.health_token_store.upsert_active_token_row",
            side_effect=lambda _, row: captured.update(row),
        ), patch("utils.health_token_store.update_fitbit_registry_after_auth"):
            save_google_health_tokens_for_watch(
                Mock(),
                watchName="P-001",
                project="study-a",
                oauth_client_key="prod",
                token_data={
                    "access_token": "access-secret",
                    "refresh_token": "refresh-secret",
                    "access_expires_at": 2000000000,
                },
            )
        self.assertEqual(captured["token_secret_ref"], "projects/p/secrets/token")
        self.assertEqual(captured["access_token"], "")
        self.assertEqual(captured["refresh_token"], "")


class ArchiveBehaviorTests(unittest.TestCase):
    def test_drive_segments_reject_traversal_and_remove_slashes(self):
        self.assertEqual(safe_drive_segment("Study / A"), "Study - A")
        with self.assertRaises(ValueError):
            safe_drive_segment("../")

    def test_periods_do_not_precede_cutover(self):
        tz = ZoneInfo("Asia/Jerusalem")
        now = datetime(2026, 9, 14, 12, tzinfo=tz)
        cutover = datetime(2026, 9, 14, 9, tzinfo=tz)
        daily = period_windows(now, "daily", cutover)
        monthly = period_windows(now, "monthly", cutover)
        self.assertEqual([item.period for item in daily], ["2026-09-14"])
        self.assertEqual(monthly[0].start_date.isoformat(), "2026-09-14")

    def test_zip_bytes_are_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "raw.json").write_text('{"ok":true}', encoding="utf-8")
            first = _zip_tree(root, {"schema_version": 1})
            second = _zip_tree(root, {"schema_version": 1})
        self.assertEqual(first, second)

    def test_google_health_sleep_pages_are_capped_at_25(self):
        client = GoogleHealthClient(access_token_provider=lambda: "not-used")
        with patch.object(client, "request", return_value={"dataPoints": []}) as request:
            client.list_data_points("sleep", page_size=10000)
        self.assertEqual(request.call_args.kwargs["params"]["pageSize"], 25)


if __name__ == "__main__":
    unittest.main()
