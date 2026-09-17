import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from entity.Sheet import GoogleSheetsAdapter
from entity.Sheet import Spreadsheet
from utils.access_control import (
    AccessContext,
    build_access_context,
    resolve_access_context,
    synchronize_access_context,
    user_access_from_secrets,
)
from utils.demo_data import DEMO_SHEETS, create_demo_spreadsheet
from utils.fitbit_token_store import is_state_used, resolve_state
from utils.health_token_store import resolve_oauth_state


PROJECT_ROOT = Path(__file__).resolve().parent


class SessionState(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


class AccessContextTests(unittest.TestCase):
    def test_anonymous_context_has_no_capabilities(self):
        context = build_access_context(streamlit_logged_in=False, demo_mode=False)
        self.assertTrue(context.is_anonymous)
        self.assertFalse(context.can_read_real_data)
        self.assertFalse(context.can_write)
        self.assertFalse(context.can_call_external_services)

    def test_demo_context_is_guest_and_read_only(self):
        context = build_access_context(streamlit_logged_in=False, demo_mode=True)
        self.assertTrue(context.is_guest)
        self.assertEqual(context.role, "Guest")
        self.assertEqual(context.project, "Demo")
        self.assertFalse(context.can_read_real_data)
        self.assertFalse(context.can_write)
        self.assertFalse(context.can_manage_devices)

    def test_unrecognized_google_user_is_a_guest(self):
        context = build_access_context(
            streamlit_logged_in=True,
            user_email="unknown@example.invalid",
            role="Guest",
            project="None",
        )
        self.assertTrue(context.is_guest)
        self.assertFalse(context.can_read_real_data)

    def test_authorized_roles_receive_expected_capabilities(self):
        for role in ("Admin", "Manager", "Student"):
            with self.subTest(role=role):
                context = build_access_context(
                    streamlit_logged_in=True,
                    user_email=f"{role.lower()}@example.invalid",
                    role=role,
                    project="Study A",
                )
                self.assertTrue(context.is_authenticated)
                self.assertTrue(context.can_read_real_data)
                self.assertTrue(context.can_write)
                self.assertEqual(context.can_manage_devices, role in {"Admin", "Manager"})
                self.assertEqual(context.can_call_external_services, role in {"Admin", "Manager"})

    def test_mapping_style_streamlit_user_resolves_manager_assignment(self):
        fake_streamlit = SimpleNamespace(
            user={
                "is_logged_in": True,
                "email": "shvartzmanrotem@campus.haifa.ac.il",
            },
            session_state=SessionState(),
        )

        with patch("utils.access_control.st", fake_streamlit):
            context = resolve_access_context(
                lambda email: ("Manager", "mdma")
                if email.split("@", 1)[0].casefold() == "shvartzmanrotem"
                else ("Guest", "None")
            )

        self.assertTrue(context.is_authenticated)
        self.assertEqual(context.role, "Manager")
        self.assertEqual(context.project, "mdma")
        self.assertTrue(context.can_read_real_data)

    def test_user_access_assignment_keeps_manager_out_of_guest_mode(self):
        secrets = {
            "shvartzmanrotem": "Manager,mdma",
            "unknown_setting": "not-a-role",
        }
        role, project = user_access_from_secrets(
            secrets,
            "ShvartzmanRotem@campus.haifa.ac.il",
        )

        self.assertEqual((role, project), ("Manager", "mdma"))

    def test_user_access_assignment_can_live_in_grouped_secrets(self):
        role, project = user_access_from_secrets(
            {"users": {"shvartzmanrotem": "Manager,metiv"}},
            "shvartzmanrotem@campus.haifa.ac.il",
        )

        self.assertEqual((role, project), ("Manager", "metiv"))

    def test_dotted_user_assignment_and_unknown_role_are_handled_safely(self):
        secrets = {
            "anuta89": {"ap": "Manager,metiv"},
            "unrecognized": "Owner,metiv",
        }

        self.assertEqual(
            user_access_from_secrets(secrets, "anuta89.ap@example.invalid"),
            ("Manager", "metiv"),
        )
        self.assertEqual(
            user_access_from_secrets(secrets, "unrecognized@example.invalid"),
            ("Guest", "None"),
        )

    def test_email_claim_without_login_flag_is_not_authenticated(self):
        fake_streamlit = SimpleNamespace(
            user={"email": "shvartzmanrotem@campus.haifa.ac.il"},
            session_state=SessionState(),
        )

        with patch("utils.access_control.st", fake_streamlit):
            context = resolve_access_context(lambda _: ("Manager", "mdma"))

        self.assertTrue(context.is_anonymous)
        self.assertFalse(context.can_read_real_data)

    def test_identity_transition_clears_data_bearing_session_values(self):
        session_state = SessionState(
            access_fingerprint="authenticated:admin@example.invalid:Admin:Study A",
            spreadsheet=object(),
            fitbit_watches={"REAL-WATCH": {}},
            current_data=object(),
            sheets_cache_get_spreadsheet=(0, object()),
            harmless_preference="keep-me",
            demo_mode=True,
        )
        fake_streamlit = SimpleNamespace(session_state=session_state)

        with patch("utils.access_control.st", fake_streamlit):
            synchronize_access_context(AccessContext.guest())

        self.assertNotIn("spreadsheet", session_state)
        self.assertNotIn("fitbit_watches", session_state)
        self.assertNotIn("current_data", session_state)
        self.assertNotIn("sheets_cache_get_spreadsheet", session_state)
        self.assertNotIn("harmless_preference", session_state)
        self.assertEqual(session_state["access_mode"], "guest")

    def test_guest_to_authenticated_transition_clears_dynamic_health_frames(self):
        session_state = SessionState(
            access_fingerprint="guest:guest@example.invalid:Guest:Demo",
            demo_mode=True,
            **{
                "REAL-WATCH_hr_2026-08-15": object(),
                "aggrid_state_devices": {"selected_rows": [1]},
                "chat_REAL-WATCH": [{"content": "production message"}],
            },
        )
        fake_streamlit = SimpleNamespace(session_state=session_state)
        authenticated = AccessContext(
            mode="authenticated",
            email="manager@example.invalid",
            role="Manager",
            project="Study A",
        )

        with patch("utils.access_control.st", fake_streamlit):
            synchronize_access_context(authenticated)

        self.assertNotIn("REAL-WATCH_hr_2026-08-15", session_state)
        self.assertNotIn("aggrid_state_devices", session_state)
        self.assertNotIn("chat_REAL-WATCH", session_state)
        self.assertFalse(session_state["demo_mode"])
        self.assertEqual(session_state["access_mode"], "authenticated")

    def test_first_resolution_clears_pre_upgrade_production_cache(self):
        session_state = SessionState(
            demo_mode=True,
            spreadsheet=object(),
            selected_watch="REAL-WATCH",
        )
        fake_streamlit = SimpleNamespace(session_state=session_state)

        with patch("utils.access_control.st", fake_streamlit):
            synchronize_access_context(AccessContext.guest())

        self.assertNotIn("spreadsheet", session_state)
        self.assertNotIn("selected_watch", session_state)
        self.assertEqual(session_state["access_mode"], "guest")


class DemoDataSecurityTests(unittest.TestCase):
    def setUp(self):
        self.spreadsheet = create_demo_spreadsheet()

    def test_demo_source_is_local_and_read_only(self):
        self.assertTrue(self.spreadsheet.read_only)
        self.assertEqual(self.spreadsheet.source_kind, "demo")
        self.assertEqual(self.spreadsheet.api_key, "")

    def test_demo_records_are_clearly_synthetic_and_contain_no_tokens(self):
        serialized = json.dumps(DEMO_SHEETS)
        self.assertIn("DEMO-WATCH-001", serialized)
        self.assertIn("example.invalid", serialized)
        for row in DEMO_SHEETS["fitbit"]:
            self.assertEqual(row["token"], "")
            self.assertEqual(row["oauth_client_key"], "")
            self.assertEqual(row["health_user_id"], "")
            self.assertEqual(row["legacy_fitbit_user_id"], "")

    def test_all_mutation_entrypoints_reject_demo_source_before_external_access(self):
        with patch("entity.Sheet.SheetsAPI.get_instance") as sheets_api:
            with self.assertRaises(PermissionError):
                self.spreadsheet.update_sheet("fitbit", {"name": "SHOULD-NOT-WRITE"}, strategy="append")
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.update_row(self.spreadsheet, "fitbit", watch="DEMO-WATCH-001")
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.update_rows(self.spreadsheet, "fitbit", watch="DEMO-WATCH-001")
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.append_rows(self.spreadsheet, "fitbit", [{"name": "SHOULD-NOT-WRITE"}])
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.delete_row(self.spreadsheet, "fitbit", watch="DEMO-WATCH-001")
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.save(self.spreadsheet, "fitbit")
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.connect(self.spreadsheet)
            with self.assertRaises(PermissionError):
                self.spreadsheet.get_gspread_connection()
            sheets_api.assert_not_called()

    def test_demo_reads_use_memory_without_google_sheets(self):
        with patch("entity.Sheet.SheetsAPI.get_instance") as sheets_api:
            rows = GoogleSheetsAdapter.get_rows(
                self.spreadsheet,
                "fitbit",
                "name",
                name="DEMO-WATCH-001",
            )
            self.assertEqual(len(rows), 1)
            sheets_api.assert_not_called()

    def test_any_read_only_source_rejects_an_external_connection(self):
        read_only_source = Spreadsheet(
            name="Read-only source",
            api_key="must-not-be-used",
            read_only=True,
        )
        with patch("entity.Sheet.SheetsAPI.get_instance") as sheets_api:
            with self.assertRaises(PermissionError):
                GoogleSheetsAdapter.connect(read_only_source)
            sheets_api.assert_not_called()


class FitbitOAuthStateTests(unittest.TestCase):
    def test_valid_state_resolves(self):
        state_row = {
            "state": "valid-state",
            "watchName": "WATCH-001",
            "created_at": "100",
            "expires_at": "200",
        }
        with patch("utils.fitbit_token_store.now_ts", return_value=150), patch(
            "utils.fitbit_token_store.GoogleSheetsAdapter.get_rows",
            return_value=[state_row],
        ):
            self.assertEqual(resolve_state(Mock(), "valid-state"), state_row)

    def test_expired_or_missing_state_is_rejected(self):
        expired = {
            "state": "expired-state",
            "watchName": "WATCH-001",
            "created_at": "100",
            "expires_at": "120",
        }
        with patch("utils.fitbit_token_store.now_ts", return_value=150), patch(
            "utils.fitbit_token_store.GoogleSheetsAdapter.get_rows",
            side_effect=[[expired], []],
        ):
            self.assertIsNone(resolve_state(Mock(), "expired-state"))
            self.assertIsNone(resolve_state(Mock(), "missing-state"))

    def test_reused_state_is_detected(self):
        with patch(
            "utils.fitbit_token_store.GoogleSheetsAdapter.get_rows",
            return_value=[{"state": "already-used"}],
        ):
            self.assertTrue(is_state_used(Mock(), "already-used"))


class GoogleHealthOAuthStateTests(unittest.TestCase):
    def test_valid_unexpired_unused_state_resolves(self):
        state_row = {
            "state": "valid-state",
            "provider": "google_health",
            "watchName": "WATCH-001",
            "expires_at": "2026-08-17T12:00:00+00:00",
            "used": "FALSE",
        }
        now = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
        with patch("utils.health_token_store.utc_now", return_value=now), patch(
            "utils.health_token_store.GoogleSheetsAdapter.get_rows",
            side_effect=[[state_row], []],
        ):
            self.assertEqual(
                resolve_oauth_state(Mock(), state="valid-state", provider="google_health"),
                state_row,
            )

    def test_missing_expired_and_reused_states_are_rejected(self):
        now = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
        cases = [
            ([], "Unknown OAuth state"),
            ([{
                "state": "expired",
                "provider": "google_health",
                "expires_at": "2026-08-15T12:00:00+00:00",
                "used": "FALSE",
            }], "OAuth state expired"),
            ([{
                "state": "used",
                "provider": "google_health",
                "expires_at": "2026-08-17T12:00:00+00:00",
                "used": "TRUE",
            }], "OAuth state was already used"),
        ]
        for state_rows, message in cases:
            with self.subTest(message=message), patch(
                "utils.health_token_store.utc_now", return_value=now
            ), patch(
                "utils.health_token_store.GoogleSheetsAdapter.get_rows",
                return_value=state_rows,
            ):
                with self.assertRaisesRegex(ValueError, message):
                    resolve_oauth_state(Mock(), state="state", provider="google_health")


class PageBoundaryAndLegalTests(unittest.TestCase):
    def test_every_functional_page_branches_guest_before_production_source(self):
        expected_pages = {
            "01_Home.py",
            "02_Dashboard.py",
            "03_Fitbit_Management.py",
            "04_Alerts_Configuration.py",
            "05_NOVA_Qualtrics_Management.py",
            "06_APPSHEET_Managment.py",
            "07_Connect_Participant.py",
            "07_OAuth_Connect.py",
        }
        for filename in expected_pages:
            with self.subTest(filename=filename):
                source = (PROJECT_ROOT / "pages" / filename).read_text()
                guest_branch = source.index("if context.is_guest:")
                production_access = source.index("get_spreadsheet()") if "get_spreadsheet()" in source else source.index("get_fibro_spreasheet()")
                self.assertLess(guest_branch, production_access)
                self.assertIn("render_demo_page", source)

    def test_legal_pages_are_public_and_contain_required_disclosures(self):
        privacy = (PROJECT_ROOT / "pages" / "08_Privacy_Policy.py").read_text()
        terms = (PROJECT_ROOT / "pages" / "09_Terms_of_Service.py").read_text()

        self.assertNotIn("AuthenticationController", privacy)
        self.assertNotIn("AuthenticationController", terms)
        self.assertIn("Google API Services User Data Policy", privacy)
        self.assertIn("Google Health API User Data and Health Research Policy", privacy)
        self.assertIn("fictional examples", privacy)
        self.assertIn("medical device", terms)
        self.assertIn("laws of the State of Israel", terms)
        self.assertIn("PI_EMAIL", privacy)
        self.assertIn("PI_EMAIL", terms)
        self.assertIn("Google Secret Manager", privacy)
        self.assertIn("Shared Drive", privacy)

    def test_production_brand_name_replaces_old_visible_name(self):
        tracked_ui_files = [PROJECT_ROOT / "app.py", *sorted((PROJECT_ROOT / "pages").glob("*.py"))]
        combined = "\n".join(path.read_text() for path in tracked_ui_files)
        self.assertIn("AdmonTracker", combined)
        self.assertNotIn("Fitbit Management System", combined)


class StreamlitGuestSmokeTests(unittest.TestCase):
    def test_every_guest_page_renders_without_external_or_credential_access(self):
        import importlib
        import sys

        # A legacy test module installs a minimal auth-controller stub during
        # pytest collection. Restore the application module for this UI smoke test.
        auth_module = sys.modules.get("controllers.auth_controller")
        if auth_module is not None and not getattr(auth_module, "__file__", None):
            sys.modules.pop("controllers.auth_controller", None)
            sys.modules.pop("controllers", None)
            controllers_package = importlib.import_module("controllers")
            setattr(
                controllers_package,
                "auth_controller",
                importlib.import_module("controllers.auth_controller"),
            )

        import streamlit
        from streamlit.testing.v1 import AppTest

        disabled_actions = {
            "pages/02_Dashboard.py": {"Refresh from device", "Send message"},
            "pages/03_Fitbit_Management.py": {"Add new device", "Save changes"},
            "pages/04_Alerts_Configuration.py": {"Save configuration"},
            "pages/05_NOVA_Qualtrics_Management.py": {"Save accepted numbers"},
            "pages/07_Connect_Participant.py": {"Generate connect link"},
            "pages/07_OAuth_Connect.py": {
                "Add watch & generate link",
                "Generate link for existing watch",
            },
        }
        pages = [
            "pages/01_Home.py",
            "pages/02_Dashboard.py",
            "pages/03_Fitbit_Management.py",
            "pages/04_Alerts_Configuration.py",
            "pages/05_NOVA_Qualtrics_Management.py",
            "pages/06_APPSHEET_Managment.py",
            "pages/07_Connect_Participant.py",
            "pages/07_OAuth_Connect.py",
        ]

        # Arrow-backed UI methods are replaced because the host test environment
        # may not yet have installed the production dependency versions.
        with patch.object(streamlit, "dataframe", lambda *args, **kwargs: None), patch.object(
            streamlit, "data_editor", lambda *args, **kwargs: None
        ), patch.object(streamlit, "line_chart", lambda *args, **kwargs: None), patch(
            "streamlit.page_link", lambda *args, **kwargs: None
        ), patch(
            "entity.Sheet.SheetsAPI.get_instance"
        ) as sheets_api, patch(
            "entity.Sheet.build_google_credentials"
        ) as credentials, patch(
            "requests.get"
        ) as request_get, patch(
            "requests.post"
        ) as request_post:
            app = AppTest.from_file(str(PROJECT_ROOT / "app.py"))
            app.session_state["demo_mode"] = True
            app.run(timeout=20)
            self.assertFalse(app.exception)

            for page in pages:
                with self.subTest(page=page):
                    page_app = AppTest.from_file(str(PROJECT_ROOT / page))
                    page_app.session_state["demo_mode"] = True
                    page_app.run(timeout=20)
                    self.assertFalse(page_app.exception)
                    disclosures = [item.value for item in page_app.warning]
                    self.assertTrue(
                        any("Guest demo" in value for value in disclosures),
                        f"Missing guest disclosure on {page}: {disclosures}",
                    )

                    buttons = {button.label: button for button in page_app.button}
                    for action in disabled_actions.get(page, set()):
                        self.assertIn(action, buttons)
                        self.assertTrue(buttons[action].disabled)

            sheets_api.assert_not_called()
            credentials.assert_not_called()
            request_get.assert_not_called()
            request_post.assert_not_called()

    def test_legal_pages_render_without_authentication(self):
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_file(str(PROJECT_ROOT / "app.py")).run(timeout=20)
        self.assertFalse(app.exception)
        for page in (
            "pages/08_Privacy_Policy.py",
            "pages/09_Terms_of_Service.py",
            "pages/10_Research_Ethics.py",
            "pages/11_Participant_Authorization.py",
            "pages/12_Manage_Connection.py",
        ):
            with self.subTest(page=page):
                app.switch_page(page).run(timeout=20)
                self.assertFalse(app.exception)


if __name__ == "__main__":
    unittest.main()
