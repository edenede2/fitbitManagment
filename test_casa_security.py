"""Synthetic-only CASA regressions: no credentials or external services."""
import threading
import unittest
from copy import deepcopy
from enum import IntEnum
from types import SimpleNamespace
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

from utils.access_control import AccessContext, staff_identity_assignment, user_access_from_secrets
from utils.collection_control import CollectionStopped, assert_collection_allowed, collection_key
from utils.connection_management import disconnect_connection
from utils.secret_store import GoogleSecretStore


class MemorySecurityStore:
    """Atomic injected fake, never a production fallback."""
    def __init__(self):
        self.rows = {}
        self.lock = threading.RLock()

    def read(self, key):
        with self.lock:
            return deepcopy(self.rows.get(key, {}))

    def mutate(self, key, change):
        with self.lock:
            after = change(deepcopy(self.rows.get(key, {})))
            self.rows[key] = deepcopy(after)
            return deepcopy(after)

    def mutate_document(self, collection, key, change, *, dependency_keys=(), validate_dependencies=None):
        with self.lock:
            storage_key = key if collection == "_casa_security" else (collection, key)
            before = deepcopy(self.rows.get(storage_key, {}))
            deps = [deepcopy(self.rows.get(k, {})) for k in dependency_keys]
            if validate_dependencies:
                validate_dependencies(before, deps)
            after = change(before)
            self.rows[storage_key] = deepcopy(after)
            return deepcopy(after)

    def fenced_write(self, gate_key, generation, collection, document_id, data):
        with self.lock:
            gate = self.rows.get(gate_key, {})
            if gate.get("collection_stopped") or gate.get("generation", 0) != generation:
                raise CollectionStopped("Synthetic stale write rejected")
            self.rows[(collection, document_id)] = deepcopy(data)


def staff_fixture():
    claims = dict(is_logged_in=True, iss="https://accounts.google.com", sub="synthetic-sub",
                  email="researcher@approved.example", email_verified=True, exp=2000, aud="synthetic-client")
    settings = {"auth": {"google": {"client_id": "synthetic-client"}}, "staff_identities": [
        dict(issuer=claims["iss"], subject=claims["sub"], email=claims["email"],
             role="Manager", project="Synthetic-A", enabled=True)]}
    return settings, claims


class IdentityTests(unittest.TestCase):
    def test_prefix_never_authorizes(self):
        settings = {"reviewer_demo": "Manager,Synthetic-A"}
        for email in ("reviewer_demo@approved.example", "reviewer_demo@unapproved.example"):
            self.assertEqual(user_access_from_secrets(settings, email), ("Guest", "None"))

    def test_binding_is_explicit_and_claims_are_required(self):
        settings, claims = staff_fixture()
        allowed = staff_identity_assignment(settings, claims, now=1000)
        self.assertTrue(allowed.can_read_real_data)
        self.assertTrue(allowed.allows_project("Synthetic-A"))
        self.assertFalse(allowed.allows_project("Synthetic-B"))
        for field, bad in (("sub", "other-sub"), ("iss", "https://unapproved.example"),
                           ("email", "researcher@unapproved.example"), ("email_verified", False),
                           ("email_verified", "true"), ("exp", 999), ("exp", float("nan")),
                           ("exp", float("inf")), ("aud", "other-client")):
            with self.subTest(field=field):
                self.assertFalse(staff_identity_assignment(settings, {**claims, field: bad}, now=1000).can_read_real_data)
        for field in ("sub", "iss", "email_verified", "exp", "email"):
            modified = {k: v for k, v in claims.items() if k != field}
            self.assertFalse(staff_identity_assignment(settings, modified, now=1000).can_read_real_data)

    def test_student_and_revoked_binding_are_denied(self):
        settings, claims = staff_fixture()
        settings["staff_identities"][0]["role"] = "Student"
        self.assertFalse(staff_identity_assignment(settings, claims, now=1000).can_read_real_data)
        settings["staff_identities"][0].update(role="Manager", enabled=False)
        self.assertFalse(staff_identity_assignment(settings, claims, now=1000).can_read_real_data)


class NumericState(IntEnum):
    ENABLED = 1
    DISABLED = 2
    DESTROYED = 3

    def __str__(self):
        return str(self.value)


class CleanupTests(unittest.TestCase):
    def test_enabled_and_disabled_numeric_versions_destroyed_and_verified(self):
        client = Mock()
        versions = [SimpleNamespace(name=f"projects/synthetic/secrets/demo/versions/{i}", state=state)
                    for i, state in enumerate(NumericState, 1)]
        client.list_secret_versions.return_value = versions
        client.get_secret_version.return_value = SimpleNamespace(state=NumericState.DESTROYED)
        GoogleSecretStore("synthetic", client).destroy_versions("projects/synthetic/secrets/demo")
        self.assertEqual(client.destroy_secret_version.call_count, 2)
        self.assertEqual(client.get_secret_version.call_count, 2)

    def test_delayed_or_failed_destruction_is_not_success(self):
        for fail in (False, True):
            client = Mock()
            client.list_secret_versions.return_value = [SimpleNamespace(name="synthetic", state=1)]
            client.get_secret_version.return_value = SimpleNamespace(state=2)
            if fail:
                client.destroy_secret_version.side_effect = RuntimeError("synthetic-provider-body")
            with self.assertRaisesRegex(RuntimeError, "pending") as result:
                GoogleSecretStore("synthetic", client).destroy_versions("projects/synthetic/secrets/demo")
            self.assertNotIn("synthetic-provider-body", str(result.exception))


class DisconnectTests(unittest.TestCase):
    def setUp(self):
        self.store = MemorySecurityStore()
        self.row = dict(provider="google_health", watchName="SYNTHETIC", project="Synthetic-A",
                        management_token_hash="synthetic-hash")

    def test_secret_failure_cannot_prevent_collection_stop_and_retry(self):
        with patch.dict("os.environ", {"DATA_BACKEND": "firestore"}), \
             patch("utils.collection_control.get_security_store", return_value=self.store), \
             patch("utils.connection_management.GoogleSheetsAdapter.get_rows", return_value=[
                 dict(watchName="SYNTHETIC", project="Synthetic-A", token_secret_ref="synthetic-ref")]), \
             patch("utils.connection_management.GoogleSheetsAdapter.update_matching_rows", return_value=1), \
             patch("utils.connection_management._update_row_by_keys", return_value=1), \
             patch("utils.connection_management.load_json_secret", side_effect=RuntimeError("synthetic outage")), \
             patch("utils.connection_management.destroy_secret_versions", side_effect=RuntimeError("synthetic outage")):
            warnings = disconnect_connection(Mock(), self.row)
            stopped = self.store.read(collection_key("SYNTHETIC", "google_health"))
            self.assertTrue(stopped["collection_stopped"])
            self.assertEqual(stopped["provider_revocation_status"], "pending_manual")
            self.assertEqual(stopped["secret_cleanup_status"], "pending")
            self.assertEqual(stopped["data_deletion_status"], "not_requested")
            self.assertTrue(warnings)
            with self.assertRaises(CollectionStopped):
                assert_collection_allowed("SYNTHETIC", "google_health")
            disconnect_connection(Mock(), {**self.row, "revoked_at": "already-stopped"})
            self.assertEqual(self.store.read(collection_key("SYNTHETIC", "google_health"))["generation"], 1)

    def test_metadata_failure_also_happens_after_stop(self):
        with patch.dict("os.environ", {"DATA_BACKEND": "firestore"}), \
             patch("utils.collection_control.get_security_store", return_value=self.store), \
             patch("utils.connection_management.GoogleSheetsAdapter.get_rows", side_effect=RuntimeError("synthetic outage")):
            self.assertTrue(disconnect_connection(Mock(), self.row))
            self.assertTrue(self.store.read(collection_key("SYNTHETIC", "google_health"))["collection_stopped"])


class SessionAndEnrollmentTests(unittest.TestCase):
    def setUp(self):
        enable = patch.dict("os.environ", {"GOOGLE_MFA_CLAIMS_ENABLED": "true"})
        enable.start()
        self.addCleanup(enable.stop)
        self.store = MemorySecurityStore()
        self.actor = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        self.control = dict(watchName="SYNTHETIC", project="Synthetic-A", provider="fitbit",
                            enrollment_id="synthetic-enrollment", generation=1, collection_stopped=False)
        self.store.rows[collection_key("SYNTHETIC", "fitbit")] = dict(self.control)

    def test_one_use_codes_and_sessions_replacement_reassignment_expiry(self):
        from utils.participant_sessions import issue_management_code, exchange_management_code, resolve_management_session
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        with patch("utils.participant_sessions.get_security_store", return_value=self.store), \
             patch("utils.participant_sessions._now", return_value=now):
            code = issue_management_code(self.control)
            session = exchange_management_code(code, browser_nonce="synthetic-browser")
            self.assertEqual(resolve_management_session(session, browser_nonce="synthetic-browser")["enrollment_id"], "synthetic-enrollment")
            with self.assertRaises(PermissionError):
                exchange_management_code(code, browser_nonce="synthetic-browser")
            with self.assertRaises(PermissionError):
                resolve_management_session(session, browser_nonce="different-browser")
            replacement = issue_management_code(self.control)
            with self.assertRaises(PermissionError):
                resolve_management_session(session, browser_nonce="synthetic-browser")
            fresh = exchange_management_code(replacement, browser_nonce="synthetic-browser")
            with patch("utils.participant_sessions._now", return_value=now + timedelta(minutes=11)):
                with self.assertRaises(PermissionError):
                    resolve_management_session(fresh, browser_nonce="synthetic-browser")
            self.store.rows[collection_key("SYNTHETIC", "fitbit")]["enrollment_id"] = "another-enrollment"
            with self.assertRaises(PermissionError):
                resolve_management_session(fresh, browser_nonce="synthetic-browser")

    def test_code_expiry_and_rate_limit(self):
        from utils.participant_sessions import issue_management_code, exchange_management_code
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        with patch("utils.participant_sessions.get_security_store", return_value=self.store), \
             patch("utils.participant_sessions._now", return_value=now):
            code = issue_management_code(self.control)
            for attempt in range(6):
                with self.assertRaises(PermissionError):
                    exchange_management_code("synthetic-invalid", browser_nonce="limited-browser")
            with self.assertRaises(PermissionError):
                exchange_management_code(code, browser_nonce="limited-browser")
            with patch("utils.participant_sessions._now", return_value=now + timedelta(hours=24)):
                with self.assertRaises(PermissionError):
                    exchange_management_code(code, browser_nonce="other-browser")

    def test_google_mfa_missing_stale_and_session_revoke(self):
        from utils.staff_sessions import google_mfa_is_fresh, create_staff_session, validate_staff_session, revoke_staff_sessions
        claims = {"amr": ["pwd", "mfa"], "auth_time": 1000}
        self.assertTrue(google_mfa_is_fresh(claims, 1001))
        for bad in ({}, {"amr": ["pwd"], "auth_time": 1000}, {**claims, "auth_time": -50000},
                    {**claims, "auth_time": 2000}, {**claims, "auth_time": float("nan")}):
            self.assertFalse(google_mfa_is_fresh(bad, 1001))
        with patch("utils.staff_sessions.get_security_store", return_value=self.store):
            session = create_staff_session(self.actor, claims, now=1001)
            validate_staff_session(self.actor, session, now=1002)
            with self.assertRaises(PermissionError):
                validate_staff_session(self.actor, session, now=1002 + 1800)
            revoke_staff_sessions(self.actor.principal_id, now=1003)
            with self.assertRaises(PermissionError):
                validate_staff_session(self.actor, session, now=1004)

    def test_totp_vector_replay_recovery_and_rate_limit(self):
        from utils.staff_sessions import totp_at, create_staff_session
        from utils.security_store import security_key
        key = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"  # Public RFC test vector, not a credential.
        self.assertEqual(totp_at(key, 1), "287082")
        authority = security_key("staff-authority", self.actor.principal_id)
        self.store.rows[authority] = dict(mfa_secret_ref="synthetic-ref", mfa_status="enrolled", epoch=1,
            recovery_hashes=[security_key("recovery", "synthetic-recovery")])
        with patch("utils.staff_sessions.get_security_store", return_value=self.store), \
             patch("utils.staff_sessions.load_json_secret", return_value={"totp_key": key}):
            create_staff_session(self.actor, {}, code="287082", now=59)
            with self.assertRaises(PermissionError):
                create_staff_session(self.actor, {}, code="287082", now=59)
            create_staff_session(self.actor, {}, code="synthetic-recovery", now=60)
            with self.assertRaises(PermissionError):
                create_staff_session(self.actor, {}, code="synthetic-recovery", now=60)
            for _ in range(5):
                with self.assertRaises(PermissionError):
                    create_staff_session(self.actor, {}, code="bad", now=61)
            with self.assertRaises(PermissionError):
                create_staff_session(self.actor, {}, code=totp_at(key, 2), now=61)

    def test_month_boundaries_reminders_never_stop_or_delete(self):
        from utils.enrollment_security import calendar_month_after, deadline_reminders
        tz = timezone.utc
        self.assertEqual(calendar_month_after(datetime(2024, 1, 31, tzinfo=tz)).day, 29)
        self.assertEqual(calendar_month_after(datetime(2026, 12, 31, tzinfo=tz)).year, 2027)
        enrollment = {**self.control, "collection_end": "2026-10-09T00:00:00+00:00",
                      "deletion_due": "2026-11-09T00:00:00+00:00"}
        before = deepcopy(enrollment)
        self.assertEqual(deadline_reminders(enrollment, now=datetime(2026, 10, 8, tzinfo=tz))[0]["stage"], "24h")
        self.assertEqual(deadline_reminders(enrollment, now=datetime(2026, 10, 10, tzinfo=tz))[0]["owner"], "Admin")
        self.assertEqual(before, enrollment)


class AtomicOAuthTests(unittest.TestCase):
    def test_concurrent_callbacks_only_one_claim_and_crash_does_not_reopen(self):
        from utils.health_token_store import mark_oauth_state_used, HEALTH_STATES_TAB
        from utils.firestore_schema import spec_for_source_sheet, stable_document_id
        store = MemorySecurityStore()
        row = dict(state="synthetic-state", provider="fitbit", watchName="SYNTHETIC", project="Synthetic-A",
                   enrollment_id="synthetic-enrollment", collection_generation=1, used="FALSE",
                   expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
        spec = spec_for_source_sheet(HEALTH_STATES_TAB)
        store.rows[(spec.collection, stable_document_id(spec, row, 0))] = row
        store.rows[collection_key("SYNTHETIC", "fitbit")] = dict(enrollment_id="synthetic-enrollment",
            generation=1, project="Synthetic-A", provider="fitbit", collection_stopped=False)
        def claim(_):
            try:
                mark_oauth_state_used(Mock(), state_row=row, code="synthetic-code")
                return True
            except ValueError:
                return False
        with patch("utils.health_token_store.GoogleSheetsAdapter._is_firestore_sheet", return_value=True), \
             patch("utils.security_store.get_security_store", return_value=store), \
             patch("utils.health_token_store.participant_disclosure_enforced", return_value=False):
            with ThreadPoolExecutor(max_workers=8) as pool:
                self.assertEqual(sum(pool.map(claim, range(8))), 1)
            self.assertFalse(claim(0))  # A crash after claim still requires a fresh state.

    def test_stale_fenced_token_write_is_denied(self):
        store = MemorySecurityStore()
        key = collection_key("SYNTHETIC", "fitbit")
        store.rows[key] = dict(generation=2, collection_stopped=True)
        with self.assertRaises(CollectionStopped):
            store.fenced_write(key, 1, "fitbit_oauth_tokens", "synthetic-token", {"status": "connected"})
        self.assertNotIn(("fitbit_oauth_tokens", "synthetic-token"), store.rows)


class AdditionalBoundaryTests(unittest.TestCase):
    def test_legacy_inventory_counts_do_not_infer_or_authorize_identities(self):
        from scripts.casa_staff_migration import legacy_assignment_counts
        settings = {"synthetic-prefix": "Manager,Synthetic-A", "users": {"synthetic-student": "Student,Synthetic-A"},
                    "unrelated_config": "not an assignment"}
        self.assertEqual(legacy_assignment_counts(settings), {"Admin": 0, "Manager": 1, "Student": 1})
        self.assertEqual(user_access_from_secrets(settings, "synthetic-prefix@approved.example"), ("Guest", "None"))

    def setUp(self):
        enable = patch.dict("os.environ", {"GOOGLE_MFA_CLAIMS_ENABLED": "true"})
        enable.start()
        self.addCleanup(enable.stop)

    def test_google_mfa_claims_are_not_enabled_without_feasibility_approval(self):
        from utils.staff_sessions import create_staff_session
        actor = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        with patch.dict("os.environ", {"GOOGLE_MFA_CLAIMS_ENABLED": "false"}), \
             patch("utils.staff_sessions.get_security_store", return_value=MemorySecurityStore()):
            with self.assertRaises(PermissionError):
                create_staff_session(actor, {"amr": ["mfa"], "auth_time": 1000}, now=1001)

    def test_expired_session_logout_revokes_other_tabs(self):
        from utils.staff_sessions import create_staff_session, validate_staff_session, revoke_session_owner
        store = MemorySecurityStore()
        actor = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        with patch("utils.staff_sessions.get_security_store", return_value=store):
            old = create_staff_session(actor, {"amr": ["mfa"], "auth_time": 1000}, now=1001)
            newer = create_staff_session(actor, {"amr": ["mfa"], "auth_time": 2000}, now=2001)
            revoke_session_owner(old, now=3000)
            with self.assertRaises(PermissionError):
                validate_staff_session(actor, newer, now=3001)

    def test_expired_native_identity_can_self_revoke_but_not_read_data(self):
        from utils.access_control import native_logout_principal
        settings, claims = staff_fixture()
        expired = {**claims, "exp": 0}
        self.assertFalse(staff_identity_assignment(settings, expired, now=1000).can_read_real_data)
        self.assertEqual(native_logout_principal(expired), staff_identity_assignment(settings, claims, now=1000).principal_id)
        self.assertEqual(native_logout_principal({**expired, "iss": "https://unapproved.example"}), "")

    def test_incomplete_health_tokens_never_reach_secret_store(self):
        from utils.health_token_store import _store_token_payload
        with patch("utils.health_token_store.store_json_secret") as store:
            with self.assertRaises(ValueError):
                _store_token_payload(provider="google_health", project="Synthetic-A", watch_name="SYNTHETIC",
                    token_id="synthetic", token_data={"access_token": "synthetic"})
            store.assert_not_called()

    def test_archive_discards_inflight_response_and_does_not_start_next_request(self):
        from run_drive_archive_collection import _fitbit_heart_rate_records, PeriodWindow
        from datetime import date
        response = Mock(ok=True)
        guard = Mock(side_effect=[None, CollectionStopped("Synthetic withdrawal")])
        with patch("run_drive_archive_collection.requests.get", return_value=response) as request:
            with self.assertRaises(CollectionStopped):
                _fitbit_heart_rate_records("synthetic", PeriodWindow("synthetic", date(2026, 10, 1), date(2026, 10, 2), "daily"),
                    collection_guard=guard)
            self.assertEqual(request.call_count, 1)
            response.json.assert_not_called()

    def test_archive_stop_prevents_any_external_request(self):
        from run_drive_archive_collection import _fitbit_api_json
        with patch("run_drive_archive_collection.requests.get") as request:
            with self.assertRaises(CollectionStopped):
                _fitbit_api_json("synthetic", "synthetic", data_type="heart_rate",
                    collection_guard=Mock(side_effect=CollectionStopped("Synthetic withdrawal")))
            request.assert_not_called()

    def test_stale_staff_capability_is_rechecked_at_service_boundary(self):
        from utils.access_control import AccessDenied, require_project_access
        former = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        with patch.dict("os.environ", {"DATA_BACKEND": "firestore"}), \
             patch("utils.access_control.current_access_context", return_value=AccessContext.guest()):
            with self.assertRaises(AccessDenied):
                require_project_access("Synthetic-A", former)

    def test_missing_enrollment_fails_closed_at_cutover(self):
        store = MemorySecurityStore()
        with patch.dict("os.environ", {"DATA_BACKEND": "firestore", "ENROLLMENT_BINDING_REQUIRED": "true"}), \
             patch("utils.collection_control.get_security_store", return_value=store):
            with self.assertRaises(CollectionStopped):
                assert_collection_allowed("SYNTHETIC", "fitbit")

    def test_same_subject_duplicate_binding_denied(self):
        settings, claims = staff_fixture()
        settings["staff_identities"] *= 2
        self.assertFalse(staff_identity_assignment(settings, claims, now=1000).can_read_real_data)

    def test_pinned_secret_versions_never_use_latest_for_publication(self):
        client = Mock()
        client.add_secret_version.return_value = SimpleNamespace(name="projects/synthetic/secrets/demo/versions/7")
        store = GoogleSecretStore("synthetic", client)
        reference = store.put_json(secret_id="demo", payload={"synthetic": True},
            existing_ref="projects/synthetic/secrets/demo/versions/6", pin_version=True)
        self.assertEqual(reference, "projects/synthetic/secrets/demo/versions/7")
        self.assertEqual(client.add_secret_version.call_args.kwargs["request"]["parent"], "projects/synthetic/secrets/demo")
        client.create_secret.assert_not_called()

    def test_pinned_secret_version_accepts_trusted_project_number_response(self):
        client = Mock()
        client.add_secret_version.return_value = SimpleNamespace(name="projects/123456789/secrets/demo/versions/7")
        self.assertEqual(GoogleSecretStore("synthetic", client).put_json(secret_id="demo", payload={"synthetic": True},
            pin_version=True), "projects/123456789/secrets/demo/versions/7")

    def test_old_enrollment_fitbit_token_is_not_read(self):
        from utils.fitbit_token_store import get_latest_tokens
        control = dict(enrollment_id="new-subject", generation=2)
        with patch("utils.collection_control.assert_collection_allowed", return_value=control), \
             patch("utils.fitbit_token_store._get_latest_token_row", return_value={
                 "enrollment_id": "old-subject", "token_secret_ref": "synthetic-ref"}), \
             patch("utils.fitbit_token_store.load_json_secret") as secret:
            with self.assertRaises(CollectionStopped):
                get_latest_tokens(Mock(), "SYNTHETIC")
            secret.assert_not_called()

    def test_scope_validation_missing_partial_extra_and_complete(self):
        from utils.oauth_permissions import require_granted_scopes
        from utils.health_oauth_clients import DEFAULT_GOOGLE_HEALTH_SCOPES
        scopes = DEFAULT_GOOGLE_HEALTH_SCOPES
        require_granted_scopes(" ".join(scopes), scopes, provider="google_health")
        for value in (None, scopes[0], " ".join([*scopes, "https://www.googleapis.com/auth/googlehealth.unapproved.readonly"])):
            with self.assertRaises(ValueError):
                require_granted_scopes(value, scopes, provider="google_health")
        require_granted_scopes("activity heartrate sleep respiratory_rate", ["activity", "heartrate"], provider="fitbit")
        with self.assertRaises(ValueError):
            require_granted_scopes("activity", ["activity", "heartrate"], provider="fitbit")

    def test_oauth_endpoint_cannot_send_credentials_to_arbitrary_url(self):
        from utils.google_health_oauth import refresh_google_health_tokens
        with patch("utils.google_health_oauth.requests.post") as post:
            with self.assertRaises(ValueError):
                refresh_google_health_tokens(refresh_token="synthetic", client_id="synthetic", client_secret="synthetic",
                    token_uri="https://unapproved.example/token")
            post.assert_not_called()

    def test_mfa_secure_enrollment_and_recovery(self):
        from utils.staff_sessions import issue_mfa_enrollment, prepare_mfa_enrollment, confirm_mfa_enrollment, totp_at
        from utils.security_store import security_key
        store = MemorySecurityStore()
        admin = AccessContext("authenticated", "admin@approved.example", "Admin", "*", "synthetic-admin")
        actor = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        with patch("utils.staff_sessions.get_security_store", return_value=store), \
             patch("utils.staff_sessions.time.time", return_value=1000), \
             patch("utils.staff_sessions.store_json_secret", return_value="synthetic-ref") as save:
            with self.assertRaises(PermissionError):
                issue_mfa_enrollment(actor.principal_id, actor)
            approval = issue_mfa_enrollment(actor.principal_id, admin)
            with self.assertRaises(PermissionError):
                prepare_mfa_enrollment(admin, approval)
            key = prepare_mfa_enrollment(actor, approval)
            with self.assertRaises(PermissionError):
                prepare_mfa_enrollment(actor, approval)
            with patch("utils.staff_sessions.load_json_secret", return_value={"totp_key": key}):
                recovery = confirm_mfa_enrollment(actor, totp_at(key, 1000 // 30))
                with self.assertRaises(PermissionError):
                    confirm_mfa_enrollment(actor, totp_at(key, 1000 // 30))
            authority = store.read(security_key("staff-authority", actor.principal_id))
            self.assertEqual(authority["mfa_status"], "enrolled")
            self.assertNotIn(key, repr(authority))
            self.assertEqual(len(recovery), 8)
            for code in recovery:
                self.assertNotIn(code, repr(authority))
            self.assertEqual(save.call_args.args[2], {"totp_key": key})

    def test_session_polling_does_not_extend_idle_and_absolute_timeout(self):
        from utils.staff_sessions import create_staff_session, validate_staff_session
        store = MemorySecurityStore()
        actor = AccessContext("authenticated", "researcher@approved.example", "Manager", "Synthetic-A", "synthetic-staff")
        with patch("utils.staff_sessions.get_security_store", return_value=store):
            session = create_staff_session(actor, {"amr": ["mfa"], "auth_time": 1000}, now=1001)
            validate_staff_session(actor, session, now=2000, touch=False)
            with self.assertRaises(PermissionError):
                validate_staff_session(actor, session, now=2801, touch=False)
            for now in range(1100, 29800, 100):
                validate_staff_session(actor, session, now=now)
            with self.assertRaises(PermissionError):
                validate_staff_session(actor, session, now=29801)

    def test_management_code_concurrent_consumption(self):
        from utils.participant_sessions import issue_management_code, exchange_management_code
        store = MemorySecurityStore()
        control = dict(watchName="SYNTHETIC", provider="fitbit", project="Synthetic-A", enrollment_id="synthetic-enrollment")
        store.rows[collection_key("SYNTHETIC", "fitbit")] = control
        with patch("utils.participant_sessions.get_security_store", return_value=store):
            code = issue_management_code(control)
            def attempt(index):
                try:
                    return bool(exchange_management_code(code, browser_nonce=f"synthetic-browser-{index}"))
                except PermissionError:
                    return False
            with ThreadPoolExecutor(max_workers=8) as pool:
                self.assertEqual(sum(pool.map(attempt, range(8))), 1)

    def test_sdk_adapter_reads_dependencies_before_write(self):
        from utils.security_store import FirestoreSecurityStore
        operations = []
        class Reference:
            def __init__(self, collection, key):
                self.collection, self.key = collection, key
            def get(self, transaction=None):
                operations.append(("read", self.collection, self.key))
                return SimpleNamespace(exists=True, to_dict=lambda: {"value": 1})
        class Collection:
            def __init__(self, name):
                self.name = name
            def document(self, key):
                return Reference(self.name, key)
        transaction = SimpleNamespace(set=lambda reference, data: operations.append(("write", reference.collection, reference.key)))
        client = SimpleNamespace(collection=Collection, transaction=lambda: transaction)
        fake_cloud = SimpleNamespace(firestore=SimpleNamespace(transactional=lambda function: function))
        with patch.dict("sys.modules", {"google.cloud": fake_cloud}):
            result = FirestoreSecurityStore(client).mutate_document("synthetic", "target",
                lambda row: {**row, "updated": True}, dependency_keys=["dependency"],
                validate_dependencies=lambda row, dependencies: self.assertEqual(dependencies, [{"value": 1}]))
        self.assertEqual([op[0] for op in operations], ["read", "read", "write"])
        self.assertTrue(result["updated"])
