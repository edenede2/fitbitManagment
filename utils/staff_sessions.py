"""Revocable staff sessions and app MFA, using only authoritative stores.

Timeouts below are application design choices, not claimed CASA requirements.
TOTP follows RFC 6238; keys live in Secret Manager, never Firestore or Sheets.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
import math
import os

from utils.security_store import get_security_store, security_key
from utils.secret_store import load_json_secret, store_json_secret

IDLE_SECONDS = 30 * 60
ABSOLUTE_SECONDS = 8 * 60 * 60


def google_mfa_is_fresh(claims: dict, now: float) -> bool:
    methods = claims.get("amr")
    try:
        authenticated = float(claims["auth_time"])
    except (KeyError, ValueError, TypeError):
        return False
    return (math.isfinite(authenticated) and isinstance(methods, (list, tuple)) and "mfa" in methods
            and 0 <= now - authenticated < ABSOLUTE_SECONDS)


def totp_at(key: str, step: int) -> str:
    raw = base64.b32decode(key.upper(), casefold=True)
    digest = hmac.new(raw, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = digest[-1] & 15
    number = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return f"{number % 1000000:06d}"


def _verify_app_mfa(principal: str, code: str, now: float) -> None:
    store = get_security_store()
    authority_key = security_key("staff-authority", principal)
    authority = store.read(authority_key)
    ref = str(authority.get("mfa_secret_ref") or "")
    if not ref or authority.get("mfa_status") != "enrolled":
        raise PermissionError("Ask an Admin for secure MFA enrollment")
    # External secret access occurs OUTSIDE the retried transaction callback.
    secret = load_json_secret(ref)
    expected = {step: totp_at(secret["totp_key"], step) for step in range(int(now // 30) - 1, int(now // 30) + 2)}
    recovery_hash = security_key("recovery", code)

    def verify(before):
        if before.get("mfa_secret_ref") != ref or before.get("mfa_status") != "enrolled":
            raise PermissionError("MFA enrollment changed")
        failures = int(before.get("failures", 0)) if now < before.get("reset_at", 0) else 0
        if failures >= 5:
            return {**before, "verification_ok": False}
        recovery = list(before.get("recovery_hashes") or [])
        accepted = None
        if len(code) == 6 and code.isdigit():
            accepted = next((step for step, value in expected.items()
                             if step > int(before.get("last_totp_step", -1)) and hmac.compare_digest(code, value)), None)
        recovered = recovery_hash in recovery
        ok = accepted is not None or recovered
        if recovered:
            recovery.remove(recovery_hash)
        return {**before, "verification_ok": ok, "failures": 0 if ok else failures + 1,
                "reset_at": now + 15 * 60 if now >= before.get("reset_at", 0) else before["reset_at"],
                "last_totp_step": accepted if accepted is not None else before.get("last_totp_step", -1),
                "recovery_hashes": recovery, "last_recovery_at": now if recovered else before.get("last_recovery_at", 0)}

    result = store.mutate(authority_key, verify)
    if not result.get("verification_ok"):
        raise PermissionError("Invalid, replayed or rate-limited MFA code")


def create_staff_session(actor, claims: dict, *, code: str = "", now: float | None = None) -> str:
    if not actor.can_read_real_data or not actor.principal_id:
        raise PermissionError("An approved staff identity is required")
    now = time.time() if now is None else now
    store = get_security_store()
    authority_key = security_key("staff-authority", actor.principal_id)
    authority = store.read(authority_key)
    claims_enabled = os.getenv("GOOGLE_MFA_CLAIMS_ENABLED", "false").casefold() == "true"
    google_verified = claims_enabled and google_mfa_is_fresh(claims, now) and float(claims["auth_time"]) > authority.get("revoked_before", 0)
    if not google_verified:
        _verify_app_mfa(actor.principal_id, code.strip(), now)
    session_id = secrets.token_urlsafe(32)
    session = {"principal_id": actor.principal_id, "role": actor.role, "project": actor.project,
               "epoch": int(authority.get("epoch", 0)), "created_at": now, "last_seen_at": now,
               "mfa_method": "google_claims" if google_verified else "app_mfa", "revoked": False}
    store.mutate(security_key("staff-session", session_id), lambda _: session)
    return session_id


def validate_staff_session(actor, session_id: str, *, now: float | None = None, touch: bool = True) -> None:
    now = time.time() if now is None else now
    store = get_security_store()
    def validate(before, dependencies):
        authority = dependencies[0]
        if (not before or before.get("revoked")
                or any(before.get(k) != getattr(actor, k) for k in ("principal_id", "role", "project"))
                or before.get("epoch") != int(authority.get("epoch", 0))
                or now - before["created_at"] >= ABSOLUTE_SECONDS
                or now - before["last_seen_at"] >= IDLE_SECONDS):
            raise PermissionError("Staff session expired or revoked")
    store.mutate_document("_casa_security", security_key("staff-session", session_id),
        lambda before: {**before, "last_seen_at": now} if touch else before,
        dependency_keys=[security_key("staff-authority", actor.principal_id)], validate_dependencies=validate)


def revoke_staff_sessions(principal: str, *, now: float | None = None) -> None:
    now = time.time() if now is None else now
    get_security_store().mutate(security_key("staff-authority", principal),
        lambda before: {**before, "epoch": int(before.get("epoch", 0)) + 1, "revoked_before": now})


def revoke_session_owner(session_id: str, *, now: float | None = None) -> None:
    """Logout of an expired session still revokes its owner's other tabs.

    The opaque ID comes only from the server's Streamlit session state, never a
    query parameter. Expired claims cannot grant data access, but must not prevent
    a holder from terminating their own existing application sessions.
    """
    record = get_security_store().read(security_key("staff-session", session_id))
    if record.get("principal_id"):
        revoke_staff_sessions(record["principal_id"], now=now)


def issue_mfa_enrollment(target_principal: str, admin) -> str:
    if not admin.can_manage_devices or admin.role != "Admin":
        raise PermissionError("Admin approval is required")
    code = secrets.token_urlsafe(32)
    now = time.time()
    get_security_store().mutate(security_key("mfa-enrollment", code), lambda _: {
        "principal_id": target_principal, "expires_at": now + 15 * 60,
        "approved_by": admin.principal_id, "used": False})
    return code


def prepare_mfa_enrollment(actor, enrollment_code: str) -> str:
    """Returns a key only to the approved user's setup UI; never log this return."""
    if not actor.can_read_real_data or not actor.principal_id:
        raise PermissionError("An approved staff identity is required")
    store = get_security_store()
    now = time.time()
    key = base64.b32encode(secrets.token_bytes(20)).decode()

    def consume(before):
        if (before.get("principal_id") != actor.principal_id or before.get("used")
                or now >= before.get("expires_at", 0)):
            raise PermissionError("Invalid or expired enrollment approval")
        return {**before, "used": True}

    store.mutate(security_key("mfa-enrollment", enrollment_code), consume)
    ref = store_json_secret("staff-mfa", [actor.principal_id, secrets.token_hex(8)], {"totp_key": key})
    if not ref:
        raise RuntimeError("Secret Manager is required for MFA enrollment")
    store.mutate(security_key("staff-authority", actor.principal_id), lambda before: {
        **before, "mfa_secret_ref": ref, "mfa_status": "pending", "setup_expires_at": now + 15 * 60,
        "epoch": int(before.get("epoch", 0)) + 1, "last_totp_step": -1,
        "setup_failures": 0, "failures": 0, "recovery_hashes": []})
    return key


def confirm_mfa_enrollment(actor, code: str) -> list[str]:
    store = get_security_store()
    authority_key = security_key("staff-authority", actor.principal_id)
    before = store.read(authority_key)
    now = time.time()
    if before.get("mfa_status") != "pending" or now >= before.get("setup_expires_at", 0):
        raise PermissionError("MFA setup expired")
    secret = load_json_secret(before["mfa_secret_ref"])
    step = int(now // 30)
    valid = len(code) == 6 and hmac.compare_digest(code, totp_at(secret["totp_key"], step))
    recovery = [secrets.token_urlsafe(24) for _ in range(8)]

    def confirm(current):
        if (current.get("mfa_status") != "pending" or current.get("mfa_secret_ref") != before["mfa_secret_ref"]
                or now >= current.get("setup_expires_at", 0) or current.get("setup_failures", 0) >= 5):
            raise PermissionError("MFA setup expired or rate-limited")
        if not valid:
            return {**current, "setup_failures": current.get("setup_failures", 0) + 1}
        return {**current, "mfa_status": "enrolled", "last_totp_step": step,
                "recovery_hashes": [security_key("recovery", c) for c in recovery]}

    result = store.mutate(authority_key, confirm)
    if result.get("mfa_status") != "enrolled":
        raise PermissionError("Invalid MFA setup code")
    return recovery
