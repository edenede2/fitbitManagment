"""One-time participant codes. Only hashes reach durable records; no URL tokens."""
from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

from utils.collection_control import collection_key
from utils.security_store import get_security_store, security_key


def _now():
    return datetime.now(timezone.utc)


def _current(enrollment: dict) -> dict:
    current = get_security_store().read(collection_key(enrollment["watchName"], enrollment["provider"]))
    if not current.get("enrollment_id") or any(
        current.get(k) != enrollment.get(k) for k in ("enrollment_id", "project", "provider")
    ):
        raise PermissionError("This enrollment has been reassigned")
    return current


def issue_management_code(enrollment: dict) -> str:
    """Called only after staff authorization or validated provider callback.

    Replacement invalidates prior codes/sessions for the same enrollment.
    """
    _current(enrollment)
    code = secrets.token_urlsafe(32)
    now = _now()
    epoch = secrets.token_hex(16)
    store = get_security_store()
    store.mutate(security_key("management-epoch", enrollment["enrollment_id"]),
                 lambda _: {"epoch": epoch})
    grant = {"watchName": enrollment["watchName"], "project": enrollment["project"],
             "provider": enrollment["provider"], "enrollment_id": enrollment["enrollment_id"],
             "epoch": epoch, "expires_at": (now + timedelta(hours=24)).isoformat(), "used": False}
    store.mutate(security_key("management-code", code), lambda _: grant)
    return code


def exchange_management_code(code: str, *, browser_nonce: str) -> str:
    now = _now()
    store = get_security_store()
    limiter_key = security_key("management-rate", browser_nonce)

    def rate(before):
        if not before or now >= datetime.fromisoformat(before["reset_at"]):
            before = {"attempts": 0, "reset_at": (now + timedelta(minutes=15)).isoformat()}
        if before["attempts"] >= 5:
            raise PermissionError("Please wait before trying another code")
        return {**before, "attempts": before["attempts"] + 1}

    store.mutate(limiter_key, rate)
    grant = store.read(security_key("management-code", code))
    if not grant:
        raise PermissionError("Invalid or expired access code")
    _current(grant)
    epoch = store.read(security_key("management-epoch", grant["enrollment_id"])).get("epoch")

    def consume(current):
        if not current or current.get("used") or now >= datetime.fromisoformat(current["expires_at"]) or current.get("epoch") != epoch:
            raise PermissionError("Invalid or expired access code")
        return {**current, "used": True, "used_at": now.isoformat()}

    def validate_dependencies(current, dependencies):
        control, authority = dependencies
        if (current.get("epoch") != authority.get("epoch")
                or not control.get("enrollment_id")
                or any(current.get(k) != control.get(k) for k in ("enrollment_id", "project", "provider"))):
            raise PermissionError("Invalid or revoked access code")
    consumed = store.mutate_document("_casa_security", security_key("management-code", code), consume,
        dependency_keys=[collection_key(grant["watchName"], grant["provider"]),
                         security_key("management-epoch", grant["enrollment_id"])],
        validate_dependencies=validate_dependencies)
    session = secrets.token_urlsafe(32)
    store.mutate(security_key("management-session", session), lambda _: {
        **consumed, "browser_hash": security_key("browser", browser_nonce),
        "absolute_expires_at": (now + timedelta(minutes=30)).isoformat(), "last_seen_at": now.isoformat(),
    })
    return session


def resolve_management_session(session: str, *, browser_nonce: str) -> dict:
    now = _now()
    store = get_security_store()
    row = store.read(security_key("management-session", session))
    if not row:
        raise PermissionError("Management session expired")
    _current(row)
    epoch = store.read(security_key("management-epoch", row["enrollment_id"])).get("epoch")

    def validate(current):
        if (current.get("browser_hash") != security_key("browser", browser_nonce)
                or current.get("epoch") != epoch
                or now >= datetime.fromisoformat(current["absolute_expires_at"])
                or now - datetime.fromisoformat(current["last_seen_at"]) >= timedelta(minutes=10)):
            raise PermissionError("Management session expired or revoked")
        return {**current, "last_seen_at": now.isoformat()}

    def dependency_check(current, dependencies):
        control, authority = dependencies
        if (current.get("epoch") != authority.get("epoch")
                or any(current.get(k) != control.get(k) for k in ("enrollment_id", "project", "provider"))):
            raise PermissionError("Management session revoked")
    return store.mutate_document("_casa_security", security_key("management-session", session), validate,
        dependency_keys=[collection_key(row["watchName"], row["provider"]),
                         security_key("management-epoch", row["enrollment_id"])],
        validate_dependencies=dependency_check)
