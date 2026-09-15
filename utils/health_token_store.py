from __future__ import annotations

import hashlib
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import gspread

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from utils.compliance import (
    disclosure_document_hash,
    disclosure_version,
    participant_disclosure_enforced,
)
from utils.secret_store import (
    load_json_secret,
    plaintext_secret_fallback_allowed,
    secret_manager_enabled,
    store_json_secret,
)


HEALTH_STATES_TAB = "health_oauth_states"
HEALTH_USED_TAB = "health_oauth_state_used"
HEALTH_TOKENS_TAB = "health_oauth_tokens"
HEALTH_REAUTH_TAB = "health_reauth_queue"
HEALTH_LOGS_TAB = "health_api_logs"
HEALTH_CONSENTS_TAB = "health_oauth_consents"
FITBIT_SHEET = "fitbit"

STATE_COLUMNS = [
    "state", "provider", "watchName", "project", "oauth_client_key", "purpose",
    "created_by", "created_at", "expires_at", "used", "used_at", "callback_error",
    "staff_consent_verified", "staff_consent_verified_at", "adult_verified",
    "participant_language", "participant_disclosure_version",
    "participant_document_hash", "participant_acknowledged_at",
]
USED_COLUMNS = ["state", "provider", "watchName", "used_at", "code_hash"]
TOKEN_COLUMNS = [
    "token_id", "watchName", "project", "provider", "oauth_client_key",
    "health_user_id", "legacy_fitbit_user_id", "token_secret_ref", "access_token", "refresh_token",
    "access_expires_at", "refresh_expires_at", "refresh_token_expires_in", "scope",
    "token_type", "auth_status", "last_refresh_at", "last_refresh_error",
    "created_at", "updated_at", "is_active",
]
CONSENT_COLUMNS = [
    "consent_id", "state", "watchName", "project", "provider", "purpose",
    "language", "disclosure_version", "document_hash", "staff_consent_verified_at",
    "adult_verified", "participant_acknowledged_at",
]
REAUTH_COLUMNS = [
    "queue_id", "watchName", "project", "provider", "reason", "detected_at",
    "reauth_link", "reauth_state", "status", "completed_at", "last_error",
]
LOG_COLUMNS = [
    "log_id", "timestamp", "watchName", "project", "provider", "operation",
    "status", "http_status", "data_type", "start_time", "end_time", "message", "error",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def new_uuid() -> str:
    return str(uuid.uuid4())


def ts_to_iso(ts: int | float | None) -> str:
    if ts in (None, ""):
        return ""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def iso_to_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _ordered_row(columns: list[str], values: dict[str, Any]) -> OrderedDict:
    return OrderedDict((column, values.get(column, "")) for column in columns)


def _worksheet(spreadsheet: Spreadsheet, tab: str):
    return spreadsheet.get_gspread_connection().worksheet(tab)


def _find_row_number(
    worksheet,
    *,
    keys: dict[str, Any],
    require_latest: bool = True,
) -> tuple[int | None, list[str]]:
    headers = worksheet.row_values(1)
    if not headers:
        return None, []

    records = worksheet.get_all_records()
    matched_rows: list[int] = []
    for offset, record in enumerate(records, start=2):
        if all(_values_equal(record.get(key, ""), value) for key, value in keys.items()):
            matched_rows.append(offset)

    if not matched_rows:
        return None, headers
    return (matched_rows[-1] if require_latest else matched_rows[0]), headers


def _values_equal(left: Any, right: Any) -> bool:
    def normalize(value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value or "").strip()

    return normalize(left) == normalize(right)


def _update_row_by_keys(
    spreadsheet: Spreadsheet,
    tab: str,
    *,
    keys: dict[str, Any],
    updates: dict[str, Any],
) -> bool:
    try:
        ws = _worksheet(spreadsheet, tab)
    except gspread.exceptions.WorksheetNotFound:
        return False

    row_number, headers = _find_row_number(ws, keys=keys)
    if not row_number:
        return False

    for key, value in updates.items():
        if key not in headers:
            continue
        ws.update_cell(row_number, headers.index(key) + 1, "" if value is None else str(value))
    return True


def _append(spreadsheet: Spreadsheet, tab: str, columns: list[str], values: dict[str, Any]) -> None:
    row = _ordered_row(columns, values)
    workbook = spreadsheet.get_gspread_connection()
    try:
        ws = workbook.worksheet(tab)
    except gspread.exceptions.WorksheetNotFound:
        ws = workbook.add_worksheet(title=tab, rows=1000, cols=max(len(columns), 1))
        ws.append_row(columns)

    headers = ws.row_values(1)
    if not headers:
        headers = columns
        ws.append_row(headers)

    missing_headers = [column for column in columns if column not in headers]
    if missing_headers:
        headers = headers + missing_headers
        ws.resize(cols=len(headers))
        ws.update("1:1", [headers])

    ws.append_row([row.get(header, "") for header in headers])


def save_oauth_state(
    spreadsheet: Spreadsheet,
    *,
    state: str,
    provider: str,
    watchName: str,
    project: str,
    oauth_client_key: str,
    purpose: str = "connect",
    created_by: str | None = None,
    staff_consent_verified: bool = False,
    adult_verified: bool = False,
    ttl_hours: int = 48,
) -> None:
    now = utc_now()
    _append(
        spreadsheet,
        HEALTH_STATES_TAB,
        STATE_COLUMNS,
        {
            "state": state,
            "provider": provider,
            "watchName": watchName,
            "project": project,
            "oauth_client_key": oauth_client_key,
            "purpose": purpose,
            "created_by": created_by or "",
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(hours=ttl_hours)).isoformat(),
            "used": "FALSE",
            "used_at": "",
            "callback_error": "",
            "staff_consent_verified": "TRUE" if staff_consent_verified else "FALSE",
            "staff_consent_verified_at": now.isoformat() if staff_consent_verified else "",
            "adult_verified": "TRUE" if adult_verified else "FALSE",
            "participant_language": "",
            "participant_disclosure_version": "",
            "participant_document_hash": "",
            "participant_acknowledged_at": "",
        },
    )


def resolve_oauth_state(spreadsheet: Spreadsheet, *, state: str, provider: str) -> dict[str, Any]:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        HEALTH_STATES_TAB,
        "state",
        "provider",
        state=state,
        provider=provider,
    )
    if not rows:
        raise ValueError("Unknown OAuth state")

    state_row = rows[-1]
    if str(state_row.get("used", "FALSE")).upper() == "TRUE":
        raise ValueError("OAuth state was already used")

    expires_at = iso_to_dt(str(state_row.get("expires_at") or ""))
    if expires_at and utc_now() > expires_at:
        raise ValueError("OAuth state expired")

    used_rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        HEALTH_USED_TAB,
        "state",
        "provider",
        state=state,
        provider=provider,
    )
    if used_rows:
        raise ValueError("OAuth state replay detected")

    return state_row


def resolve_oauth_state_any_provider(spreadsheet: Spreadsheet, *, state: str) -> dict[str, Any]:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        HEALTH_STATES_TAB,
        "state",
        state=state,
    )
    if not rows:
        raise ValueError("Unknown OAuth state")
    provider = str(rows[-1].get("provider") or "")
    if provider not in {"fitbit", "google_health"}:
        raise ValueError("OAuth state has an unsupported provider")
    return resolve_oauth_state(spreadsheet, state=state, provider=provider)


def _is_true(value: Any) -> bool:
    return str(value or "").strip().upper() == "TRUE"


def assert_state_authorized_for_callback(state_row: dict[str, Any]) -> None:
    """Fail closed once the ethics-approved participant disclosure is enforced."""
    if not participant_disclosure_enforced():
        return
    if not _is_true(state_row.get("staff_consent_verified")):
        raise ValueError("Staff consent verification is missing")
    if not _is_true(state_row.get("adult_verified")):
        raise ValueError("Adult eligibility verification is missing")
    if not str(state_row.get("participant_acknowledged_at") or "").strip():
        raise ValueError("Participant disclosure acknowledgement is missing")
    if str(state_row.get("participant_disclosure_version") or "") != disclosure_version():
        raise ValueError("Participant disclosure version is no longer current")


def acknowledge_oauth_state(
    spreadsheet: Spreadsheet,
    *,
    state_row: dict[str, Any],
    language: str,
) -> dict[str, Any]:
    """Persist a versioned, pseudonymous acknowledgement without names or IPs."""
    if language not in {"en", "he"}:
        raise ValueError("Unsupported disclosure language")
    if not _is_true(state_row.get("staff_consent_verified")):
        raise ValueError("Research staff have not verified the approved study consent")
    if not _is_true(state_row.get("adult_verified")):
        raise ValueError("This authorization flow is limited to adults")

    existing_time = str(state_row.get("participant_acknowledged_at") or "").strip()
    if existing_time:
        return state_row

    acknowledged_at = utc_now_iso()
    version = disclosure_version()
    document_hash = disclosure_document_hash()
    updates = {
        "participant_language": language,
        "participant_disclosure_version": version,
        "participant_document_hash": document_hash,
        "participant_acknowledged_at": acknowledged_at,
    }
    # Append the immutable audit record first. The state is only marked as
    # acknowledged after the consent record exists, so a partial Sheets failure
    # cannot allow a callback without its corresponding audit evidence.
    _append(
        spreadsheet,
        HEALTH_CONSENTS_TAB,
        CONSENT_COLUMNS,
        {
            "consent_id": new_uuid(),
            "state": state_row["state"],
            "watchName": state_row.get("watchName", ""),
            "project": state_row.get("project", ""),
            "provider": state_row.get("provider", ""),
            "purpose": state_row.get("purpose", "connect"),
            "language": language,
            "disclosure_version": version,
            "document_hash": document_hash,
            "staff_consent_verified_at": state_row.get("staff_consent_verified_at", ""),
            "adult_verified": "TRUE",
            "participant_acknowledged_at": acknowledged_at,
        },
    )
    updated = _update_row_by_keys(
        spreadsheet,
        HEALTH_STATES_TAB,
        keys={"state": state_row["state"], "provider": state_row["provider"]},
        updates=updates,
    )
    if not updated:
        raise RuntimeError("Could not persist participant acknowledgement")
    return {**state_row, **updates}


def mark_oauth_state_used(
    spreadsheet: Spreadsheet,
    *,
    state_row: dict[str, Any],
    code: str | None = None,
) -> None:
    code_hash = hashlib.sha256(code.encode()).hexdigest() if code else ""
    used_at = utc_now_iso()
    _append(
        spreadsheet,
        HEALTH_USED_TAB,
        USED_COLUMNS,
        {
            "state": state_row["state"],
            "provider": state_row["provider"],
            "watchName": state_row["watchName"],
            "used_at": used_at,
            "code_hash": code_hash,
        },
    )
    _update_row_by_keys(
        spreadsheet,
        HEALTH_STATES_TAB,
        keys={"state": state_row["state"], "provider": state_row["provider"]},
        updates={"used": "TRUE", "used_at": used_at},
    )


def _identity_value(identity: dict[str, Any] | None, *keys: str) -> str:
    if not identity:
        return ""
    for key in keys:
        value = identity.get(key)
        if value:
            return str(value)
    return ""


def get_active_token_row(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    provider: str,
) -> dict[str, Any] | None:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        HEALTH_TOKENS_TAB,
        "watchName",
        "provider",
        watchName=watchName,
        provider=provider,
    )
    active = [row for row in rows if str(row.get("is_active", "TRUE")).upper() == "TRUE"]
    if not active:
        return None
    row = dict(active[-1])
    secret_ref = str(row.get("token_secret_ref") or "").strip()
    if secret_ref:
        try:
            row.update(load_json_secret(secret_ref))
        except Exception:
            if not plaintext_secret_fallback_allowed() or not row.get("access_token"):
                raise
    elif not plaintext_secret_fallback_allowed() and (row.get("access_token") or row.get("refresh_token")):
        raise RuntimeError("Plaintext OAuth token fallback is disabled")
    return row


def _store_token_payload(
    *,
    provider: str,
    project: str,
    watch_name: str,
    token_id: str,
    token_data: dict[str, Any],
    existing_ref: str = "",
) -> tuple[str, str, str]:
    access_token = str(token_data.get("access_token") or "")
    refresh_token = str(token_data.get("refresh_token") or "")
    secret_ref = store_json_secret(
        "participant-oauth",
        [provider, project, watch_name, token_id],
        {"access_token": access_token, "refresh_token": refresh_token},
        existing_ref=existing_ref,
    )
    if secret_manager_enabled():
        if not secret_ref:
            raise RuntimeError("Secret Manager did not return a token secret reference")
        return secret_ref, "", ""
    return "", access_token, refresh_token


def is_access_token_valid(token_row: dict[str, Any], safety_margin_minutes: int = 5) -> bool:
    expires_at = iso_to_dt(str(token_row.get("access_expires_at") or ""))
    if not expires_at:
        return False
    return utc_now() < expires_at - timedelta(minutes=safety_margin_minutes)


def upsert_active_token_row(spreadsheet: Spreadsheet, row: dict[str, Any]) -> None:
    updated = _update_row_by_keys(
        spreadsheet,
        HEALTH_TOKENS_TAB,
        keys={
            "watchName": row["watchName"],
            "provider": row["provider"],
            "is_active": "TRUE",
        },
        updates=row,
    )
    if not updated:
        _append(spreadsheet, HEALTH_TOKENS_TAB, TOKEN_COLUMNS, row)


def save_google_health_tokens_for_watch(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    oauth_client_key: str,
    token_data: dict[str, Any],
    identity: dict[str, Any] | None = None,
) -> None:
    now = utc_now_iso()
    existing = get_active_token_row(spreadsheet, watchName=watchName, provider="google_health")
    refresh_token = token_data.get("refresh_token") or (existing or {}).get("refresh_token", "")

    health_user_id = _identity_value(identity, "healthUserId", "health_user_id", "userId", "id")
    legacy_fitbit_user_id = _identity_value(
        identity,
        "legacyFitbitUserId",
        "legacyUserId",
        "legacy_fitbit_user_id",
        "fitbitUserId",
    )

    token_id = (existing or {}).get("token_id") or new_uuid()
    token_secret_ref, access_token, stored_refresh_token = _store_token_payload(
        provider="google_health",
        project=project,
        watch_name=watchName,
        token_id=token_id,
        token_data={**token_data, "refresh_token": refresh_token},
        existing_ref=str((existing or {}).get("token_secret_ref") or ""),
    )
    row = {
        "token_id": token_id,
        "watchName": watchName,
        "project": project,
        "provider": "google_health",
        "oauth_client_key": oauth_client_key,
        "health_user_id": health_user_id,
        "legacy_fitbit_user_id": legacy_fitbit_user_id,
        "token_secret_ref": token_secret_ref,
        "access_token": access_token,
        "refresh_token": stored_refresh_token,
        "access_expires_at": ts_to_iso(token_data.get("access_expires_at")),
        "refresh_expires_at": ts_to_iso(token_data.get("refresh_expires_at")),
        "refresh_token_expires_in": token_data.get("refresh_token_expires_in") or "",
        "scope": token_data.get("scope") or "",
        "token_type": token_data.get("token_type") or "Bearer",
        "auth_status": "connected",
        "last_refresh_at": (existing or {}).get("last_refresh_at", ""),
        "last_refresh_error": "",
        "created_at": (existing or {}).get("created_at") or now,
        "updated_at": now,
        "is_active": "TRUE",
    }
    upsert_active_token_row(spreadsheet, row)
    update_fitbit_registry_after_auth(
        spreadsheet,
        watchName=watchName,
        project=project,
        provider="google_health",
        oauth_client_key=oauth_client_key,
        health_user_id=health_user_id,
        legacy_fitbit_user_id=legacy_fitbit_user_id,
        auth_status="connected",
    )


def update_token_row_after_refresh(
    spreadsheet: Spreadsheet,
    token_row: dict[str, Any],
    token_data: dict[str, Any],
) -> None:
    now = utc_now_iso()
    refresh_token = token_data.get("refresh_token") or token_row.get("refresh_token", "")
    secret_ref, access_token, stored_refresh_token = _store_token_payload(
        provider=str(token_row.get("provider") or "google_health"),
        project=str(token_row.get("project") or ""),
        watch_name=str(token_row.get("watchName") or ""),
        token_id=str(token_row.get("token_id") or new_uuid()),
        token_data={**token_data, "refresh_token": refresh_token},
        existing_ref=str(token_row.get("token_secret_ref") or ""),
    )
    updates = {
        "token_secret_ref": secret_ref,
        "access_token": access_token,
        "refresh_token": stored_refresh_token,
        "access_expires_at": ts_to_iso(token_data.get("access_expires_at")),
        "refresh_expires_at": ts_to_iso(token_data.get("refresh_expires_at")),
        "refresh_token_expires_in": token_data.get("refresh_token_expires_in") or "",
        "scope": token_data.get("scope") or token_row.get("scope", ""),
        "token_type": token_data.get("token_type") or token_row.get("token_type", "Bearer"),
        "auth_status": "connected",
        "last_refresh_at": now,
        "last_refresh_error": "",
        "updated_at": now,
        "is_active": "TRUE",
    }
    _update_row_by_keys(
        spreadsheet,
        HEALTH_TOKENS_TAB,
        keys={
            "watchName": token_row.get("watchName", ""),
            "provider": token_row.get("provider", ""),
            "is_active": "TRUE",
        },
        updates=updates,
    )


def mark_reauth_required(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    reason: str,
    error: str | None = None,
    reauth_link: str | None = None,
    reauth_state: str | None = None,
) -> None:
    _append(
        spreadsheet,
        HEALTH_REAUTH_TAB,
        REAUTH_COLUMNS,
        {
            "queue_id": new_uuid(),
            "watchName": watchName,
            "project": project,
            "provider": provider,
            "reason": reason,
            "detected_at": utc_now_iso(),
            "reauth_link": reauth_link or "",
            "reauth_state": reauth_state or "",
            "status": "open",
            "completed_at": "",
            "last_error": error or "",
        },
    )
    update_fitbit_registry_auth_status(
        spreadsheet,
        watchName=watchName,
        project=project,
        auth_status="reauth_required",
        last_auth_error=error or reason,
        reauth_link=reauth_link or "",
    )


def update_fitbit_registry_after_auth(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    oauth_client_key: str,
    health_user_id: str | None,
    legacy_fitbit_user_id: str | None,
    auth_status: str,
) -> None:
    updates = {
        "oauth_type": provider,
        "provider": provider,
        "oauth_client_key": oauth_client_key,
        "auth_status": auth_status,
        "health_user_id": health_user_id or "",
        "legacy_fitbit_user_id": legacy_fitbit_user_id or "",
        "last_auth_error": "",
    }
    if not _update_row_by_keys(
        spreadsheet,
        FITBIT_SHEET,
        keys={"name": watchName, "project": project},
        updates=updates,
    ):
        _update_row_by_keys(
            spreadsheet,
            FITBIT_SHEET,
            keys={"name": watchName},
            updates=updates,
        )


def update_fitbit_registry_auth_status(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    auth_status: str,
    last_auth_error: str = "",
    reauth_link: str = "",
) -> None:
    updates = {
        "auth_status": auth_status,
        "last_auth_error": last_auth_error,
        "reauth_link": reauth_link,
        "reauth_link_created_at": utc_now_iso() if reauth_link else "",
    }
    if not _update_row_by_keys(
        spreadsheet,
        FITBIT_SHEET,
        keys={"name": watchName, "project": project},
        updates=updates,
    ):
        _update_row_by_keys(spreadsheet, FITBIT_SHEET, keys={"name": watchName}, updates=updates)


def get_valid_access_token(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    provider: str,
    oauth_client_config,
) -> str:
    token_row = get_active_token_row(spreadsheet, watchName=watchName, provider=provider)
    if not token_row:
        mark_reauth_required(
            spreadsheet,
            watchName=watchName,
            project="",
            provider=provider,
            reason="missing_token",
        )
        raise RuntimeError(f"No active token found for {watchName}/{provider}")

    if is_access_token_valid(token_row):
        return str(token_row["access_token"])

    refresh_token = str(token_row.get("refresh_token") or "")
    if not refresh_token:
        mark_reauth_required(
            spreadsheet,
            watchName=watchName,
            project=str(token_row.get("project") or ""),
            provider=provider,
            reason="missing_refresh_token",
        )
        raise RuntimeError(f"Missing refresh token for {watchName}/{provider}")

    try:
        if provider != "google_health":
            raise NotImplementedError(f"Refresh not implemented for provider={provider}")

        from utils.google_health_oauth import refresh_google_health_tokens

        token_data = refresh_google_health_tokens(
            refresh_token=refresh_token,
            client_id=oauth_client_config.client_id,
            client_secret=oauth_client_config.client_secret,
            token_uri=oauth_client_config.token_uri,
        )
        update_token_row_after_refresh(spreadsheet, token_row, token_data)
        return str(token_data["access_token"])
    except Exception as exc:
        error_text = str(exc)
        if any(marker in error_text for marker in ("invalid_grant", "expired", "revoked")):
            mark_reauth_required(
                spreadsheet,
                watchName=watchName,
                project=str(token_row.get("project") or ""),
                provider=provider,
                reason="invalid_grant",
                error=error_text,
            )
        raise


def log_health_api_event(
    spreadsheet: Spreadsheet,
    *,
    provider: str,
    operation: str,
    status: str,
    watchName: str = "",
    project: str = "",
    http_status: int | str = "",
    data_type: str = "",
    start_time: str = "",
    end_time: str = "",
    message: str = "",
    error: str = "",
) -> None:
    _append(
        spreadsheet,
        HEALTH_LOGS_TAB,
        LOG_COLUMNS,
        {
            "log_id": new_uuid(),
            "timestamp": utc_now_iso(),
            "watchName": watchName,
            "project": project,
            "provider": provider,
            "operation": operation,
            "status": status,
            "http_status": http_status,
            "data_type": data_type,
            "start_time": start_time,
            "end_time": end_time,
            "message": message,
            "error": sanitize_error(error),
        },
    )


def sanitize_error(value: Any, *secrets: str) -> str:
    text = " ".join(str(value or "").strip().split())
    for secret in secrets:
        if secret and len(str(secret)) > 6:
            text = text.replace(str(secret), "[redacted]")
    for marker in ("access_token", "refresh_token", "client_secret", "authorization"):
        if marker in text.casefold():
            return "Sensitive provider error was redacted"
    return text[:500]
