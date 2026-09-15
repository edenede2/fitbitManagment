"""Pseudonymous self-service connection revocation and deletion requests."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from utils.compliance import app_base_url, deletion_text
from utils.health_token_store import (
    _append,
    _update_row_by_keys,
    get_active_token_row,
    new_uuid,
    update_fitbit_registry_auth_status,
    utc_now,
    utc_now_iso,
)
from utils.secret_store import destroy_secret_versions


MANAGEMENT_TAB = "health_connection_management"
DELETION_REQUESTS_TAB = "health_deletion_requests"
MANAGEMENT_COLUMNS = [
    "management_token_hash", "watchName", "project", "provider", "created_at",
    "expires_at", "revoked_at", "deletion_requested_at",
]
DELETION_COLUMNS = [
    "request_id", "management_token_hash", "watchName", "project", "provider",
    "requested_at", "status", "approved_policy_text",
]


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_management_link(
    spreadsheet: Spreadsheet,
    *,
    watch_name: str,
    project: str,
    provider: str,
    validity_days: int = 365,
) -> str:
    token = secrets.token_urlsafe(32)
    now = utc_now()
    _append(
        spreadsheet,
        MANAGEMENT_TAB,
        MANAGEMENT_COLUMNS,
        {
            "management_token_hash": _hash_token(token),
            "watchName": watch_name,
            "project": project,
            "provider": provider,
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(days=validity_days)).isoformat(),
        },
    )
    return f"{app_base_url()}/Manage_Connection?{urlencode({'token': token})}"


def resolve_management_token(spreadsheet: Spreadsheet, token: str) -> dict[str, Any]:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        MANAGEMENT_TAB,
        "management_token_hash",
        management_token_hash=_hash_token(token),
    )
    if not rows:
        raise ValueError("Unknown management link")
    row = rows[-1]
    expires = str(row.get("expires_at") or "")
    if expires and utc_now() > datetime.fromisoformat(expires.replace("Z", "+00:00")):
        raise ValueError("Management link expired")
    return row


def _deactivate_fitbit_token(spreadsheet: Spreadsheet, watch_name: str) -> None:
    ws = spreadsheet.get_gspread_connection().worksheet("fitbit_oauth_tokens")
    headers = [str(item or "").strip() for item in ws.row_values(1)]
    for required in ("status", "revoked_at"):
        if required not in headers:
            headers.append(required)
    ws.resize(cols=len(headers))
    ws.update("1:1", [headers])
    matches = [
        (index, row)
        for index, row in enumerate(ws.get_all_records(), start=2)
        if str(row.get("watchName") or "") == watch_name
    ]
    if matches:
        row_number, _ = matches[-1]
        for field, value in {
            "status": "disconnected",
            "revoked_at": utc_now_iso(),
            "access_token": "",
            "refresh_token": "",
        }.items():
            ws.update_cell(row_number, headers.index(field) + 1, value)


def _clear_legacy_fitbit_tokens(spreadsheet: Spreadsheet, watch_name: str) -> list[str]:
    """Remove every legacy plaintext/ref token for a disconnected watch."""
    warnings: list[str] = []
    ws = spreadsheet.get_gspread_connection().worksheet("fitbit")
    headers = [str(item or "").strip() for item in ws.row_values(1)]
    if "name" not in headers:
        return warnings
    records = ws.get_all_records()
    for row_number, row in enumerate(records, start=2):
        if str(row.get("name") or "") != watch_name:
            continue
        for field in ("token", "token_secret_ref"):
            if field in headers:
                ws.update_cell(row_number, headers.index(field) + 1, "")
        secret_ref = str(row.get("token_secret_ref") or "")
        if secret_ref:
            try:
                destroy_secret_versions(secret_ref)
            except Exception:
                warnings.append("A legacy Secret Manager version could not be destroyed")
    return warnings


def disconnect_connection(spreadsheet: Spreadsheet, management_row: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    provider = str(management_row.get("provider") or "")
    watch_name = str(management_row.get("watchName") or "")
    project = str(management_row.get("project") or "")
    if str(management_row.get("revoked_at") or ""):
        return warnings

    if provider == "google_health":
        token_row = get_active_token_row(spreadsheet, watchName=watch_name, provider=provider)
        if token_row:
            from utils.google_health_oauth import revoke_google_health_token

            token = str(token_row.get("refresh_token") or token_row.get("access_token") or "")
            if token:
                try:
                    revoke_google_health_token(token)
                except Exception:
                    warnings.append("Google did not confirm remote token revocation")
            _update_row_by_keys(
                spreadsheet,
                "health_oauth_tokens",
                keys={"watchName": watch_name, "provider": provider, "is_active": "TRUE"},
                updates={
                    "is_active": "FALSE",
                    "auth_status": "disconnected",
                    "access_token": "",
                    "refresh_token": "",
                    "updated_at": utc_now_iso(),
                },
            )
            try:
                destroy_secret_versions(str(token_row.get("token_secret_ref") or ""))
            except Exception:
                warnings.append("A Google Health Secret Manager version could not be destroyed")
    elif provider == "fitbit":
        from utils.fitbit_oauth import revoke_token
        from utils.fitbit_token_store import get_latest_tokens

        token_row = get_latest_tokens(spreadsheet, watch_name)
        if token_row:
            token = str(token_row.get("refresh_token") or token_row.get("access_token") or "")
            if token:
                try:
                    revoke_token(token)
                except Exception:
                    warnings.append("Fitbit did not confirm remote token revocation")
            _deactivate_fitbit_token(spreadsheet, watch_name)
            try:
                destroy_secret_versions(str(token_row.get("token_secret_ref") or ""))
            except Exception:
                warnings.append("A Fitbit Secret Manager version could not be destroyed")
        warnings.extend(_clear_legacy_fitbit_tokens(spreadsheet, watch_name))
    else:
        raise ValueError("Unsupported provider")

    update_fitbit_registry_auth_status(
        spreadsheet,
        watchName=watch_name,
        project=project,
        auth_status="disconnected",
    )
    _update_row_by_keys(
        spreadsheet,
        MANAGEMENT_TAB,
        keys={"management_token_hash": management_row["management_token_hash"]},
        updates={"revoked_at": utc_now_iso()},
    )
    return warnings


def record_deletion_request(spreadsheet: Spreadsheet, management_row: dict[str, Any], language: str) -> None:
    if str(management_row.get("deletion_requested_at") or ""):
        return
    requested = utc_now_iso()
    _append(
        spreadsheet,
        DELETION_REQUESTS_TAB,
        DELETION_COLUMNS,
        {
            "request_id": new_uuid(),
            "management_token_hash": management_row["management_token_hash"],
            "watchName": management_row.get("watchName", ""),
            "project": management_row.get("project", ""),
            "provider": management_row.get("provider", ""),
            "requested_at": requested,
            "status": "open",
            "approved_policy_text": deletion_text(language),
        },
    )
    _update_row_by_keys(
        spreadsheet,
        MANAGEMENT_TAB,
        keys={"management_token_hash": management_row["management_token_hash"]},
        updates={"deletion_requested_at": requested},
    )
