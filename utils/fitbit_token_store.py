# utils/fitbit_token_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import time
from collections import OrderedDict

import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from utils.fitbit_oauth import refresh_tokens, now_ts
from utils.secret_store import (
    load_json_secret,
    plaintext_secret_fallback_allowed,
    secret_manager_enabled,
    store_json_secret,
)

OAUTH_STATES_TAB = "oauth_states"
OAUTH_USED_TAB = "oauth_state_used"
TOKENS_TAB = "fitbit_oauth_tokens"
FITBIT_SHEET = "fitbit"  # where watchName is registered (and linked to project)

def _append(sp: Spreadsheet, tab: str, row: OrderedDict) -> None:
    # IMPORTANT: append_rows writes values by dict order -> keep OrderedDict aligned with header order
    if not GoogleSheetsAdapter.append_rows(sp, tab, [row]):
        raise RuntimeError(f"Could not persist OAuth data in {tab}")

def _update_fitbit_token(sp: Spreadsheet, watch_name: str, access_token: str) -> None:
    """Directly update the 'token' column in the fitbit sheet for the given watch.
    
    The fitbit sheet uses 'name' as the watch identifier column.
    Uses gspread directly because GoogleSheetsAdapter.update_rows is not suited
    for partial-column updates.
    """
    try:
        updated = GoogleSheetsAdapter.update_matching_rows(
            sp,
            FITBIT_SHEET,
            keys={"name": watch_name},
            updates={"token": access_token},
            latest_only=True,
        )
        if not updated:
            print(f"[fitbit_token_store] Watch '{watch_name}' not found — token not updated.")
    except Exception as e:
        print(f"[fitbit_token_store] Failed to update Fitbit token metadata: {type(e).__name__}")

def save_state(
    sp: Spreadsheet,
    *,
    state: str,
    watch_name: str,
    project: str,
    ttl_seconds: int = 48 * 60 * 60,
) -> None:
    created_at = now_ts()
    row = OrderedDict([
        ("state", state),
        ("watchName", watch_name),
        ("project", project),
        ("created_at", str(created_at)),
        ("expires_at", str(created_at + ttl_seconds)),
    ])
    _append(sp, OAUTH_STATES_TAB, row)

def mark_state_used(sp: Spreadsheet, *, state: str, watch_name: str) -> None:
    row = OrderedDict([
        ("state", state),
        ("used_at", str(now_ts())),
        ("watchName", watch_name),
    ])
    _append(sp, OAUTH_USED_TAB, row)

def is_state_used(sp: Spreadsheet, state: str) -> bool:
    rows = GoogleSheetsAdapter.get_rows(sp, OAUTH_USED_TAB, "state", state=state)
    return len(rows) > 0

def resolve_state(sp: Spreadsheet, state: str) -> Optional[Dict[str, Any]]:
    # Find the latest matching state (append-only)
    rows = GoogleSheetsAdapter.get_rows(sp, OAUTH_STATES_TAB, "state", state=state)
    if not rows:
        return None
    state_row = rows[-1]
    try:
        expires_at = int(state_row.get("expires_at", 0) or 0)
        if not expires_at:
            created_at = int(state_row.get("created_at", 0) or 0)
            expires_at = created_at + (48 * 60 * 60) if created_at else 0
    except (TypeError, ValueError):
        return None

    if not expires_at or now_ts() > expires_at:
        return None
    return state_row

def save_tokens_for_watch(sp: Spreadsheet, *, watch_name: str, token_json: dict) -> None:
    expires_in = int(token_json.get("expires_in", 0) or 0)
    created_at = now_ts()
    expires_at = created_at + max(expires_in - 30, 0)  # 30s safety buffer

    existing = get_latest_tokens(sp, watch_name)
    secret_ref = store_json_secret(
        "participant-oauth",
        ["fitbit", watch_name],
        {
            "access_token": token_json.get("access_token", ""),
            "refresh_token": token_json.get("refresh_token", "")
            or (existing or {}).get("refresh_token", ""),
        },
        existing_ref=str((existing or {}).get("token_secret_ref") or ""),
    )
    protected = secret_manager_enabled()
    row = OrderedDict([
        ("watchName", watch_name),
        ("fitbit_user_id", token_json.get("user_id", "")),
        ("token_secret_ref", secret_ref),
        ("access_token", "" if protected else token_json.get("access_token", "")),
        ("refresh_token", "" if protected else token_json.get("refresh_token", "")),
        ("expires_at", str(expires_at)),
        ("scope", token_json.get("scope", "")),
        ("created_at", str(created_at)),
        ("status", "connected"),
        ("revoked_at", ""),
    ])
    _append(sp, TOKENS_TAB, row)
    if not protected:
        _update_fitbit_token(sp, watch_name, token_json.get("access_token", ""))

def get_latest_tokens(sp: Spreadsheet, watch_name: str) -> Optional[Dict[str, Any]]:
    if sp is None:
        raise ValueError("Spreadsheet connection unavailable")

    rows = GoogleSheetsAdapter.get_rows(sp, TOKENS_TAB, "watchName", watchName=watch_name)
    if not rows:
        return None
    active = [row for row in rows if str(row.get("status") or "connected").casefold() == "connected"]
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
        raise RuntimeError("Plaintext Fitbit token fallback is disabled")
    return row

def get_legacy_fitbit_token(sp: Spreadsheet, watch_name: str) -> str:
    """Return the static token from the legacy fitbit sheet, if one exists."""
    if sp is None:
        raise ValueError("Spreadsheet connection unavailable")

    rows = GoogleSheetsAdapter.get_rows(sp, FITBIT_SHEET, "name", name=watch_name)
    if not rows:
        raise ValueError(f"No fitbit row found for watch '{watch_name}'")

    row = rows[-1]
    token = row.get("token", "")
    secret_ref = str(row.get("token_secret_ref") or "").strip()
    if secret_ref:
        try:
            token = load_json_secret(secret_ref).get("access_token", "")
        except Exception:
            if not plaintext_secret_fallback_allowed() or not token:
                raise
    if token and not plaintext_secret_fallback_allowed():
        if not secret_ref:
            raise RuntimeError("Legacy plaintext Fitbit token fallback is disabled")
    if not token:
        raise ValueError(f"No legacy token found for watch '{watch_name}'")

    return token

def get_valid_access_token(sp: Spreadsheet, watch_name: str, *, force_refresh: bool = False) -> str:
    """
    Returns a valid access token for this watch.
    If expired, or force_refresh=True, refresh and store new token row, then return new token.
    """
    tok = get_latest_tokens(sp, watch_name)
    if not tok:
        raise ValueError(f"No OAuth tokens found for watch '{watch_name}'")

    access_token = tok.get("access_token", "")
    refresh_token_val = tok.get("refresh_token", "")
    try:
        expires_at = int(tok.get("expires_at", 0) or 0)
    except (TypeError, ValueError):
        expires_at = 0

    if not access_token or not refresh_token_val:
        raise ValueError(f"Tokens incomplete for watch '{watch_name}'")

    if not force_refresh and now_ts() < expires_at:
        return access_token

    # Refresh
    new_tok = refresh_tokens(refresh_token_val)
    save_tokens_for_watch(sp, watch_name=watch_name, token_json=new_tok)
    return new_tok.get("access_token", "")
