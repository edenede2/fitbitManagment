# utils/fitbit_token_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import time
from collections import OrderedDict

import streamlit as st
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from utils.fitbit_oauth import refresh_tokens, now_ts

OAUTH_STATES_TAB = "oauth_states"
OAUTH_USED_TAB = "oauth_state_used"
TOKENS_TAB = "fitbit_oauth_tokens"
FITBIT_SHEET = "fitbit"  # where watchName is registered (and linked to project)

def _append(sp: Spreadsheet, tab: str, row: OrderedDict) -> None:
    # IMPORTANT: append_rows writes values by dict order -> keep OrderedDict aligned with header order
    GoogleSheetsAdapter.append_rows(sp, tab, [row])

def _update(sp: Spreadsheet, tab: str, row_id_col: str, row_id_val: str, updates: Dict[str, Any]) -> None:
    # IMPORTANT: update_rows writes values by dict order -> keep OrderedDict aligned with header order
    GoogleSheetsAdapter.update_rows(sp, tab, row_id_col, row_id_val, updates)

def save_state(sp: Spreadsheet, *, state: str, watch_name: str, project: str) -> None:
    row = OrderedDict([
        ("state", state),
        ("watchName", watch_name),
        ("project", project),
        ("created_at", str(now_ts())),
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
    return rows[-1]

def save_tokens_for_watch(sp: Spreadsheet, *, watch_name: str, token_json: dict) -> None:
    expires_in = int(token_json.get("expires_in", 0) or 0)
    expires_at = now_ts() + max(expires_in - 30, 0)  # 30s safety buffer

    row = OrderedDict([
        ("watchName", watch_name),
        ("fitbit_user_id", token_json.get("user_id", "")),
        ("access_token", token_json.get("access_token", "")),
        ("refresh_token", token_json.get("refresh_token", "")),
        ("expires_at", str(expires_at)),
        ("scope", token_json.get("scope", "")),
        ("created_at", str(now_ts())),
    ])
    _append(sp, TOKENS_TAB, row)
    _update(sp, FITBIT_SHEET, "watchName", watch_name, {"token": token_json.get("access_token", "")})

def get_latest_tokens(sp: Spreadsheet, watch_name: str) -> Optional[Dict[str, Any]]:
    rows = GoogleSheetsAdapter.get_rows(sp, TOKENS_TAB, "watchName", watchName=watch_name)
    if not rows:
        return None
    return rows[-1]

def get_valid_access_token(sp: Spreadsheet, watch_name: str) -> str:
    """
    Returns a valid access token for this watch.
    If expired -> refresh and store new token row, then return new token.
    """
    tok = get_latest_tokens(sp, watch_name)
    if not tok:
        raise ValueError(f"No OAuth tokens found for watch '{watch_name}'")

    access_token = tok.get("access_token", "")
    refresh_token_val = tok.get("refresh_token", "")
    expires_at = int(tok.get("expires_at", 0) or 0)

    if not access_token or not refresh_token_val:
        raise ValueError(f"Tokens incomplete for watch '{watch_name}'")

    if now_ts() < expires_at:
        return access_token

    # Refresh
    new_tok = refresh_tokens(refresh_token_val)
    save_tokens_for_watch(sp, watch_name=watch_name, token_json=new_tok)
    return new_tok.get("access_token", "")