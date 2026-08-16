from __future__ import annotations

import streamlit as st

from entity.Sheet import Spreadsheet
from utils.fitbit_oauth import exchange_code_for_tokens
from utils.fitbit_token_store import (
    is_state_used,
    mark_state_used,
    resolve_state,
    save_tokens_for_watch,
)
from utils.rate_limit_ui import show_rate_limit_notice


def _get_callback_spreadsheet(auth_controller=None) -> Spreadsheet | None:
    if st.session_state.get("spreadsheet") is not None:
        return st.session_state.spreadsheet

    if auth_controller is not None:
        spreadsheet = auth_controller.get_spreadsheet()
        if spreadsheet is not None:
            return spreadsheet

    spreadsheet_key = st.secrets.get("spreadsheet_key", "")
    if not spreadsheet_key:
        return None
    return Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)


def handle_fitbit_callback(auth_controller=None) -> bool:
    """
    Handle Fitbit OAuth callbacks without requiring a logged-in lab user.
    Returns True when the current request was handled and page rendering should stop.
    """
    qp = st.query_params
    code = qp.get("code")
    state = qp.get("state")

    if qp.get("fitbit_callback") != "1":
        if not code or not state:
            return False

        sp_probe = _get_callback_spreadsheet(auth_controller)
        if sp_probe is None:
            return False
        try:
            if resolve_state(sp_probe, state) is None:
                return False
        except Exception:
            return False

    if not code or not state:
        st.error("Fitbit OAuth callback missing code/state")
        return True

    sp = _get_callback_spreadsheet(auth_controller)
    if sp is None:
        st.error("Could not connect to spreadsheet for Fitbit OAuth callback")
        return True

    if is_state_used(sp, state):
        st.error("This OAuth link was already used. Please generate a new one.")
        return True

    state_row = resolve_state(sp, state)
    if not state_row:
        st.error("Unknown state. Please generate a new connect link from the lab app.")
        return True

    watch_name = state_row.get("watchName")
    if not watch_name:
        st.error("State record missing watchName")
        return True

    try:
        token_json = exchange_code_for_tokens(code)
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="fitbit",
            key="fitbit_token_exchange",
            context="connecting to Fitbit",
        ):
            st.error(f"Token exchange failed: {e}")
        return True

    try:
        save_tokens_for_watch(sp, watch_name=watch_name, token_json=token_json)
        mark_state_used(sp, state=state, watch_name=watch_name)
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            provider="google_sheets",
            key="fitbit_token_store",
            context="saving Fitbit OAuth tokens",
        ):
            st.error(f"Failed to store tokens: {e}")
        return True

    st.success(f"Fitbit watch '{watch_name}' connected successfully. You can close this tab.")
    try:
        st.query_params.clear()
    except Exception:
        pass
    return True
