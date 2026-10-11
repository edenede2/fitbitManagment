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
from utils.health_token_store import (
    assert_state_authorized_for_callback,
    mark_oauth_state_used,
    resolve_oauth_state,
    record_oauth_exchange_outcome,
)
from utils.rate_limit_ui import show_rate_limit_notice
from utils.connection_management import create_management_code


def _get_callback_spreadsheet(auth_controller=None) -> Spreadsheet | None:
    # OAuth callbacks intentionally ignore the interactive session data source.
    # A guest session must never redirect callback writes into a demo object, and
    # an authenticated session must not be required for participant callbacks.
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
    if qp.get("fitbit_callback") == "1" and qp.get("error"):
        st.error("Fitbit authorization was cancelled or denied. Request a new link to try again.")
        st.query_params.clear()
        return True

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
    st.query_params.clear()

    sp = _get_callback_spreadsheet(auth_controller)
    if sp is None:
        st.error("Could not connect to spreadsheet for Fitbit OAuth callback")
        return True

    generic_state = False
    try:
        state_row = resolve_oauth_state(sp, state=state, provider="fitbit")
        generic_state = True
        assert_state_authorized_for_callback(state_row)
    except ValueError as generic_error:
        # Temporary compatibility path for links generated before the unified,
        # consent-aware state table was deployed.
        if "Unknown OAuth state" not in str(generic_error):
            st.error("This authorization link is invalid or no longer current. Please request a new link.")
            return True
        if is_state_used(sp, state):
            st.error("This OAuth link was already used. Please generate a new one.")
            return True
        state_row = resolve_state(sp, state)
        if not state_row:
            st.error("Unknown state. Please generate a new connect link from the lab app.")
            return True
        from utils.compliance import participant_disclosure_enforced

        if participant_disclosure_enforced():
            st.error("This legacy link has no approved disclosure record. Please request a new link.")
            return True

    watch_name = state_row.get("watchName")
    if not watch_name:
        st.error("State record missing watchName")
        return True

    try:
        # Consume a validated state before contacting Fitbit. Failed exchanges
        # require a fresh link and cannot leave a replayable callback state.
        if generic_state:
            mark_oauth_state_used(sp, state_row=state_row, code=code)
        else:
            mark_state_used(sp, state=state, watch_name=watch_name)
    except Exception as e:
        if not show_rate_limit_notice(
            e,
            key="fitbit_state_consumption",
            context="validating the Fitbit OAuth link",
        ):
            st.error("The authorization link could not be validated. Please request a new link.")
        return True

    try:
        token_json = exchange_code_for_tokens(code)
        from utils.fitbit_oauth import _cfg, revoke_token
        from utils.oauth_permissions import require_granted_scopes
        try:
            require_granted_scopes(token_json.get("scope"), _cfg()[3].split(), provider="fitbit")
        except ValueError:
            try:
                revoke_token(str(token_json.get("access_token") or ""))
            except Exception:
                pass
            raise
    except Exception as e:
        if generic_state:
            try:
                record_oauth_exchange_outcome(sp, state_row,
                    "partial_grant" if isinstance(e, ValueError) else "exchange_failed_or_uncertain")
            except Exception:
                pass  # A consumed state remains claimed; Admin reconciles it.
        if not show_rate_limit_notice(
            e,
            provider="fitbit",
            key="fitbit_token_exchange",
            context="connecting to Fitbit",
        ):
            st.error("Fitbit authorization could not be completed. Please request a new link.")
        return True

    try:
        save_tokens_for_watch(sp, watch_name=watch_name, token_json=token_json,
            enrollment_id=str(state_row.get("enrollment_id") or ""),
            generation=state_row.get("collection_generation"))
        if generic_state:
            record_oauth_exchange_outcome(sp, state_row, "token_stored")
    except Exception as e:
        if generic_state:
            try:
                record_oauth_exchange_outcome(sp, state_row, "storage_failed_or_uncertain")
            except Exception:
                pass
        if not show_rate_limit_notice(
            e,
            key="fitbit_token_store",
            context="saving Fitbit OAuth tokens",
        ):
            st.error("The connection was authorized but could not be stored securely. Please contact the study team.")
        return True

    management_code = ""
    try:
        management_code = create_management_code(
            sp,
            watch_name=watch_name,
            project=str(state_row.get("project") or ""),
            provider="fitbit",
        )
    except Exception:
        # Authorization and secure token storage are already complete.
        pass
    st.success(f"Fitbit watch '{watch_name}' connected successfully. You can close this tab.")
    if management_code:
        st.info("Save this one-use access code privately. It expires in 24 hours; the team can issue a replacement.")
        st.code(management_code, language=None)
        st.page_link("pages/12_Manage_Connection.py", label="Manage this connection")
    else:
        st.warning("The connection is active, but an access code could not be created. Contact the study team.")
    try:
        st.query_params.clear()
    except Exception:
        pass
    return True
