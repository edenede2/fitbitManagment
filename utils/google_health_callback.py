from __future__ import annotations

import streamlit as st

from entity.Sheet import Spreadsheet
from utils.google_health_oauth import (
    exchange_code_for_google_health_tokens,
    get_google_health_identity,
)
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import (
    log_health_api_event,
    mark_oauth_state_used,
    resolve_oauth_state,
    save_google_health_tokens_for_watch,
)
from utils.rate_limit_ui import show_rate_limit_notice


def _get_callback_spreadsheet() -> Spreadsheet | None:
    if st.session_state.get("spreadsheet") is not None:
        return st.session_state.spreadsheet

    spreadsheet_key = st.secrets.get("spreadsheet_key", "")
    if not spreadsheet_key:
        return None

    # Do not call GoogleSheetsAdapter.connect here. OAuth callbacks only need
    # targeted reads/writes, and a full connect can exhaust Sheets read quota.
    return Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)


def handle_google_health_callback(auth_controller) -> bool:
    """
    Handles Google Health OAuth callbacks without requiring admin login.
    Returns True if the current request was handled as a Google Health callback.
    """
    qp = st.query_params
    if qp.get("google_health_callback") != "1":
        return False

    state = qp.get("state")
    code = qp.get("code")

    error = qp.get("error")
    if error:
        st.error(f"Google Health OAuth failed: {error}")
        return True
    if not code or not state:
        st.error("Google Health OAuth callback missing code/state")
        return True

    sp = _get_callback_spreadsheet()
    if sp is None:
        st.error("Could not connect to spreadsheet for Google Health OAuth callback")
        return True

    watch_name = ""
    project = ""
    try:
        state_row = resolve_oauth_state(sp, state=state, provider="google_health")
        oauth_client_key = state_row["oauth_client_key"]
        watch_name = state_row["watchName"]
        project = state_row["project"]

        cfg = get_oauth_client_config(sp, oauth_client_key)
        token_data = exchange_code_for_google_health_tokens(
            code=code,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            redirect_uri=cfg.redirect_uri,
            token_uri=cfg.token_uri,
        )

        identity = None
        identity_error = ""
        try:
            identity = get_google_health_identity(token_data["access_token"])
        except Exception as exc:
            identity_error = str(exc)

        save_google_health_tokens_for_watch(
            sp,
            watchName=watch_name,
            project=project,
            oauth_client_key=oauth_client_key,
            token_data=token_data,
            identity=identity,
        )
        mark_oauth_state_used(sp, state_row=state_row, code=code)

        log_health_api_event(
            sp,
            provider="google_health",
            watchName=watch_name,
            project=project,
            operation="oauth_callback",
            status="warning" if identity_error else "success",
            message="Google Health OAuth completed" if not identity_error else "OAuth tokens saved; identity lookup failed",
            error=identity_error,
        )
    except Exception as exc:
        try:
            log_health_api_event(
                sp,
                provider="google_health",
                watchName=watch_name,
                project=project,
                operation="oauth_callback",
                status="error",
                error=str(exc),
            )
        except Exception:
            pass
        if not show_rate_limit_notice(
            exc,
            key="google_health_callback",
            context="finishing Google Health connection",
        ):
            st.error(f"Google Health OAuth callback failed: {exc}")
        return True

    st.success(f"Google Health connected successfully for watch '{watch_name}'. You can close this tab.")
    try:
        st.query_params.clear()
    except Exception:
        pass
    return True
