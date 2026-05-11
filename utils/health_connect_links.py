from __future__ import annotations

from uuid import uuid4

from entity.Sheet import Spreadsheet
from utils.fitbit_oauth import build_authorize_url as build_fitbit_authorize_url
from utils.fitbit_token_store import save_state as save_fitbit_state
from utils.google_health_oauth import build_google_health_authorize_url
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import save_oauth_state


def create_health_connect_link(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    oauth_client_key: str = "",
    purpose: str = "connect",
    created_by: str | None = None,
) -> str:
    provider = provider or "fitbit"
    state = str(uuid4())

    if provider == "fitbit":
        save_fitbit_state(spreadsheet, state=state, watch_name=watchName, project=project)
        return build_fitbit_authorize_url(state)

    if provider == "google_health":
        if not oauth_client_key:
            raise ValueError("oauth_client_key is required for Google Health links")
        save_oauth_state(
            spreadsheet,
            state=state,
            provider=provider,
            watchName=watchName,
            project=project,
            oauth_client_key=oauth_client_key,
            purpose=purpose,
            created_by=created_by,
        )
        cfg = get_oauth_client_config(spreadsheet, oauth_client_key)
        return build_google_health_authorize_url(
            client_id=cfg.client_id,
            redirect_uri=cfg.redirect_uri,
            scopes=cfg.scopes,
            state=state,
            auth_uri=cfg.auth_uri,
        )

    raise ValueError(f"Unsupported provider: {provider}")
