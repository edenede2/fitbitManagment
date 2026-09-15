from __future__ import annotations

from uuid import uuid4

from entity.Sheet import Spreadsheet
from utils.fitbit_oauth import build_authorize_url as build_fitbit_authorize_url
from utils.google_health_oauth import build_google_health_authorize_url
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import save_oauth_state
from utils.compliance import (
    assert_disclosure_can_be_enforced,
    participant_authorization_url,
    participant_disclosure_enforced,
)


def build_provider_authorize_url(
    spreadsheet: Spreadsheet,
    *,
    state_row: dict,
) -> str:
    provider = str(state_row.get("provider") or "")
    if provider == "fitbit":
        return build_fitbit_authorize_url(str(state_row["state"]))
    if provider == "google_health":
        oauth_client_key = str(state_row.get("oauth_client_key") or "")
        cfg = get_oauth_client_config(spreadsheet, oauth_client_key)
        return build_google_health_authorize_url(
            client_id=cfg.client_id,
            redirect_uri=cfg.redirect_uri,
            scopes=cfg.scopes,
            state=str(state_row["state"]),
            auth_uri=cfg.auth_uri,
        )
    raise ValueError(f"Unsupported provider: {provider}")


def create_health_connect_link(
    spreadsheet: Spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    oauth_client_key: str = "",
    purpose: str = "connect",
    created_by: str | None = None,
    staff_consent_verified: bool = False,
    adult_verified: bool = False,
) -> str:
    provider = provider or "fitbit"
    state = str(uuid4())

    if provider not in {"fitbit", "google_health"}:
        raise ValueError(f"Unsupported provider: {provider}")
    if provider == "google_health" and not oauth_client_key:
        raise ValueError("oauth_client_key is required for Google Health links")
    if participant_disclosure_enforced():
        assert_disclosure_can_be_enforced()
        if not staff_consent_verified or not adult_verified:
            raise ValueError(
                "Staff must verify approved consent and adult eligibility before generating a link"
            )

    save_oauth_state(
        spreadsheet,
        state=state,
        provider=provider,
        watchName=watchName,
        project=project,
        oauth_client_key=oauth_client_key,
        purpose=purpose,
        created_by=created_by,
        staff_consent_verified=staff_consent_verified,
        adult_verified=adult_verified,
    )
    state_row = {
        "state": state,
        "provider": provider,
        "oauth_client_key": oauth_client_key,
    }
    if participant_disclosure_enforced():
        return participant_authorization_url(state)
    return build_provider_authorize_url(spreadsheet, state_row=state_row)
