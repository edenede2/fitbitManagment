from __future__ import annotations

from typing import Any

from services.google_health_client import GoogleHealthClient
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import get_valid_access_token


class HealthClientFactory:
    @staticmethod
    def from_watch_row(spreadsheet, row: dict[str, Any]):
        watch_name = row.get("name") or row.get("watchName")
        provider = str(row.get("oauth_type") or row.get("provider") or "fitbit").strip() or "fitbit"

        if provider == "fitbit":
            return None

        if provider == "google_health":
            oauth_client_key = row.get("oauth_client_key") or "google_health_staging"
            cfg = get_oauth_client_config(spreadsheet, oauth_client_key)

            def access_token_provider() -> str:
                return get_valid_access_token(
                    spreadsheet,
                    watchName=watch_name,
                    provider=provider,
                    oauth_client_config=cfg,
                )

            return GoogleHealthClient(access_token_provider=access_token_provider)

        raise ValueError(f"Unsupported health provider: {provider}")
