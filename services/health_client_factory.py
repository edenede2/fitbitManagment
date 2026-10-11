from __future__ import annotations

from typing import Any

from services.google_health_client import GoogleHealthClient
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import get_valid_access_token


class HealthClientFactory:
    @staticmethod
    def from_watch_row(spreadsheet, row: dict[str, Any]):
        watch_name = row.get("name") or row.get("watchName")
        provider = (
            str(row.get("oauth_type") or row.get("provider") or "fitbit")
            .strip()
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
        ) or "fitbit"

        if provider in {"fitbit", "fitbit_web_api", "fitbit_api"}:
            return None

        if provider in {"google", "google_health", "google_health_api", "health"}:
            from utils.collection_control import assert_collection_allowed
            provider = "google_health"
            oauth_client_key = row.get("oauth_client_key") or "google_health_staging"
            cfg = get_oauth_client_config(spreadsheet, oauth_client_key)
            cached_access_token: str | None = None
            generation = assert_collection_allowed(watch_name, provider,
                project=str(row.get("project") or ""), enrollment_id=str(row.get("enrollment_id") or ""),
                generation=row.get("collection_generation")).get("generation", 0)

            def collection_guard():
                return assert_collection_allowed(watch_name, provider,
                    project=str(row.get("project") or ""), generation=generation)

            def access_token_provider() -> str:
                # A dashboard refresh performs several Google Health requests.
                # Resolve/refresh the participant token once for this client
                # instead of rereading operational storage for every endpoint.
                nonlocal cached_access_token
                collection_guard()
                if cached_access_token is None:
                    cached_access_token = get_valid_access_token(
                        spreadsheet,
                        watchName=watch_name,
                        provider=provider,
                        oauth_client_config=cfg,
                    )
                return cached_access_token

            return GoogleHealthClient(access_token_provider=access_token_provider, collection_guard=collection_guard)

        raise ValueError(f"Unsupported health provider: {provider}")
