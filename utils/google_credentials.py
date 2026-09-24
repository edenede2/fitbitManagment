"""Shared Google service-account credential loading.

Streamlit Community Cloud supplies the service-account document through
``[gcp_service_account]`` in its secrets configuration.  Heroku may instead
provide the same document as one base64-encoded environment variable.
"""

from __future__ import annotations

import base64
import json
import os
from functools import lru_cache
from typing import Any, Iterable

from google.oauth2.service_account import Credentials


BOOTSTRAP_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON_B64"


def _decode_bootstrap(value: str) -> dict[str, Any]:
    try:
        decoded = base64.b64decode(value.encode("ascii"), validate=True).decode("utf-8")
        document = json.loads(decoded)
    except Exception as exc:
        raise ValueError(f"{BOOTSTRAP_ENV} must be base64-encoded service-account JSON") from exc
    if not isinstance(document, dict) or document.get("type") != "service_account":
        raise ValueError(f"{BOOTSTRAP_ENV} does not contain a service-account document")
    return document


def _secrets_mapping() -> Any:
    from model.config import get_secrets

    return get_secrets()


@lru_cache(maxsize=1)
def get_service_account_info() -> dict[str, Any]:
    encoded = os.getenv(BOOTSTRAP_ENV, "").strip()
    if encoded:
        return _decode_bootstrap(encoded)

    try:
        service = _secrets_mapping().get("gcp_service_account", {})
    except Exception as exc:
        raise RuntimeError(
            f"Google credentials are unavailable; configure {BOOTSTRAP_ENV} "
            "or [gcp_service_account] in Streamlit secrets"
        ) from exc

    if not isinstance(service, dict):
        try:
            service = dict(service)
        except Exception as exc:
            raise ValueError("[gcp_service_account] must be a mapping") from exc
    if service.get("type") != "service_account":
        raise ValueError("[gcp_service_account] is not a service-account document")
    return service


def runtime_setting(name: str, default: str = "") -> str:
    """Read a setting from the environment, then Streamlit secrets."""
    configured = os.getenv(name)
    if configured is not None:
        return str(configured).strip()
    try:
        value = _secrets_mapping().get(name, default)
    except Exception:
        value = default
    return str(value).strip()


def google_project_id() -> str:
    configured = runtime_setting("GOOGLE_CLOUD_PROJECT")
    return configured or str(get_service_account_info().get("project_id") or "").strip()


def build_google_credentials(scopes: Iterable[str]) -> Credentials:
    return Credentials.from_service_account_info(
        get_service_account_info(),
        scopes=list(scopes),
    )


def clear_google_credentials_cache() -> None:
    """Clear cached credentials after a configuration/key rotation."""
    get_service_account_info.cache_clear()
