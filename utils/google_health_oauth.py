from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlencode

import requests


GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_HEALTH_BASE_URL = "https://health.googleapis.com/v4"
GOOGLE_REVOKE_URI = "https://oauth2.googleapis.com/revoke"


def _provider_error(action: str, response) -> RuntimeError:
    # Provider bodies can echo codes, tokens, client credentials, or account data.
    # Keep details in provider-side logs and expose only the HTTP status locally.
    return RuntimeError(f"Google {action} failed with HTTP {response.status_code}")


def now_ts() -> int:
    return int(time.time())


def build_google_health_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    state: str,
    auth_uri: str = GOOGLE_AUTH_URI,
    prompt: str = "consent",
) -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        "state": state,
        "access_type": "offline",
        "prompt": prompt,
    }
    return f"{auth_uri}?{urlencode(params)}"


def normalize_google_token_response(token_response: dict[str, Any]) -> dict[str, Any]:
    now = now_ts()
    expires_in = int(token_response.get("expires_in", 3599) or 3599)
    refresh_expires_in = token_response.get("refresh_token_expires_in")
    refresh_expires_at = None
    if refresh_expires_in not in (None, ""):
        refresh_expires_at = now + int(refresh_expires_in)

    return {
        "access_token": token_response.get("access_token"),
        "refresh_token": token_response.get("refresh_token"),
        "access_expires_at": now + expires_in,
        "refresh_expires_at": refresh_expires_at,
        "refresh_token_expires_in": refresh_expires_in,
        "scope": token_response.get("scope"),
        "token_type": token_response.get("token_type", "Bearer"),
    }


def exchange_code_for_google_health_tokens(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    token_uri: str = GOOGLE_TOKEN_URI,
) -> dict[str, Any]:
    response = requests.post(
        token_uri,
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    if not response.ok:
        raise _provider_error("token exchange", response)
    return normalize_google_token_response(response.json())


def refresh_google_health_tokens(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    token_uri: str = GOOGLE_TOKEN_URI,
) -> dict[str, Any]:
    response = requests.post(
        token_uri,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if not response.ok:
        raise _provider_error("token refresh", response)

    normalized = normalize_google_token_response(response.json())
    if not normalized.get("refresh_token"):
        normalized["refresh_token"] = refresh_token
    return normalized


def get_google_health_identity(access_token: str) -> dict[str, Any]:
    response = requests.get(
        f"{GOOGLE_HEALTH_BASE_URL}/users/me/identity",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    if not response.ok:
        raise _provider_error("Health identity lookup", response)
    return response.json()


def revoke_google_health_token(token: str, revoke_uri: str = GOOGLE_REVOKE_URI) -> None:
    response = requests.post(
        revoke_uri,
        data={"token": token},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if not response.ok:
        raise _provider_error("token revocation", response)
