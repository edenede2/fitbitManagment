# utils/fitbit_oauth.py
from __future__ import annotations

from urllib.parse import urlencode
import base64
import os
import uuid
import time
import requests

from model.config import get_secrets

AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"

def _cfg():
    try:
        secrets = get_secrets()
    except Exception:
        secrets = {}
    client_id = (
        os.getenv("FITBIT_CLIENT_ID")
        or secrets.get("FITBIT_CLIENT_ID")
        or secrets.get("fitbit_client_id")
    )
    client_secret = (
        os.getenv("FITBIT_CLIENT_SECRET")
        or secrets.get("FITBIT_CLIENT_SECRET")
        or secrets.get("fitbit_client_secret")
    )
    redirect_uri = (
        os.getenv("FITBIT_REDIRECT_URI")
        or secrets.get("FITBIT_REDIRECT_URI")
        or secrets.get("fitbit_redirect_uri")
    )
    scopes = (
        os.getenv("FITBIT_SCOPES")
        or secrets.get("FITBIT_SCOPES")
        or secrets.get("fitbit_scopes")
        or ""
    ).strip()
    missing = [
        name for name, value in (
            ("FITBIT_CLIENT_ID", client_id),
            ("FITBIT_CLIENT_SECRET", client_secret),
            ("FITBIT_REDIRECT_URI", redirect_uri),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing Fitbit OAuth secrets: {', '.join(missing)}")
    return (
        client_id,
        client_secret,
        redirect_uri,
        scopes,
    )

def new_state() -> str:
    return str(uuid.uuid4())

def build_authorize_url(state: str) -> str:
    client_id, _, redirect_uri, scopes = _cfg()
    if not scopes:
        raise ValueError("Missing FITBIT_SCOPES in Streamlit secrets")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scopes,  # Fitbit expects space-separated scopes
        "state": state,
    }
    return f"{AUTH_URL}?{urlencode(params)}"

def _basic_auth_header(client_id: str, client_secret: str) -> dict:
    b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return {"Authorization": f"Basic {b64}"}

def exchange_code_for_tokens(code: str) -> dict:
    client_id, client_secret, redirect_uri, _ = _cfg()
    headers = _basic_auth_header(client_id, client_secret)

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def refresh_tokens(refresh_token: str) -> dict:
    client_id, client_secret, _, _ = _cfg()
    headers = _basic_auth_header(client_id, client_secret)

    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def now_ts() -> int:
    return int(time.time())
