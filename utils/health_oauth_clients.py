from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List
from urllib.parse import urlsplit, urlunsplit

import gspread

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet


CLIENTS_TAB = "health_oauth_clients"
CLIENT_COLUMNS = [
    "client_key", "provider", "enviroment", "client_id", "client_secret",
    "credentials_json_raw", "redirect_uri", "auth_uri", "token_uri", "scopes",
    "status", "created_at", "updated_at", "notes",
]
DEFAULT_GOOGLE_HEALTH_SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
]


@dataclass
class OAuthClientConfig:
    client_key: str
    provider: str
    environment: str
    client_id: str
    client_secret: str
    redirect_uri: str
    auth_uri: str
    token_uri: str
    scopes: List[str]
    credentials_json_raw: str | None = None


def parse_scopes(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(scope).strip() for scope in value if str(scope).strip()]
    return [scope.strip() for scope in str(value or "").split() if scope.strip()]


def scopes_to_string(scopes: str | list[str]) -> str:
    if isinstance(scopes, str):
        return " ".join(parse_scopes(scopes))
    return " ".join(parse_scopes(scopes))


def _first_present(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_active_oauth_clients(
    spreadsheet: Spreadsheet,
    *,
    provider: str = "google_health",
) -> list[dict[str, Any]]:
    rows = GoogleSheetsAdapter.get_rows(spreadsheet, CLIENTS_TAB, "provider", provider=provider)
    return [
        row for row in rows
        if str(row.get("status", "active") or "active").lower() == "active"
    ]


def parse_google_oauth_client_json(raw_json: str) -> dict[str, Any]:
    parsed = json.loads(raw_json)
    oauth_config = parsed.get("web") or parsed.get("installed") or {}
    if not oauth_config:
        raise ValueError("OAuth client JSON must contain a 'web' or 'installed' object")

    client_id = oauth_config.get("client_id", "")
    client_secret = oauth_config.get("client_secret", "")
    if not client_id or not client_secret:
        raise ValueError("OAuth client JSON is missing client_id/client_secret")

    redirect_uris = oauth_config.get("redirect_uris") or []
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": oauth_config.get("auth_uri") or "https://accounts.google.com/o/oauth2/v2/auth",
        "token_uri": oauth_config.get("token_uri") or "https://oauth2.googleapis.com/token",
        "redirect_uris": redirect_uris,
        "project_id": oauth_config.get("project_id", ""),
    }


def make_default_client_key(parsed_json: dict[str, Any], enviroment: str) -> str:
    project_id = str(parsed_json.get("project_id") or "").strip()
    if project_id:
        return f"google_health_{enviroment}_{project_id}".replace("-", "_")
    client_id = str(parsed_json.get("client_id") or "").split(".apps.googleusercontent.com")[0]
    suffix = client_id[-8:] if client_id else "client"
    return f"google_health_{enviroment}_{suffix}"


def is_streamlit_auth_callback_uri(redirect_uri: str) -> bool:
    return urlsplit(str(redirect_uri or "")).path.rstrip("/") == "/oauth2callback"


def suggest_google_health_redirect_uri(redirect_uri: str) -> str:
    """
    Convert a Streamlit login redirect URI into an app-level health callback URI.
    Google Cloud must list this exact URI under Authorized redirect URIs.
    """
    parsed = urlsplit(str(redirect_uri or ""))
    if not parsed.scheme or not parsed.netloc:
        return str(redirect_uri or "")
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "google_health_callback=1", ""))


def _worksheet(spreadsheet: Spreadsheet, tab: str):
    return spreadsheet.get_gspread_connection().worksheet(tab)


def _find_row_by_client_key(worksheet, client_key: str) -> tuple[int | None, list[str]]:
    headers = worksheet.row_values(1)
    if not headers:
        return None, []
    records = worksheet.get_all_records()
    for offset, record in enumerate(records, start=2):
        if str(record.get("client_key", "")).strip() == client_key:
            return offset, headers
    return None, headers


def _ordered_client_row(values: dict[str, Any]) -> dict[str, Any]:
    return {column: values.get(column, "") for column in CLIENT_COLUMNS}


def upsert_oauth_client_config(
    spreadsheet: Spreadsheet,
    *,
    client_key: str,
    provider: str,
    enviroment: str,
    client_id: str,
    client_secret: str,
    credentials_json_raw: str,
    redirect_uri: str,
    auth_uri: str,
    token_uri: str,
    scopes: str | list[str],
    status: str = "active",
    notes: str = "",
) -> None:
    if is_streamlit_auth_callback_uri(redirect_uri):
        suggested = suggest_google_health_redirect_uri(redirect_uri)
        raise ValueError(
            "Do not use Streamlit's /oauth2callback redirect URI for Google Health OAuth. "
            f"Use {suggested} and add it to Google Cloud Authorized redirect URIs."
        )

    now = utc_now_iso()
    values = _ordered_client_row({
        "client_key": client_key,
        "provider": provider,
        "enviroment": enviroment,
        "client_id": client_id,
        "client_secret": client_secret,
        "credentials_json_raw": credentials_json_raw,
        "redirect_uri": redirect_uri,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "scopes": scopes_to_string(scopes),
        "status": status,
        "created_at": now,
        "updated_at": now,
        "notes": notes,
    })
    # Support both the current sheet typo and the correctly spelled variant.
    values["environment"] = enviroment

    try:
        ws = _worksheet(spreadsheet, CLIENTS_TAB)
        row_number, headers = _find_row_by_client_key(ws, client_key)
    except gspread.exceptions.WorksheetNotFound:
        workbook = spreadsheet.get_gspread_connection()
        ws = workbook.add_worksheet(title=CLIENTS_TAB, rows=1000, cols=len(CLIENT_COLUMNS))
        ws.append_row(CLIENT_COLUMNS)
        ws.append_row([values.get(header, "") for header in CLIENT_COLUMNS])
        return

    if not row_number:
        headers = headers or CLIENT_COLUMNS
        missing_headers = [column for column in CLIENT_COLUMNS if column not in headers]
        if missing_headers:
            headers = headers + missing_headers
            ws.resize(cols=len(headers))
            ws.update("1:1", [headers])
        ws.append_row([values.get(header, "") for header in headers])
        return

    if "created_at" in headers:
        existing_created_at = ws.cell(row_number, headers.index("created_at") + 1).value
        if existing_created_at:
            values["created_at"] = existing_created_at

    for key, value in values.items():
        if key in headers:
            ws.update_cell(row_number, headers.index(key) + 1, "" if value is None else str(value))


def get_oauth_client_config(spreadsheet: Spreadsheet, client_key: str) -> OAuthClientConfig:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        CLIENTS_TAB,
        "client_key",
        client_key=client_key,
    )
    matches = [
        row for row in rows
        if str(row.get("status", "active") or "active").lower() == "active"
    ]

    if not matches:
        raise ValueError(f"No active OAuth client config found for client_key={client_key}")

    row = matches[-1]
    raw = str(row.get("credentials_json_raw") or "").strip() or None

    client_id = _first_present(row, "client_id")
    client_secret = _first_present(row, "client_secret")
    auth_uri = _first_present(row, "auth_uri") or "https://accounts.google.com/o/oauth2/v2/auth"
    token_uri = _first_present(row, "token_uri") or "https://oauth2.googleapis.com/token"

    if raw:
        parsed = json.loads(raw)
        oauth_config = parsed.get("web") or parsed.get("installed") or {}
        client_id = client_id or oauth_config.get("client_id", "")
        client_secret = client_secret or oauth_config.get("client_secret", "")
        auth_uri = auth_uri or oauth_config.get("auth_uri", "")
        token_uri = token_uri or oauth_config.get("token_uri", "")

    scopes = parse_scopes(row.get("scopes", ""))
    if not client_id or not client_secret:
        raise ValueError(f"OAuth client {client_key} is missing client_id/client_secret")
    if not row.get("redirect_uri"):
        raise ValueError(f"OAuth client {client_key} is missing redirect_uri")
    if is_streamlit_auth_callback_uri(str(row.get("redirect_uri") or "")):
        suggested = suggest_google_health_redirect_uri(str(row.get("redirect_uri") or ""))
        raise ValueError(
            f"OAuth client {client_key} uses Streamlit's /oauth2callback redirect URI. "
            f"Use {suggested} for Google Health OAuth."
        )
    if not scopes:
        raise ValueError(f"OAuth client {client_key} is missing scopes")

    return OAuthClientConfig(
        client_key=str(row["client_key"]),
        provider=str(row.get("provider") or "google_health"),
        environment=str(row.get("environment") or row.get("enviroment") or "staging"),
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=str(row["redirect_uri"]),
        auth_uri=auth_uri,
        token_uri=token_uri,
        scopes=scopes,
        credentials_json_raw=raw,
    )
