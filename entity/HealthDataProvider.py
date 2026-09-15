import base64
import datetime
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from utils.secret_store import load_json_secret, plaintext_secret_fallback_allowed

from entity.Watch import ApiAuthError, ApiRateLimitError, Watch, WatchFactory
from services.google_health_client import GoogleHealthClient as SpreadsheetGoogleHealthClient


GOOGLE_PROVIDER_NAMES = {"google", "google_health", "google health", "google_health_api", "health"}
FITBIT_PROVIDER_NAMES = {"fitbit", "fitbit_web_api", "fitbit web api", "fitbit_api"}
RATE_LIMIT_TRACKING_FILE = Path(__file__).resolve().parent.parent / "data" / "rate_limited_watches.json"


@dataclass
class SnapshotContext:
    """Per-run cache for sheet-backed config so collection does not re-read Sheets per watch."""

    spreadsheet: Any
    google_tokens_by_watch: dict[str, dict[str, Any]] = field(default_factory=dict)
    fitbit_tokens_by_watch: dict[str, dict[str, Any]] = field(default_factory=dict)
    oauth_clients_by_key: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self._load_google_tokens()
        self._load_fitbit_tokens()
        self._load_oauth_clients()

    def _sheet_rows(self, sheet_name: str) -> list[dict[str, Any]]:
        if self.spreadsheet is None:
            return []
        try:
            sheet = self.spreadsheet.get_sheet(sheet_name)
            return [clean_row(row) for row in getattr(sheet, "data", []) if isinstance(row, dict)]
        except Exception as exc:
            print(f"Could not preload {sheet_name}: {exc}")
            return []

    def _load_google_tokens(self) -> None:
        for row in self._sheet_rows("health_oauth_tokens"):
            if normalize_provider(row.get("provider")) != "google_health":
                continue
            if str(row.get("is_active", "TRUE")).strip().upper() != "TRUE":
                continue
            watch_name = str(row.get("watchName") or "").strip()
            if watch_name:
                secret_ref = str(row.get("token_secret_ref") or "").strip()
                if secret_ref:
                    try:
                        row.update(load_json_secret(secret_ref))
                    except Exception:
                        if not plaintext_secret_fallback_allowed() or not row.get("access_token"):
                            raise
                self.google_tokens_by_watch[watch_name] = row

    def _load_fitbit_tokens(self) -> None:
        for row in self._sheet_rows("fitbit_oauth_tokens"):
            if str(row.get("status") or "connected").strip().casefold() != "connected":
                continue
            watch_name = str(row.get("watchName") or "").strip()
            if watch_name:
                secret_ref = str(row.get("token_secret_ref") or "").strip()
                if secret_ref:
                    try:
                        row.update(load_json_secret(secret_ref))
                    except Exception:
                        if not plaintext_secret_fallback_allowed() or not row.get("access_token"):
                            raise
                self.fitbit_tokens_by_watch[watch_name] = row

    def get_active_fitbit_token_row(self, watch_name: str) -> dict[str, Any] | None:
        return self.fitbit_tokens_by_watch.get(str(watch_name or "").strip())

    def _load_oauth_clients(self) -> None:
        for row in self._sheet_rows("health_oauth_clients"):
            if normalize_provider(row.get("provider")) != "google_health":
                continue
            if str(row.get("status", "active") or "active").strip().lower() != "active":
                continue
            client_key = str(row.get("client_key") or "").strip()
            if client_key:
                self.oauth_clients_by_key[client_key] = row

    def get_active_google_token_row(self, watch_name: str) -> dict[str, Any] | None:
        return self.google_tokens_by_watch.get(str(watch_name or "").strip())

    def get_oauth_client_config(self, client_key: str):
        client_key = str(client_key or "google_health_staging").strip()
        row = self.oauth_clients_by_key.get(client_key)
        if row:
            return _oauth_client_config_from_row(row, client_key)

        from utils.health_oauth_clients import get_oauth_client_config

        cfg = get_oauth_client_config(self.spreadsheet, client_key)
        self.oauth_clients_by_key[client_key] = {
            "client_key": cfg.client_key,
            "provider": cfg.provider,
            "enviroment": cfg.environment,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "redirect_uri": cfg.redirect_uri,
            "auth_uri": cfg.auth_uri,
            "token_uri": cfg.token_uri,
            "scopes": " ".join(cfg.scopes),
            "credentials_json_raw": cfg.credentials_json_raw or "",
            "status": "active",
        }
        return cfg

    def get_valid_google_access_token(self, row: dict[str, Any]) -> str:
        watch_name = str(row.get("name") or row.get("watchName") or "").strip()
        token_row = self.get_active_google_token_row(watch_name)
        if not token_row:
            raise RuntimeError(f"No active Google Health token found for {watch_name}")

        from utils.health_token_store import is_access_token_valid, update_token_row_after_refresh

        if is_access_token_valid(token_row):
            return str(token_row.get("access_token") or "")

        refresh_token = str(token_row.get("refresh_token") or "")
        if not refresh_token:
            raise RuntimeError(f"Missing Google Health refresh token for {watch_name}")

        oauth_client_key = row.get("oauth_client_key") or token_row.get("oauth_client_key") or "google_health_staging"
        cfg = self.get_oauth_client_config(str(oauth_client_key))

        from utils.google_health_oauth import refresh_google_health_tokens

        token_data = refresh_google_health_tokens(
            refresh_token=refresh_token,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            token_uri=cfg.token_uri,
        )
        update_token_row_after_refresh(self.spreadsheet, token_row, token_data)
        token_row = {**token_row, **{
            "access_token": token_data.get("access_token") or "",
            "refresh_token": token_data.get("refresh_token") or token_row.get("refresh_token", ""),
            "access_expires_at": _token_ts_to_iso(token_data.get("access_expires_at")),
        }}
        self.google_tokens_by_watch[watch_name] = token_row
        return str(token_row.get("access_token") or "")

    def google_health_client(self, row: dict[str, Any]) -> SpreadsheetGoogleHealthClient:
        return SpreadsheetGoogleHealthClient(
            access_token_provider=lambda: self.get_valid_google_access_token(row)
        )

    def get_valid_fitbit_access_token(self, row: dict[str, Any], *, force_refresh: bool = False) -> str:
        watch_name = str(row.get("name") or row.get("watchName") or "").strip()
        token_row = self.fitbit_tokens_by_watch.get(watch_name)

        if token_row:
            from utils.fitbit_oauth import now_ts, refresh_tokens
            from utils.fitbit_token_store import save_tokens_for_watch

            access_token = str(token_row.get("access_token") or "")
            refresh_token = str(token_row.get("refresh_token") or "")
            try:
                expires_at = int(token_row.get("expires_at", 0) or 0)
            except (TypeError, ValueError):
                expires_at = 0

            if access_token and refresh_token and not force_refresh and now_ts() < expires_at:
                return access_token

            if refresh_token:
                try:
                    new_tokens = refresh_tokens(refresh_token)
                    save_tokens_for_watch(self.spreadsheet, watch_name=watch_name, token_json=new_tokens)
                    expires_in = int(new_tokens.get("expires_in", 0) or 0)
                    updated_row = {
                        **token_row,
                        "access_token": new_tokens.get("access_token", ""),
                        "refresh_token": new_tokens.get("refresh_token", refresh_token),
                        "expires_at": str(now_ts() + max(expires_in - 30, 0)),
                        "scope": new_tokens.get("scope", token_row.get("scope", "")),
                        "fitbit_user_id": new_tokens.get("user_id", token_row.get("fitbit_user_id", "")),
                        "created_at": str(now_ts()),
                    }
                    self.fitbit_tokens_by_watch[watch_name] = updated_row
                    return str(updated_row.get("access_token") or "")
                except Exception as exc:
                    if access_token and not force_refresh:
                        print(
                            f"Fitbit token refresh failed for {watch_name}; "
                            f"trying existing OAuth access token before marking failed: {exc}"
                        )
                        return access_token
                    raise

            if access_token:
                return access_token

        token = row.get("token")
        if has_value(token):
            return str(token)
        raise ValueError(f"No Fitbit token available for watch '{watch_name}'")


def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize spreadsheet keys so accidental whitespace in headers does not break lookups."""
    return {str(key).strip(): value for key, value in row.items()}


def has_value(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return text != "" and text.lower() not in {"nan", "none", "null"}


def normalize_provider(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _token_ts_to_iso(ts: int | float | str | None) -> str:
    if not has_value(ts):
        return ""
    try:
        return datetime.datetime.fromtimestamp(float(ts), tz=datetime.timezone.utc).isoformat()
    except (TypeError, ValueError):
        return str(ts)


def _oauth_client_config_from_row(row: dict[str, Any], client_key: str):
    from utils.health_oauth_clients import (
        OAuthClientConfig,
        is_streamlit_auth_callback_uri,
        parse_scopes,
        suggest_google_health_redirect_uri,
    )

    raw = str(row.get("credentials_json_raw") or "").strip() or None
    client_id = str(row.get("client_id") or "").strip()
    client_secret = str(row.get("client_secret") or "").strip()
    auth_uri = str(row.get("auth_uri") or "https://accounts.google.com/o/oauth2/v2/auth").strip()
    token_uri = str(row.get("token_uri") or "https://oauth2.googleapis.com/token").strip()

    secret_ref = str(row.get("client_secret_ref") or "").strip()
    if secret_ref:
        try:
            protected = load_json_secret(secret_ref)
            client_id = client_id or str(protected.get("client_id") or "")
            client_secret = str(protected.get("client_secret") or "")
            auth_uri = str(protected.get("auth_uri") or auth_uri)
            token_uri = str(protected.get("token_uri") or token_uri)
        except Exception:
            if not plaintext_secret_fallback_allowed() or not client_secret:
                raise
    elif not plaintext_secret_fallback_allowed() and (client_secret or raw):
        raise RuntimeError(f"OAuth client {client_key} still uses plaintext secret storage")

    if raw and plaintext_secret_fallback_allowed():
        parsed = json.loads(raw)
        oauth_config = parsed.get("web") or parsed.get("installed") or {}
        client_id = client_id or oauth_config.get("client_id", "")
        client_secret = client_secret or oauth_config.get("client_secret", "")
        auth_uri = auth_uri or oauth_config.get("auth_uri", "")
        token_uri = token_uri or oauth_config.get("token_uri", "")

    scopes = parse_scopes(row.get("scopes", ""))
    redirect_uri = str(row.get("redirect_uri") or "").strip()
    if not client_id or not client_secret:
        raise ValueError(f"OAuth client {client_key} is missing client_id/client_secret")
    if not redirect_uri:
        raise ValueError(f"OAuth client {client_key} is missing redirect_uri")
    if is_streamlit_auth_callback_uri(redirect_uri):
        suggested = suggest_google_health_redirect_uri(redirect_uri)
        raise ValueError(
            f"OAuth client {client_key} uses Streamlit's /oauth2callback redirect URI. "
            f"Use {suggested} for Google Health OAuth."
        )
    if not scopes:
        raise ValueError(f"OAuth client {client_key} is missing scopes")

    return OAuthClientConfig(
        client_key=client_key,
        provider=str(row.get("provider") or "google_health"),
        environment=str(row.get("enviroment") or row.get("environment") or ""),
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        auth_uri=auth_uri,
        token_uri=token_uri,
        scopes=scopes,
        credentials_json_raw=None if secret_ref else raw,
    )


def _jwt_payload(token: str) -> Dict[str, Any]:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8"))
    except Exception:
        return {}


def is_fitbit_token_expired(token: Any, skew_seconds: int = 300) -> bool:
    if not has_value(token):
        return True
    payload = _jwt_payload(str(token))
    exp = payload.get("exp")
    if not exp:
        return False
    try:
        return int(exp) <= int(datetime.datetime.now().timestamp()) + skew_seconds
    except (TypeError, ValueError):
        return False


def google_health_available(row: Dict[str, Any], spreadsheet=None) -> bool:
    provider = normalize_provider(row.get("provider"))
    oauth_type = normalize_provider(row.get("oauth_type"))
    auth_status = normalize_provider(row.get("auth_status"))
    has_google_identity = has_value(row.get("health_user_id")) or has_value(row.get("oauth_client_key"))
    explicit_google = provider in GOOGLE_PROVIDER_NAMES or oauth_type in GOOGLE_PROVIDER_NAMES
    auth_allows_fetch = auth_status not in {"expired", "revoked", "disconnected", "failed", "error"}
    if auth_allows_fetch and (explicit_google or has_google_identity):
        return True

    if spreadsheet is None:
        return False

    watch_name = row.get("name") or row.get("watchName")
    if not has_value(watch_name):
        return False

    if hasattr(spreadsheet, "get_active_google_token_row"):
        return spreadsheet.get_active_google_token_row(str(watch_name)) is not None

    try:
        from utils.health_token_store import get_active_token_row

        return get_active_token_row(
            spreadsheet,
            watchName=str(watch_name),
            provider="google_health",
        ) is not None
    except Exception as exc:
        print(f"Could not check Google Health token store for {watch_name}: {exc}")
        return False


def choose_provider(row: Dict[str, Any], spreadsheet=None) -> Tuple[str, str]:
    row = clean_row(row)
    provider = normalize_provider(row.get("provider"))
    oauth_type = normalize_provider(row.get("oauth_type"))
    token = row.get("token")
    row_google_available = google_health_available(row)
    fitbit_expired = is_fitbit_token_expired(token)
    explicit_fitbit = provider in FITBIT_PROVIDER_NAMES or oauth_type in FITBIT_PROVIDER_NAMES
    explicit_google = provider in GOOGLE_PROVIDER_NAMES or oauth_type in GOOGLE_PROVIDER_NAMES

    watch_name = str(row.get("name") or row.get("watchName") or "").strip()
    if explicit_fitbit and hasattr(spreadsheet, "get_active_fitbit_token_row"):
        if spreadsheet.get_active_fitbit_token_row(watch_name) is not None:
            return "fitbit", "spreadsheet provider is Fitbit Web API and OAuth token is available"
    if explicit_fitbit and has_value(token) and not fitbit_expired:
        return "fitbit", "spreadsheet provider is Fitbit Web API and token is valid"
    if explicit_google and row_google_available:
        return "google_health", "spreadsheet provider is Google Health"

    google_available = row_google_available or google_health_available(row, spreadsheet)
    if google_available and (fitbit_expired or not has_value(token)):
        return "google_health", "Fitbit token missing/expired and Google Health identity exists"
    if google_available and not explicit_fitbit:
        return "google_health", "Google Health identity exists"
    return "fitbit", "defaulting to Fitbit Web API"


def _parse_datetime(value: Any) -> Optional[datetime.datetime]:
    if not has_value(value):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return None


def _as_float(value: Any) -> Optional[float]:
    if not has_value(value):
        return None
    try:
        return float(str(value).replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _latest_timestamp(payload: Any) -> Optional[str]:
    candidates = _walk_values(payload, {"timestamp", "time", "dateTime", "datetime", "startTime", "endTime"})
    parsed = [_parse_datetime(value) for value in candidates]
    parsed = [value for value in parsed if value]
    if not parsed:
        return None
    return max(parsed).isoformat()


def _walk_values(payload: Any, keys: set) -> list:
    values = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key) in keys:
                values.append(value)
            values.extend(_walk_values(value, keys))
    elif isinstance(payload, list):
        for item in payload:
            values.extend(_walk_values(item, keys))
    return values


def _latest_numeric(payload: Any, keys: set) -> Optional[float]:
    values = [_as_float(value) for value in _walk_values(payload, keys)]
    values = [value for value in values if value is not None]
    if not values:
        if isinstance(payload, (int, float, str)):
            return _as_float(payload)
        return None
    return values[-1]


class GoogleHealthClient:
    def __init__(self, row: Dict[str, Any]):
        self.row = clean_row(row)
        self.base_url = (
            os.getenv("GOOGLE_HEALTH_API_BASE_URL")
            or os.getenv("HEALTH_API_BASE_URL")
            or ""
        ).rstrip("/")
        self.user_id = (
            self.row.get("health_user_id")
            or self.row.get("google_health_user_id")
            or self.row.get("oauth_client_key")
            or self.row.get("name")
            or ""
        )
        self.token = (
            self.row.get("google_health_token")
            or (self.row.get("token") if normalize_provider(self.row.get("provider")) in GOOGLE_PROVIDER_NAMES else "")
            or os.getenv("GOOGLE_HEALTH_API_TOKEN")
            or os.getenv("HEALTH_API_TOKEN")
            or ""
        )
        self.api_key = os.getenv("GOOGLE_HEALTH_API_KEY") or os.getenv("HEALTH_API_KEY") or ""

    def _endpoint(self, feature: str) -> str:
        env_key = f"GOOGLE_HEALTH_{feature.upper()}_ENDPOINT"
        template = os.getenv(env_key)
        if template:
            return template.format(user_id=self.user_id, feature=feature)
        defaults = {
            "sync": "/users/{user_id}/sync",
            "heart_rate": "/users/{user_id}/heart-rate",
            "steps": "/users/{user_id}/steps",
            "sleep": "/users/{user_id}/sleep",
        }
        return defaults[feature].format(user_id=self.user_id)

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if has_value(self.token):
            headers["Authorization"] = f"Bearer {self.token}"
        if has_value(self.api_key):
            headers["X-API-Key"] = str(self.api_key)
        if has_value(self.row.get("oauth_client_key")):
            headers["X-Client-Key"] = str(self.row.get("oauth_client_key"))
        return headers

    def fetch_feature(self, feature: str, start_time: datetime.datetime, end_time: datetime.datetime) -> Dict[str, Any]:
        if not self.base_url:
            print("Google Health API base URL is not configured; using spreadsheet timestamps only")
            return {}
        url = f"{self.base_url}{self._endpoint(feature)}"
        params = {
            "user_id": self.user_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
        }
        response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        if response.status_code != 200:
            print(f"Google Health {feature} request failed for {self.row.get('name', '')}: {response.status_code}")
            return {}
        try:
            return response.json()
        except ValueError:
            return {}

    def snapshot(self) -> Dict[str, Any]:
        now = datetime.datetime.now()
        hour_ago = now - datetime.timedelta(hours=1)
        six_hours_ago = now - datetime.timedelta(hours=6)
        yesterday = now - datetime.timedelta(days=1)

        sync_payload = self.fetch_feature("sync", hour_ago, now)
        hr_payload = self.fetch_feature("heart_rate", hour_ago, now)
        steps_payload = self.fetch_feature("steps", six_hours_ago, now)
        sleep_payload = self.fetch_feature("sleep", yesterday, now)

        sleep_start = _walk_values(sleep_payload, {"startTime", "start_time", "start"})
        sleep_end = _walk_values(sleep_payload, {"endTime", "end_time", "end"})
        duration = _latest_numeric(sleep_payload, {"duration", "duration_ms", "sleep_duration", "minutesAsleep"})
        if duration and duration > 1000:
            duration = duration / (1000 * 60 * 60)
        elif duration and duration > 24:
            duration = duration / 60

        sync_date = (
            _latest_timestamp(sync_payload)
            or _latest_timestamp(hr_payload)
            or _latest_timestamp(steps_payload)
            or self.row.get("last_data_timestamp")
            or self.row.get("last_successful_fetch_at")
            or ""
        )

        return {
            **self.row,
            "provider": "google_health",
            "syncDate": sync_date,
            "battery": "",
            "battery_supported": False,
            "HR": _latest_numeric(hr_payload, {"heartRate", "heart_rate", "bpm", "value"}),
            "steps": _latest_numeric(steps_payload, {"steps", "step_count", "count", "value"}),
            "sleep_start": sleep_start[-1] if sleep_start else "",
            "sleep_end": sleep_end[-1] if sleep_end else "",
            "sleep_duration": duration or "",
        }


def _fitbit_snapshot_with_token(row: Dict[str, Any], token: str) -> Dict[str, Any]:
    row = clean_row(row)
    watch = Watch(
        name=str(row.get("name") or row.get("watchName") or ""),
        project=str(row.get("project") or ""),
        token=str(token),
    )
    watch.update_device_info(force_fetch=True)
    hr = watch.get_current_hourly_HR(force_fetch=True) if hasattr(watch, "get_current_hourly_HR") else ""
    steps = watch.get_current_hourly_steps(force_fetch=True) if hasattr(watch, "get_current_hourly_steps") else ""
    sleep_start, sleep_end = (
        watch.get_last_sleep_start_end(force_fetch=True)
        if hasattr(watch, "get_last_sleep_start_end")
        else ("", "")
    )
    sleep_duration = ""
    if sleep_start and sleep_end:
        sleep_start_dt = _parse_datetime(sleep_start)
        sleep_end_dt = _parse_datetime(sleep_end)
        if sleep_start_dt and sleep_end_dt:
            sleep_duration = (sleep_end_dt - sleep_start_dt).total_seconds() / 3600
    return {
        **row,
        "provider": "fitbit",
        "battery": watch.battery_level if hasattr(watch, "battery_level") else "",
        "battery_supported": True,
        "HR": hr,
        "syncDate": watch.last_sync_time.isoformat() if getattr(watch, "last_sync_time", None) else "",
        "sleep_start": sleep_start or "",
        "sleep_end": sleep_end or "",
        "sleep_duration": sleep_duration or "",
        "steps": steps,
    }


def fitbit_snapshot(row: Dict[str, Any], context: SnapshotContext | None = None) -> Dict[str, Any]:
    row = clean_row(row)
    if context is not None:
        token = context.get_valid_fitbit_access_token(row)
        try:
            return _fitbit_snapshot_with_token(row, token)
        except ApiAuthError:
            refreshed_token = context.get_valid_fitbit_access_token(row, force_refresh=True)
            print(f"Refreshed Fitbit token after 401 for watch {row.get('name') or row.get('watchName')}")
            return _fitbit_snapshot_with_token(row, refreshed_token)

    token = row.get("token")
    if has_value(token):
        return _fitbit_snapshot_with_token(row, str(token))

    watch = WatchFactory.create_from_details(row)
    watch.update_device_info(force_fetch=True)
    hr = watch.get_current_hourly_HR(force_fetch=True) if hasattr(watch, "get_current_hourly_HR") else ""
    steps = watch.get_current_hourly_steps(force_fetch=True) if hasattr(watch, "get_current_hourly_steps") else ""
    sleep_start, sleep_end = (
        watch.get_last_sleep_start_end(force_fetch=True)
        if hasattr(watch, "get_last_sleep_start_end")
        else ("", "")
    )
    sleep_duration = ""
    if sleep_start and sleep_end:
        sleep_start_dt = _parse_datetime(sleep_start)
        sleep_end_dt = _parse_datetime(sleep_end)
        if sleep_start_dt and sleep_end_dt:
            sleep_duration = (sleep_end_dt - sleep_start_dt).total_seconds() / 3600
    return {
        **row,
        "provider": "fitbit",
        "battery": watch.battery_level if hasattr(watch, "battery_level") else "",
        "battery_supported": True,
        "HR": hr,
        "syncDate": watch.last_sync_time.isoformat() if getattr(watch, "last_sync_time", None) else "",
        "sleep_start": sleep_start or "",
        "sleep_end": sleep_end or "",
        "sleep_duration": sleep_duration or "",
        "steps": steps,
    }


def google_health_snapshot(spreadsheet, row: Dict[str, Any], context: SnapshotContext | None = None) -> Dict[str, Any]:
    row = clean_row(row)
    try:
        if context is not None:
            client = context.google_health_client(row)
        else:
            from services.health_client_factory import HealthClientFactory

            client_row = {**row, "provider": "google_health", "oauth_type": "google_health"}
            client = HealthClientFactory.from_watch_row(spreadsheet, client_row)
            if client is None:
                raise RuntimeError("Google Health client factory returned no client")

        hr = client.get_current_hourly_hr()
        steps = client.get_current_hourly_steps()
        try:
            sleep_start, sleep_end = client.get_last_sleep_start_end()
            sleep_duration = client.get_last_sleep_duration()
        except Exception as sleep_exc:
            print(f"Google Health sleep fetch failed for {row.get('name', '')}: {sleep_exc}")
            sleep_start, sleep_end, sleep_duration = None, None, None

        # Google Health does not expose Fitbit-style device battery or sync metadata.
        # A successful feature read marks the API observation time instead.
        sync_date = sleep_end or sleep_start
        if not sync_date and any(value not in (None, "") for value in (hr, steps, sleep_duration)):
            sync_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return {
            **row,
            "provider": "google_health",
            "syncDate": sync_date or "",
            "battery": "",
            "battery_supported": False,
            "HR": hr if hr is not None else "",
            "steps": steps if steps is not None else "",
            "sleep_start": sleep_start or "",
            "sleep_end": sleep_end or "",
            "sleep_duration": sleep_duration if sleep_duration is not None else "",
        }
    except Exception as exc:
        print(f"Spreadsheet-backed Google Health client failed for {row.get('name', '')}: {exc}")
        return GoogleHealthClient(row).snapshot()


def collect_watch_snapshot(row: Dict[str, Any], spreadsheet=None, context: SnapshotContext | None = None) -> Dict[str, Any]:
    row = clean_row(row)
    provider, reason = choose_provider(row, context or spreadsheet)
    print(f"Using {provider} for watch {row.get('name', '')}: {reason}")

    if provider == "google_health":
        if spreadsheet is not None:
            return google_health_snapshot(spreadsheet, row, context=context)
        return GoogleHealthClient(row).snapshot()

    try:
        return fitbit_snapshot(row, context=context)
    except Exception as fitbit_error:
        if google_health_available(row, context or spreadsheet):
            print(f"Fitbit fetch failed for {row.get('name', '')}; falling back to Google Health: {fitbit_error}")
            if spreadsheet is not None:
                return google_health_snapshot(spreadsheet, row, context=context)
            return GoogleHealthClient(row).snapshot()
        raise


def is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, ApiRateLimitError):
        return True
    text = str(exc).lower()
    return (
        "rate limit" in text
        or "status 429" in text
        or " 429 " in text
        or "too many requests" in text
        or "quota exceeded" in text
    )


def _rate_limit_retry_after(exc: Exception) -> int | None:
    retry_after = getattr(exc, "retry_after", None)
    try:
        return int(retry_after) if retry_after else None
    except (TypeError, ValueError):
        return None


def save_rate_limited_watches(records: list[dict[str, Any]]) -> None:
    if not records:
        return
    RATE_LIMIT_TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict[str, Any]] = []
    if RATE_LIMIT_TRACKING_FILE.exists():
        try:
            existing = json.loads(RATE_LIMIT_TRACKING_FILE.read_text())
            if not isinstance(existing, list):
                existing = []
        except Exception:
            existing = []
    existing.extend(records)
    RATE_LIMIT_TRACKING_FILE.write_text(json.dumps(existing[-1000:], indent=2, default=str))


def _watch_name(row: dict[str, Any]) -> str:
    return str(row.get("name") or row.get("watchName") or "").strip()


def collect_watch_snapshots_batch(
    rows: Iterable[dict[str, Any]],
    spreadsheet=None,
    *,
    context: SnapshotContext | None = None,
    retry_attempts: int | None = None,
    retry_delay_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch health-provider snapshots for all active rows first, then return rows for log processing.
    Rate-limited rows are saved and retried before falling back to their original row values.
    """
    context = context or SnapshotContext(spreadsheet)
    retry_attempts = retry_attempts if retry_attempts is not None else int(os.getenv("RUN_COLLECTION_MAX_ATTEMPTS", "3") or 3)
    retry_delay_seconds = retry_delay_seconds if retry_delay_seconds is not None else int(os.getenv("RUN_COLLECTION_RETRY_DELAY_SECONDS", "180") or 180)
    retry_attempts = max(1, retry_attempts)
    retry_delay_seconds = max(0, retry_delay_seconds)

    pending = [clean_row(row) for row in rows]
    snapshots_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    original_by_key: dict[tuple[str, str], dict[str, Any]] = {
        (str(row.get("project", "")), _watch_name(row)): row for row in pending
    }
    unresolved_rate_limited: list[dict[str, Any]] = []

    for attempt in range(1, retry_attempts + 1):
        next_pending: list[dict[str, Any]] = []
        rate_limit_records: list[dict[str, Any]] = []

        for row in pending:
            key = (str(row.get("project", "")), _watch_name(row))
            try:
                snapshots_by_key[key] = collect_watch_snapshot(row, spreadsheet, context=context)
                print(f"Successfully updated data for watch {_watch_name(row)} from {snapshots_by_key[key].get('provider', 'unknown')}")
            except Exception as exc:
                if is_rate_limit_error(exc):
                    record = {
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "attempt": attempt,
                        "max_attempts": retry_attempts,
                        "project": row.get("project", ""),
                        "watchName": _watch_name(row),
                        "provider": row.get("provider") or row.get("oauth_type") or "",
                        "retry_after": _rate_limit_retry_after(exc) or "",
                        "error": str(exc),
                    }
                    rate_limit_records.append(record)
                    next_pending.append(row)
                    print(f"Rate limited watch {_watch_name(row)} on attempt {attempt}/{retry_attempts}: {exc}")
                else:
                    snapshots_by_key[key] = row
                    print(f"Error updating watch {_watch_name(row)} via health API provider: {exc}")

        save_rate_limited_watches(rate_limit_records)
        unresolved_rate_limited = rate_limit_records
        if not next_pending:
            break
        if attempt >= retry_attempts:
            break

        retry_after_values = [
            int(record["retry_after"])
            for record in rate_limit_records
            if str(record.get("retry_after", "")).isdigit()
        ]
        delay = max(retry_delay_seconds, max(retry_after_values, default=0))
        if delay > 0:
            print(f"Waiting {delay}s before retrying {len(next_pending)} rate-limited watches")
            time.sleep(delay)
        pending = next_pending

    for record in unresolved_rate_limited:
        key = (str(record.get("project", "")), str(record.get("watchName", "")))
        if key not in snapshots_by_key and key in original_by_key:
            snapshots_by_key[key] = original_by_key[key]

    return [snapshots_by_key.get(key, row) for key, row in original_by_key.items()]
