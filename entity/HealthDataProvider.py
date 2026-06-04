import base64
import datetime
import json
import os
from typing import Any, Dict, Optional, Tuple

import requests

from entity.Watch import Watch, WatchFactory


GOOGLE_PROVIDER_NAMES = {"google", "google_health", "google health", "google_health_api", "health"}
FITBIT_PROVIDER_NAMES = {"fitbit", "fitbit_web_api", "fitbit web api", "fitbit_api"}


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


def fitbit_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    row = clean_row(row)
    token = row.get("token")
    if has_value(token):
        watch = Watch(
            name=str(row.get("name") or row.get("watchName") or ""),
            project=str(row.get("project") or ""),
            token=str(token),
        )
    else:
        watch = WatchFactory.create_from_details(row)
    watch.update_device_info()
    sleep_start, sleep_end = (
        watch.get_last_sleep_start_end()
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
        "HR": watch.get_current_hourly_HR() if hasattr(watch, "get_current_hourly_HR") else "",
        "syncDate": watch.last_sync_time.isoformat() if getattr(watch, "last_sync_time", None) else "",
        "sleep_start": sleep_start or "",
        "sleep_end": sleep_end or "",
        "sleep_duration": sleep_duration or "",
        "steps": watch.get_current_hourly_steps() if hasattr(watch, "get_current_hourly_steps") else "",
    }


def google_health_snapshot(spreadsheet, row: Dict[str, Any]) -> Dict[str, Any]:
    row = clean_row(row)
    try:
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


def collect_watch_snapshot(row: Dict[str, Any], spreadsheet=None) -> Dict[str, Any]:
    row = clean_row(row)
    provider, reason = choose_provider(row, spreadsheet)
    print(f"Using {provider} for watch {row.get('name', '')}: {reason}")

    if provider == "google_health":
        if spreadsheet is not None:
            return google_health_snapshot(spreadsheet, row)
        return GoogleHealthClient(row).snapshot()

    try:
        return fitbit_snapshot(row)
    except Exception as fitbit_error:
        if google_health_available(row, spreadsheet):
            print(f"Fitbit fetch failed for {row.get('name', '')}; falling back to Google Health: {fitbit_error}")
            if spreadsheet is not None:
                return google_health_snapshot(spreadsheet, row)
            return GoogleHealthClient(row).snapshot()
        raise
