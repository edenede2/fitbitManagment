#!/usr/bin/env python3
"""Collect bounded wearable periods into the Shared Drive research archive."""

from __future__ import annotations

import argparse
import io
import json
import os
import traceback
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from firbitfilesOrgenizer import (
    _resolve_watch_access_token,
    get_active_watches,
)
from run_data_collection import load_runtime_config
from services.archive_manifest import (
    archive_logical_key,
    get_archive_record,
    upsert_archive_record,
)
from services.drive_archive import SharedDriveArchive, sha256_bytes
from services.health_client_factory import HealthClientFactory


LOCAL_TZ = ZoneInfo(os.getenv("SCHEDULER_TIMEZONE", "Asia/Jerusalem"))
FITBIT_ARCHIVE_CADENCE = {
    "heart_rate": "daily",
    "hrv": "daily",
    "calories": "daily",
    "steps": "monthly",
    "sleep": "monthly",
    "temperature": "monthly",
    "breathing_rate": "monthly",
}
GOOGLE_HEALTH_ARCHIVE_CADENCE = {
    # Keep the production Google Health archive aligned with the data categories
    # named in the final ethics-approved addendum for Study 385/23.
    "heart_rate": "daily",
    "steps": "monthly",
    "sleep": "monthly",
    "breathing_rate": "monthly",
}
ARCHIVE_CADENCE_BY_PROVIDER = {
    "fitbit": FITBIT_ARCHIVE_CADENCE,
    "google_health": GOOGLE_HEALTH_ARCHIVE_CADENCE,
}
GOOGLE_DATA_TYPES = {
    "heart_rate": ("heart-rate", "rollup"),
    "steps": ("steps", "rollup"),
    "sleep": ("sleep", "sleep"),
    "breathing_rate": ("daily-respiratory-rate", "daily"),
}
SUPPORTED_ARCHIVE_PROVIDERS = {"fitbit", "google_health"}
PROVIDER_FOLDER_NAMES = {
    "fitbit": "FITBIT",
    "google_health": "GOOGLE_HEALTH",
}
ARCHIVE_CATEGORY_BY_DATA_TYPE = {
    "heart_rate": "Physical Activity",
    "steps": "Physical Activity",
    "calories": "Physical Activity",
    "breathing_rate": "Physical Activity",
    "sleep": "Sleep",
    "hrv": "Sleep",
    "temperature": "Sleep",
}
ARCHIVE_FILENAME_PREFIX = {
    "heart_rate": "heart_rate",
    "steps": "steps",
    "calories": "calories",
    "breathing_rate": "respiratory_rate",
    "sleep": "sleep",
    "hrv": "hrv",
    "temperature": "skin_temperature",
}


@dataclass(frozen=True)
class PeriodWindow:
    period: str
    start_date: date
    end_date: date
    cadence: str

    @property
    def filename_suffix(self) -> str:
        return self.period


@dataclass(frozen=True)
class ArchiveArtifact:
    filename: str
    category: str
    content: bytes
    mime_type: str = "application/json"


def archive_category(data_type: str) -> str:
    try:
        return ARCHIVE_CATEGORY_BY_DATA_TYPE[data_type]
    except KeyError as exc:
        raise ValueError(f"No archive category for {data_type}") from exc


def archive_filename(data_type: str, window: PeriodWindow) -> str:
    try:
        prefix = ARCHIVE_FILENAME_PREFIX[data_type]
    except KeyError as exc:
        raise ValueError(f"No archive filename for {data_type}") from exc
    # The example names monthly files from the bounded period's first date.
    # For daily data this is naturally the data date too.
    return f"{prefix}-{window.start_date.isoformat()}.json"


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def parse_cutover(value: str) -> datetime:
    if not value:
        raise ValueError("ARCHIVE_CUTOVER_AT is required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TZ)
    return parsed.astimezone(LOCAL_TZ)


def enabled_archive_providers(value: str | None = None) -> set[str]:
    raw = value if value is not None else os.getenv(
        "ARCHIVE_ENABLED_PROVIDERS", "fitbit,google_health"
    )
    providers = {
        item.strip().casefold()
        for item in str(raw or "").split(",")
        if item.strip()
    }
    unsupported = providers - SUPPORTED_ARCHIVE_PROVIDERS
    if unsupported:
        raise ValueError(
            "ARCHIVE_ENABLED_PROVIDERS contains unsupported values: "
            + ", ".join(sorted(unsupported))
        )
    if not providers:
        raise ValueError("ARCHIVE_ENABLED_PROVIDERS must enable at least one provider")
    return providers


def archive_item_retry_delays(value: str | None = None) -> list[float]:
    """Return bounded delays for retries of failed individual archive items."""
    raw = value if value is not None else os.getenv(
        "ARCHIVE_ITEM_RETRY_DELAYS_SECONDS",
        "5,20",
    )
    delays: list[float] = []
    for item in str(raw or "").split(","):
        if not item.strip():
            continue
        try:
            delays.append(max(0.0, min(float(item.strip()), 60.0)))
        except ValueError as exc:
            raise ValueError(
                "ARCHIVE_ITEM_RETRY_DELAYS_SECONDS must be comma-separated numbers"
            ) from exc
    return delays


def period_windows(now: datetime, cadence: str, cutover: datetime) -> list[PeriodWindow]:
    local_now = now.astimezone(LOCAL_TZ)
    cutoff_date = cutover.astimezone(LOCAL_TZ).date()
    today = local_now.date()
    windows: list[PeriodWindow] = []
    if cadence == "daily":
        for target in (today - timedelta(days=1), today):
            if target >= cutoff_date:
                windows.append(PeriodWindow(target.isoformat(), target, target, cadence))
        return windows
    if cadence != "monthly":
        raise ValueError(f"Unsupported archive cadence: {cadence}")

    month_starts = [today.replace(day=1)]
    if today.day <= 3:
        month_starts.insert(0, (today.replace(day=1) - timedelta(days=1)).replace(day=1))
    for start in month_starts:
        month_end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        end = min(month_end, today)
        bounded_start = max(start, cutoff_date)
        if bounded_start <= end:
            windows.append(PeriodWindow(start.strftime("%Y-%m"), bounded_start, end, cadence))
    return windows


def _zip_tree(root: Path, metadata: dict[str, Any]) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(root.rglob("*")):
            if file_path.is_file():
                info = zipfile.ZipInfo(file_path.relative_to(root).as_posix())
                info.date_time = (1980, 1, 1, 0, 0, 0)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o600 << 16
                archive.writestr(info, file_path.read_bytes())
        metadata_info = zipfile.ZipInfo("admontracker-metadata.json")
        metadata_info.date_time = (1980, 1, 1, 0, 0, 0)
        metadata_info.compress_type = zipfile.ZIP_DEFLATED
        metadata_info.external_attr = 0o600 << 16
        archive.writestr(
            metadata_info,
            json.dumps(metadata, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8"),
        )
    return stream.getvalue()


def _fitbit_api_json(
    token: str,
    path: str,
    *,
    data_type: str,
) -> dict[str, Any]:
    response = requests.get(
        f"https://api.fitbit.com/{path.lstrip('/')}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    if not response.ok:
        # Never include Fitbit response bodies in logs: they may contain user
        # data or provider details that do not belong in operational logs.
        raise RuntimeError(
            f"Fitbit API failed with HTTP {response.status_code} for {data_type}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Fitbit returned invalid JSON for {data_type}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Fitbit returned an invalid payload for {data_type}")
    return payload


def _fitbit_datetime(date_text: str, time_text: str) -> str:
    parsed = datetime.strptime(
        f"{date_text} {time_text.split('.')[0]}",
        "%Y-%m-%d %H:%M:%S",
    )
    return parsed.strftime("%m/%d/%y %H:%M:%S")


def _fitbit_heart_rate_records(
    token: str,
    window: PeriodWindow,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for target in pd.date_range(window.start_date, window.end_date):
        target_text = target.date().isoformat()
        payload = _fitbit_api_json(
            token,
            (
                f"1/user/-/activities/heart/date/{target_text}/1d/1sec/"
                "time/00:00/23:59.json"
            ),
            data_type="heart_rate",
        )
        summaries = payload.get("activities-heart") or []
        date_text = str(
            summaries[0].get("dateTime")
            if summaries and isinstance(summaries[0], dict)
            else target_text
        )
        dataset = (payload.get("activities-heart-intraday") or {}).get("dataset") or []
        for item in dataset:
            if not isinstance(item, dict) or item.get("time") in (None, ""):
                continue
            bpm = item.get("value")
            if bpm in (None, ""):
                continue
            # The intraday API does not return Fitbit export's confidence field.
            # Keep the compatible field present but explicitly unknown instead
            # of fabricating the legacy export value of 2.
            records.append(
                {
                    "dateTime": _fitbit_datetime(date_text, str(item["time"])),
                    "value": {"bpm": bpm, "confidence": None},
                }
            )
    return records


def _fitbit_intraday_records(
    token: str,
    window: PeriodWindow,
    *,
    signal: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for target in pd.date_range(window.start_date, window.end_date):
        target_text = target.date().isoformat()
        payload = _fitbit_api_json(
            token,
            (
                f"1/user/-/activities/{signal}/date/{target_text}/1d/1min/"
                "time/00:00/23:59.json"
            ),
            data_type=signal,
        )
        summaries = payload.get(f"activities-{signal}") or []
        date_text = str(
            summaries[0].get("dateTime")
            if summaries and isinstance(summaries[0], dict)
            else target_text
        )
        dataset = (payload.get(f"activities-{signal}-intraday") or {}).get("dataset") or []
        for item in dataset:
            if not isinstance(item, dict) or item.get("time") in (None, ""):
                continue
            value = item.get("value")
            if value in (None, ""):
                continue
            records.append(
                {
                    "dateTime": _fitbit_datetime(date_text, str(item["time"])),
                    # Fitbit's exported step example stores this value as text.
                    "value": str(value),
                }
            )
    return records


def _fitbit_sleep_records(token: str, window: PeriodWindow) -> list[dict[str, Any]]:
    payload = _fitbit_api_json(
        token,
        (
            "1.1/user/-/sleep/date/"
            f"{window.start_date.isoformat()}/{window.end_date.isoformat()}.json"
        ),
        data_type="sleep",
    )
    records: list[dict[str, Any]] = []
    for source in payload.get("sleep") or []:
        if not isinstance(source, dict):
            continue
        record = dict(source)
        if "isMainSleep" in record:
            record["mainSleep"] = record.pop("isMainSleep")
        records.append(record)
    return records


def _fitbit_range_records(
    token: str,
    window: PeriodWindow,
    *,
    data_type: str,
) -> list[dict[str, Any]]:
    start = window.start_date.isoformat()
    end = window.end_date.isoformat()
    if data_type == "breathing_rate":
        path, response_key = f"1/user/-/br/date/{start}/{end}/all.json", "br"
    elif data_type == "temperature":
        path, response_key = f"1/user/-/temp/skin/date/{start}/{end}.json", "tempSkin"
    elif data_type == "hrv":
        path, response_key = (
            f"1/user/-/hrv/date/{start}/all.json"
            if start == end
            else f"1/user/-/hrv/date/{start}/{end}/all.json"
        ), "hrv"
    else:
        raise ValueError(f"Unsupported Fitbit range data type: {data_type}")
    payload = _fitbit_api_json(token, path, data_type=data_type)
    return [item for item in payload.get(response_key) or [] if isinstance(item, dict)]


def _fitbit_package(
    spreadsheet,
    row: dict[str, Any],
    data_type: str,
    window: PeriodWindow,
) -> ArchiveArtifact:
    token = _resolve_watch_access_token(row, spreadsheet)
    if not token:
        raise RuntimeError("No active Fitbit token")
    if data_type == "heart_rate":
        records = _fitbit_heart_rate_records(token, window)
    elif data_type in {"steps", "calories"}:
        records = _fitbit_intraday_records(
            token,
            window,
            signal=data_type,
        )
    elif data_type == "sleep":
        records = _fitbit_sleep_records(token, window)
    else:
        records = _fitbit_range_records(
            token,
            window,
            data_type=data_type,
        )
    if not records:
        raise RuntimeError("Provider returned no archiveable Fitbit data")
    return ArchiveArtifact(
        filename=archive_filename(data_type, window),
        category=archive_category(data_type),
        content=_json_bytes(records),
    )


def _utc_bounds(window: PeriodWindow) -> tuple[str, str]:
    start = datetime.combine(window.start_date, time.min, tzinfo=LOCAL_TZ)
    end = datetime.combine(window.end_date + timedelta(days=1), time.min, tzinfo=LOCAL_TZ)
    return (
        start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def _google_filter(data_type: str, record_type: str, window: PeriodWindow) -> str:
    start = window.start_date.isoformat()
    end = (window.end_date + timedelta(days=1)).isoformat()
    daily_fields = {
        "daily-respiratory-rate": "dailyRespiratoryRate.date",
    }
    interval_fields = {
        "exercise": "exercise.interval.civil_start_time",
    }
    if record_type == "daily":
        field = daily_fields.get(data_type)
        if not field:
            raise ValueError(f"No daily list filter for {data_type}")
    elif record_type == "sleep":
        field = "sleep.interval.civil_end_time"
    elif record_type == "interval":
        field = interval_fields.get(data_type)
        if not field:
            raise ValueError(f"No interval list filter for {data_type}")
    else:
        raise ValueError(f"No list filter for {record_type}")
    return f'{field} >= "{start}" AND {field} < "{end}"'


def _google_package(
    spreadsheet,
    row: dict[str, Any],
    data_type: str,
    window: PeriodWindow,
) -> ArchiveArtifact:
    client = HealthClientFactory.from_watch_row(spreadsheet, row)
    if client is None:
        raise RuntimeError("Google Health client is unavailable")
    google_type, record_type = GOOGLE_DATA_TYPES[data_type]
    if record_type == "rollup":
        start_time, end_time = _utc_bounds(window)
        payload = client.fetch_raw(
            google_type,
            start_time=start_time,
            end_time=end_time,
            window_size="60s" if data_type == "heart_rate" else "3600s",
        )
    else:
        payload = client.fetch_raw(
            google_type,
            filter_expr=_google_filter(google_type, record_type, window),
        )
    if not any(payload.values()):
        raise RuntimeError("Provider returned no archiveable Google Health data")
    points = payload.get("dataPoints") or payload.get("rollupDataPoints") or []
    if not points:
        raise RuntimeError("Provider returned no archiveable Google Health data")
    return ArchiveArtifact(
        filename=archive_filename(data_type, window),
        category=archive_category(data_type),
        content=_json_bytes(points),
    )


def _archive_base_record(
    row: dict[str, Any],
    *,
    provider: str,
    data_type: str,
    cadence: str,
    window: PeriodWindow,
) -> dict[str, Any]:
    watch_name = str(row.get("name") or row.get("watchName") or "").strip()
    project = str(row.get("project") or "unassigned").strip()
    return {
        "logical_key": archive_logical_key(
            project,
            watch_name,
            provider,
            data_type,
            window.period,
        ),
        "project": project,
        "watchName": watch_name,
        "provider": provider,
        "data_type": data_type,
        "cadence": cadence,
        "period": window.period,
        "source_start": window.start_date.isoformat(),
        "source_end": window.end_date.isoformat(),
    }


def _collect_archive_item(
    *,
    spreadsheet,
    drive,
    shadow: bool,
    row: dict[str, Any],
    provider: str,
    data_type: str,
    cadence: str,
    window: PeriodWindow,
) -> str:
    """Collect one logical archive item and return its final status.

    Exceptions are intentionally allowed to escape so the caller can retry only
    this item.  Empty provider responses are a valid terminal outcome and are not
    retried.
    """
    base_record = _archive_base_record(
        row,
        provider=provider,
        data_type=data_type,
        cadence=cadence,
        window=window,
    )
    try:
        artifact = (
            _google_package(spreadsheet, row, data_type, window)
            if provider == "google_health"
            else _fitbit_package(spreadsheet, row, data_type, window)
        )
    except RuntimeError as exc:
        message = " ".join(str(exc).split())[:300]
        if "no archiveable" not in message.casefold():
            raise
        upsert_archive_record(
            spreadsheet,
            {**base_record, "status": "empty", "message": message},
        )
        return "empty"

    checksum = sha256_bytes(artifact.content)
    existing = get_archive_record(spreadsheet, base_record["logical_key"])
    if (
        existing
        and str(existing.get("checksum") or "") == checksum
        and str(existing.get("status") or "") == "uploaded"
    ):
        return "unchanged"
    if shadow:
        upsert_archive_record(
            spreadsheet,
            {
                **base_record,
                "checksum": checksum,
                "size_bytes": len(artifact.content),
                "status": "shadow_validated",
            },
        )
        return "shadow"

    upload = drive.upsert_file(
        project=base_record["project"],
        watch_name=base_record["watchName"],
        provider=PROVIDER_FOLDER_NAMES[provider],
        category=artifact.category,
        filename=artifact.filename,
        content=artifact.content,
        mime_type=artifact.mime_type,
    )
    upsert_archive_record(
        spreadsheet,
        {
            **base_record,
            "checksum": upload.checksum,
            "drive_file_id": upload.file_id,
            "drive_web_view_link": upload.web_view_link,
            "size_bytes": upload.size,
            "status": "uploaded",
            "message": upload.action,
        },
    )
    return "uploaded"


def collect_drive_archives(*, now: datetime | None = None, shadow: bool | None = None) -> dict[str, int]:
    load_runtime_config()
    now = now or datetime.now(LOCAL_TZ)
    if shadow is None:
        shadow = os.getenv("ARCHIVE_SHADOW_MODE", "true").strip().casefold() in {"1", "true", "yes", "on"}
    cutover_raw = os.getenv("ARCHIVE_CUTOVER_AT", "")
    cutover = parse_cutover(cutover_raw) if cutover_raw else now
    enabled_providers = enabled_archive_providers()
    if not shadow and not cutover_raw:
        raise RuntimeError("ARCHIVE_CUTOVER_AT must be set before disabling shadow mode")

    watches, spreadsheet = get_active_watches(return_spreadsheet=True)
    if spreadsheet is None:
        raise RuntimeError("Could not connect to the production spreadsheet")
    drive = None if shadow else SharedDriveArchive.from_environment()
    counts = {
        "uploaded": 0,
        "unchanged": 0,
        "shadow": 0,
        "empty": 0,
        "failed": 0,
        "retried": 0,
        "provider_skipped": 0,
    }
    tasks: list[dict[str, Any]] = []
    for row in watches.iter_rows(named=True):
        watch_name = str(row.get("name") or row.get("watchName") or "").strip()
        provider_value = str(row.get("provider") or row.get("oauth_type") or "fitbit").casefold()
        provider = "google_health" if "google" in provider_value or provider_value == "health" else "fitbit"
        if not watch_name:
            continue
        if provider not in enabled_providers:
            counts["provider_skipped"] += 1
            continue
        for data_type, cadence in ARCHIVE_CADENCE_BY_PROVIDER[provider].items():
            for window in period_windows(now, cadence, cutover):
                tasks.append(
                    {
                        "row": row,
                        "provider": provider,
                        "data_type": data_type,
                        "cadence": cadence,
                        "window": window,
                    }
                )

    pending = tasks
    retry_delays = archive_item_retry_delays()
    for pass_index in range(len(retry_delays) + 1):
        retry_pending: list[dict[str, Any]] = []
        for task in pending:
            row = task["row"]
            provider = task["provider"]
            data_type = task["data_type"]
            cadence = task["cadence"]
            window = task["window"]
            base_record = _archive_base_record(
                row,
                provider=provider,
                data_type=data_type,
                cadence=cadence,
                window=window,
            )
            try:
                status = _collect_archive_item(
                    spreadsheet=spreadsheet,
                    drive=drive,
                    shadow=shadow,
                    row=row,
                    provider=provider,
                    data_type=data_type,
                    cadence=cadence,
                    window=window,
                )
                counts[status] += 1
            except Exception as exc:
                if pass_index < len(retry_delays):
                    retry_pending.append(task)
                    continue
                upsert_archive_record(
                    spreadsheet,
                    {
                        **base_record,
                        "status": "failed",
                        "message": type(exc).__name__,
                    },
                )
                counts["failed"] += 1

        if not retry_pending:
            break
        counts["retried"] += len(retry_pending)
        delay = retry_delays[pass_index]
        print(
            f"Retrying {len(retry_pending)} failed archive operations "
            f"in {delay:g}s"
        )
        if delay:
            import time as time_module

            time_module.sleep(delay)
        pending = retry_pending
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Upload to Drive; default is shadow mode")
    args = parser.parse_args()
    try:
        result = collect_drive_archives(shadow=not args.apply)
        print(json.dumps(result, sort_keys=True))
        return 1 if result["failed"] else 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
