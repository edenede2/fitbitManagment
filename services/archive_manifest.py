"""Persistent Sheets manifest and scheduler audit records."""

from __future__ import annotations

from typing import Any

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from utils.health_token_store import _append, _update_row_by_keys, new_uuid, utc_now_iso


ARCHIVE_MANIFEST_TAB = "archive_manifest"
JOB_RUNS_TAB = "job_runs"
CLOCK_STATUS_TAB = "clock_status"

ARCHIVE_COLUMNS = [
    "logical_key", "project", "watchName", "provider", "data_type", "cadence",
    "period", "source_start", "source_end", "checksum", "drive_file_id",
    "drive_web_view_link", "size_bytes", "status", "message", "created_at", "updated_at",
]
JOB_COLUMNS = [
    "run_id", "job_name", "started_at", "finished_at", "status", "message",
]
CLOCK_COLUMNS = ["process", "heartbeat_at", "release", "status"]


def archive_logical_key(project: str, watch_name: str, provider: str, data_type: str, period: str) -> str:
    return "/".join([project, watch_name, provider, data_type, period])


def get_archive_record(spreadsheet: Spreadsheet, logical_key: str) -> dict[str, Any] | None:
    rows = GoogleSheetsAdapter.get_rows(
        spreadsheet,
        ARCHIVE_MANIFEST_TAB,
        "logical_key",
        logical_key=logical_key,
    )
    return rows[-1] if rows else None


def upsert_archive_record(spreadsheet: Spreadsheet, values: dict[str, Any]) -> None:
    now = utc_now_iso()
    row = {**values, "updated_at": now}
    updated = _update_row_by_keys(
        spreadsheet,
        ARCHIVE_MANIFEST_TAB,
        keys={"logical_key": values["logical_key"]},
        updates=row,
    )
    if not updated:
        _append(
            spreadsheet,
            ARCHIVE_MANIFEST_TAB,
            ARCHIVE_COLUMNS,
            {**row, "created_at": now},
        )


def start_job(spreadsheet: Spreadsheet, job_name: str) -> tuple[str, str]:
    run_id = new_uuid()
    started = utc_now_iso()
    _append(
        spreadsheet,
        JOB_RUNS_TAB,
        JOB_COLUMNS,
        {
            "run_id": run_id,
            "job_name": job_name,
            "started_at": started,
            "status": "running",
        },
    )
    return run_id, started


def finish_job(spreadsheet: Spreadsheet, run_id: str, *, status: str, message: str = "") -> None:
    _update_row_by_keys(
        spreadsheet,
        JOB_RUNS_TAB,
        keys={"run_id": run_id},
        updates={
            "finished_at": utc_now_iso(),
            "status": status,
            "message": " ".join(str(message or "").split())[:500],
        },
    )


def heartbeat(spreadsheet: Spreadsheet) -> None:
    values = {
        "process": "heroku-clock",
        "heartbeat_at": utc_now_iso(),
        "release": __import__("os").getenv("HEROKU_RELEASE_VERSION", ""),
        "status": "running",
    }
    if not _update_row_by_keys(
        spreadsheet,
        CLOCK_STATUS_TAB,
        keys={"process": "heroku-clock"},
        updates=values,
    ):
        _append(spreadsheet, CLOCK_STATUS_TAB, CLOCK_COLUMNS, values)
