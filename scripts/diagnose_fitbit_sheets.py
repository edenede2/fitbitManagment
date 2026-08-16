#!/usr/bin/env python3
"""Diagnose Fitbit OAuth token state and Google Sheets workbook limits.

This script does not print OAuth token values. With --live-probe it calls the
Fitbit devices endpoint for the selected watches. If that endpoint returns 401
and a refresh token exists, the script attempts exactly one refresh and persists
the rotated token row to the normal Fitbit OAuth token store.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Any

import gspread
import requests
from google.oauth2.service_account import Credentials

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from model.config import get_secrets
from run_data_collection import load_runtime_config
from entity.Sheet import GoogleSheetsAdapter
from utils.fitbit_oauth import FitbitOAuthError, now_ts, refresh_tokens

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]
TOKEN_HEADERS = [
    "watchName",
    "fitbit_user_id",
    "access_token",
    "refresh_token",
    "expires_at",
    "scope",
    "created_at",
]
DEVICE_URL = "https://api.fitbit.com/1/user/-/devices.json"
DEFAULT_WATCHES = ["CWT_093", "CWTt_093"]
DEFAULT_BOUNDED_SHEETS = ["FitbitLog", "suspicious_nums", "late_nums"]


def _client():
    secrets = get_secrets()
    credentials = Credentials.from_service_account_info(
        secrets["gcp_service_account"],
        scopes=SCOPES,
    )
    return gspread.authorize(credentials)


def _spreadsheet_key(explicit_key: str | None = None) -> str:
    if explicit_key:
        return explicit_key
    load_runtime_config()
    secrets = get_secrets()
    key = os.getenv("SPREADSHEET_KEY") or secrets.get("spreadsheet_key", "")
    if not key:
        raise RuntimeError("Missing SPREADSHEET_KEY/spreadsheet_key")
    return str(key)


def _records(worksheet) -> list[dict[str, Any]]:
    try:
        return worksheet.get_all_records()
    except gspread.exceptions.GSpreadException:
        values = worksheet.get_all_values()
        if not values:
            return []
        headers = [str(header or "").strip() for header in values[0]]
        return [
            {headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(headers))}
            for row in values[1:]
            if any(row)
        ]


def _safe_int(value: Any) -> int:
    try:
        return int(float(str(value or "0").strip()))
    except (TypeError, ValueError):
        return 0


def _jwt_payload(token: Any) -> dict[str, Any]:
    text = str(token or "").strip()
    try:
        payload = text.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
    except Exception:
        return {}


def _token_exp_iso(token: Any) -> str:
    exp = _jwt_payload(token).get("exp")
    if not exp:
        return ""
    try:
        return dt.datetime.fromtimestamp(int(exp), tz=dt.timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def _expires_in_seconds(expires_at: Any) -> int:
    return _safe_int(expires_at) - now_ts()


def _latest_token_row(token_rows: list[dict[str, Any]], watch_name: str) -> dict[str, Any] | None:
    rows = [
        row for row in token_rows
        if str(row.get("watchName") or "").strip() == watch_name
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: _safe_int(row.get("created_at")))


def _watch_row(fitbit_rows: list[dict[str, Any]], watch_name: str) -> dict[str, Any] | None:
    rows = [
        row for row in fitbit_rows
        if str(row.get("name") or row.get("watchName") or "").strip() == watch_name
    ]
    return rows[-1] if rows else None


def _sanitize_response_text(text: str, *secrets: str) -> str:
    redacted = " ".join(str(text or "").strip().split())
    for secret in secrets:
        if secret and len(secret) > 6:
            redacted = redacted.replace(secret, "[redacted]")
    return redacted[:500]


def _append_token_row(workbook, token_row: OrderedDict[str, Any]) -> None:
    worksheet = workbook.worksheet("fitbit_oauth_tokens")
    headers = [str(header or "").strip() for header in worksheet.row_values(1)]
    if not headers:
        headers = TOKEN_HEADERS
        worksheet.append_row(headers)
    missing = [header for header in TOKEN_HEADERS if header not in headers]
    if missing:
        headers.extend(missing)
        worksheet.resize(cols=len(headers))
        worksheet.update("1:1", [headers])
    worksheet.append_row([token_row.get(header, "") for header in headers])


def _update_legacy_fitbit_token(workbook, watch_name: str, access_token: str) -> None:
    worksheet = workbook.worksheet("fitbit")
    headers = [str(header or "").strip() for header in worksheet.row_values(1)]
    if "name" not in headers or "token" not in headers:
        return
    name_col = headers.index("name") + 1
    token_col = headers.index("token") + 1
    names = worksheet.col_values(name_col)
    for row_idx, value in enumerate(names, start=1):
        if row_idx == 1:
            continue
        if str(value).strip() == watch_name:
            worksheet.update_cell(row_idx, token_col, access_token)
            return


def _persist_refreshed_tokens(workbook, watch_name: str, previous: dict[str, Any], token_json: dict[str, Any]) -> None:
    created_at = now_ts()
    expires_in = _safe_int(token_json.get("expires_in"))
    row = OrderedDict([
        ("watchName", watch_name),
        ("fitbit_user_id", token_json.get("user_id") or previous.get("fitbit_user_id", "")),
        ("access_token", token_json.get("access_token", "")),
        ("refresh_token", token_json.get("refresh_token") or previous.get("refresh_token", "")),
        ("expires_at", str(created_at + max(expires_in - 30, 0))),
        ("scope", token_json.get("scope") or previous.get("scope", "")),
        ("created_at", str(created_at)),
    ])
    _append_token_row(workbook, row)
    if row["access_token"]:
        _update_legacy_fitbit_token(workbook, watch_name, str(row["access_token"]))


def _probe_access_token(token: str) -> tuple[int | None, str]:
    try:
        response = requests.get(
            DEVICE_URL,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=20,
        )
        return response.status_code, _sanitize_response_text(response.text, token)
    except Exception as exc:
        return None, str(exc)


def _live_probe(workbook, watch_name: str, watch_row: dict[str, Any] | None, token_row: dict[str, Any] | None) -> dict[str, Any]:
    access_token = ""
    token_source = "missing"
    if token_row and token_row.get("access_token"):
        access_token = str(token_row.get("access_token"))
        token_source = "oauth"
    elif watch_row and watch_row.get("token"):
        access_token = str(watch_row.get("token"))
        token_source = "legacy"

    result: dict[str, Any] = {
        "watchName": watch_name,
        "token_source": token_source,
        "device_status": None,
        "device_error": "",
        "refresh_attempted": False,
        "refresh_status": "",
        "refresh_persisted": False,
    }
    if not access_token:
        result["device_error"] = "missing access token"
        return result

    status, text = _probe_access_token(access_token)
    result["device_status"] = status
    if status != 200:
        result["device_error"] = text

    if status == 401 and token_row and token_row.get("refresh_token"):
        result["refresh_attempted"] = True
        refresh_token = str(token_row.get("refresh_token"))
        try:
            refreshed = refresh_tokens(refresh_token)
            _persist_refreshed_tokens(workbook, watch_name, token_row, refreshed)
            result["refresh_status"] = "success"
            result["refresh_persisted"] = True
        except FitbitOAuthError as exc:
            result["refresh_status"] = _sanitize_response_text(str(exc), refresh_token)
        except Exception as exc:
            result["refresh_status"] = _sanitize_response_text(str(exc), refresh_token)

    return result


def build_report(workbook, watch_names: list[str], live_probe: bool = False) -> dict[str, Any]:
    fitbit_ws = workbook.worksheet("fitbit")
    tokens_ws = workbook.worksheet("fitbit_oauth_tokens")
    log_ws = workbook.worksheet("log")
    fitbit_log_ws = workbook.worksheet("FitbitLog")

    fitbit_rows = _records(fitbit_ws)
    token_rows = _records(tokens_ws)
    now = now_ts()

    watches = []
    for watch_name in watch_names:
        row = _watch_row(fitbit_rows, watch_name)
        token_row = _latest_token_row(token_rows, watch_name)
        legacy_token = row.get("token", "") if row else ""
        access_token = token_row.get("access_token", "") if token_row else ""
        watch_report = {
            "watchName": watch_name,
            "fitbit_row_found": row is not None,
            "provider": (row or {}).get("provider", ""),
            "oauth_type": (row or {}).get("oauth_type", ""),
            "auth_status": (row or {}).get("auth_status", ""),
            "oauth_row_found": token_row is not None,
            "oauth_created_at": token_row.get("created_at", "") if token_row else "",
            "oauth_created_at_iso": (
                dt.datetime.fromtimestamp(_safe_int(token_row.get("created_at")), tz=dt.timezone.utc).isoformat()
                if token_row and _safe_int(token_row.get("created_at")) else ""
            ),
            "oauth_expires_at": token_row.get("expires_at", "") if token_row else "",
            "oauth_expires_in_seconds": _expires_in_seconds(token_row.get("expires_at")) if token_row else None,
            "oauth_is_expired": _safe_int(token_row.get("expires_at")) <= now if token_row else None,
            "access_jwt_exp_iso": _token_exp_iso(access_token),
            "legacy_token_present": bool(str(legacy_token or "").strip()),
            "legacy_matches_latest_oauth_access": bool(legacy_token and access_token and legacy_token == access_token),
        }
        if live_probe:
            watch_report["live_probe"] = _live_probe(workbook, watch_name, row, token_row)
        watches.append(watch_report)

    worksheets = workbook.worksheets()
    workbook_sheets = [
        {
            "title": worksheet.title,
            "allocated_rows": worksheet.row_count,
            "allocated_cols": worksheet.col_count,
            "allocated_cells": worksheet.row_count * worksheet.col_count,
        }
        for worksheet in worksheets
    ]
    allocated_cells = sum(item["allocated_cells"] for item in workbook_sheets)

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "watches": watches,
        "sheet_sizes": {
            "log": {
                "allocated_rows": log_ws.row_count,
                "allocated_cols": log_ws.col_count,
                "used_rows_from_col_a": len(log_ws.col_values(1)),
                "header_cols": len(log_ws.row_values(1)),
            },
            "FitbitLog": {
                "allocated_rows": fitbit_log_ws.row_count,
                "allocated_cols": fitbit_log_ws.col_count,
                "used_rows_from_col_a": len(fitbit_log_ws.col_values(1)),
                "header_cols": len(fitbit_log_ws.row_values(1)),
            },
            "workbook_allocated_cells": allocated_cells,
            "workbook_cell_limit": 10_000_000,
            "workbook_cells_remaining": 10_000_000 - allocated_cells,
            "worksheets": workbook_sheets,
        },
    }


def prune_bounded_history(workbook, sheet_names: list[str], max_rows: int) -> list[dict[str, Any]]:
    results = []
    for sheet_name in sheet_names:
        try:
            worksheet = workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            results.append({"sheet": sheet_name, "status": "missing"})
            continue

        before = {
            "allocated_rows": worksheet.row_count,
            "allocated_cols": worksheet.col_count,
            "used_rows_from_col_a": len(worksheet.col_values(1)),
        }
        GoogleSheetsAdapter._prune_worksheet_to_recent_rows(
            worksheet,
            max_rows=max_rows,
            incoming_count=0,
        )
        worksheet = workbook.worksheet(sheet_name)
        after = {
            "allocated_rows": worksheet.row_count,
            "allocated_cols": worksheet.col_count,
            "used_rows_from_col_a": len(worksheet.col_values(1)),
        }
        results.append({
            "sheet": sheet_name,
            "status": "pruned",
            "before": before,
            "after": after,
        })
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose Fitbit OAuth and Sheets limits")
    parser.add_argument("--spreadsheet-key", default=None)
    parser.add_argument("--watch", action="append", dest="watches", default=None)
    parser.add_argument("--live-probe", action="store_true")
    parser.add_argument("--prune-bounded-history", action="store_true")
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    key = _spreadsheet_key(args.spreadsheet_key)
    workbook = _client().open_by_key(key)
    prune_results = []
    if args.prune_bounded_history:
        prune_results = prune_bounded_history(
            workbook,
            sheet_names=DEFAULT_BOUNDED_SHEETS,
            max_rows=args.max_rows,
        )
    report = build_report(workbook, args.watches or DEFAULT_WATCHES, live_probe=args.live_probe)
    if prune_results:
        report["prune_results"] = prune_results

    if args.as_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(f"Generated at: {report['generated_at']}")
        print("\nWatches")
        for watch in report["watches"]:
            print(json.dumps(watch, indent=2, ensure_ascii=False))
        print("\nSheet sizes")
        print(json.dumps(report["sheet_sizes"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
