#!/usr/bin/env python3
"""Migrate OAuth credentials from Sheets to Google Secret Manager.

The command is a dry run unless ``--apply`` is provided. Secret values are never
printed. Use ``--clear-plaintext`` only after readback verification succeeds.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from entity.Sheet import Spreadsheet
from model.config import get_secrets
from utils.secret_store import load_json_secret, secret_manager_enabled, store_json_secret


@dataclass
class MigrationCount:
    candidates: int = 0
    migrated: int = 0
    skipped: int = 0


def _ensure_headers(ws, required: list[str]) -> list[str]:
    headers = [str(item or "").strip() for item in ws.row_values(1)]
    missing = [item for item in required if item not in headers]
    if missing:
        headers += missing
        ws.resize(cols=len(headers))
        ws.update("1:1", [headers])
    return headers


def _update_row(ws, headers: list[str], row_number: int, values: dict[str, str]) -> None:
    for key, value in values.items():
        if key in headers:
            ws.update_cell(row_number, headers.index(key) + 1, value)


def _verified_store(kind: str, identifiers: list[str], payload: dict, existing_ref: str = "") -> str:
    ref = store_json_secret(kind, identifiers, payload, existing_ref=existing_ref)
    readback = load_json_secret(ref)
    for key, value in payload.items():
        if readback.get(key) != value:
            raise RuntimeError(f"Secret Manager readback verification failed for {kind}/{key}")
    return ref


def _migrate_sheet(
    workbook,
    *,
    tab: str,
    secret_kind: str,
    id_fields: list[str],
    secret_fields: list[str],
    ref_field: str,
    apply: bool,
    clear_plaintext: bool,
) -> MigrationCount:
    count = MigrationCount()
    try:
        ws = workbook.worksheet(tab)
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return count
        raise
    headers = [str(item or "").strip() for item in ws.row_values(1)]
    if apply:
        headers = _ensure_headers(ws, [ref_field])
    for row_number, row in enumerate(ws.get_all_records(), start=2):
        payload = {field: str(row.get(field) or "") for field in secret_fields}
        if not any(payload.values()):
            count.skipped += 1
            continue
        count.candidates += 1
        if not apply:
            continue
        identifiers = [str(row.get(field) or "unknown") for field in id_fields]
        ref = _verified_store(
            secret_kind,
            identifiers,
            payload,
            existing_ref=str(row.get(ref_field) or ""),
        )
        updates = {ref_field: ref}
        if clear_plaintext:
            updates.update({field: "" for field in secret_fields})
        _update_row(ws, headers, row_number, updates)
        count.migrated += 1
    return count


def _migrate_clients(workbook, *, apply: bool, clear_plaintext: bool) -> MigrationCount:
    count = MigrationCount()
    try:
        ws = workbook.worksheet("health_oauth_clients")
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return count
        raise
    headers = [str(item or "").strip() for item in ws.row_values(1)]
    if apply:
        headers = _ensure_headers(ws, ["client_secret_ref"])
    for row_number, row in enumerate(ws.get_all_records(), start=2):
        client_secret = str(row.get("client_secret") or "")
        raw = str(row.get("credentials_json_raw") or "")
        if not client_secret and raw:
            try:
                parsed = json.loads(raw)
                client_secret = str((parsed.get("web") or parsed.get("installed") or {}).get("client_secret") or "")
            except Exception:
                pass
        if not client_secret:
            count.skipped += 1
            continue
        count.candidates += 1
        if not apply:
            continue
        payload = {
            "client_id": str(row.get("client_id") or ""),
            "client_secret": client_secret,
            "auth_uri": str(row.get("auth_uri") or ""),
            "token_uri": str(row.get("token_uri") or ""),
        }
        ref = _verified_store(
            "oauth-client",
            [
                str(row.get("provider") or "google_health"),
                str(row.get("enviroment") or row.get("environment") or "production"),
                str(row.get("client_key") or "unknown"),
            ],
            payload,
            existing_ref=str(row.get("client_secret_ref") or ""),
        )
        updates = {"client_secret_ref": ref}
        if clear_plaintext:
            updates.update({"client_secret": "", "credentials_json_raw": ""})
        _update_row(ws, headers, row_number, updates)
        count.migrated += 1
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write verified Secret Manager references")
    parser.add_argument(
        "--clear-plaintext",
        action="store_true",
        help="Blank plaintext cells after successful Secret Manager readback",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.clear_plaintext and not args.apply:
        raise SystemExit("--clear-plaintext requires --apply")
    if args.apply and not secret_manager_enabled():
        raise SystemExit("Set SECRET_MANAGER_ENABLED=true before applying the migration")

    secrets = get_secrets()
    key = str(secrets.get("spreadsheet_key") or "")
    if not key:
        raise SystemExit("Missing spreadsheet_key")
    workbook = Spreadsheet(name="Fitbit Database", api_key=key).get_gspread_connection()

    results = {
        "health_oauth_clients": _migrate_clients(
            workbook, apply=args.apply, clear_plaintext=args.clear_plaintext
        ),
        "health_oauth_tokens": _migrate_sheet(
            workbook,
            tab="health_oauth_tokens",
            secret_kind="participant-oauth",
            id_fields=["provider", "project", "watchName", "token_id"],
            secret_fields=["access_token", "refresh_token"],
            ref_field="token_secret_ref",
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
        "fitbit_oauth_tokens": _migrate_sheet(
            workbook,
            tab="fitbit_oauth_tokens",
            secret_kind="participant-oauth",
            id_fields=["watchName"],
            secret_fields=["access_token", "refresh_token"],
            ref_field="token_secret_ref",
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
        "fitbit_registry": _migrate_sheet(
            workbook,
            tab="fitbit",
            secret_kind="legacy-fitbit-token",
            id_fields=["project", "name"],
            secret_fields=["token"],
            ref_field="token_secret_ref",
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
    }
    mode = "APPLIED" if args.apply else "DRY RUN"
    print(mode)
    for tab, count in results.items():
        print(
            f"{tab}: candidates={count.candidates}, migrated={count.migrated}, skipped={count.skipped}"
        )
    if not args.apply:
        print("Re-run with --apply, then --apply --clear-plaintext after review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
