#!/usr/bin/env python3
"""Migrate active OAuth credentials from Sheets to Google Secret Manager.

The command is a dry run unless ``--apply`` is provided. Secret values are never
printed. For append-only token tables, only the latest active row in each
participant/provider group receives the Secret Manager reference. When
``--clear-plaintext`` is used, plaintext is removed from every identified row in
that group only after the active secret has been stored and read back.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from gspread.utils import rowcol_to_a1

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from entity.Sheet import Spreadsheet
from model.config import get_secrets
from utils.secret_store import load_json_secret, secret_manager_enabled, store_json_secret


_SECRET_REF_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/secrets/(?P<secret>[A-Za-z0-9_-]+)"
    r"(?:/versions/(?:latest|[0-9]+))?$"
)


@dataclass
class MigrationCount:
    candidates: int = 0
    groups: int = 0
    migrated: int = 0
    cleared: int = 0
    inactive: int = 0
    unresolved: int = 0
    skipped: int = 0
    invalid_refs: int = 0


@dataclass(frozen=True)
class SheetRow:
    number: int
    values: dict[str, Any]


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _ensure_headers(ws, required: list[str]) -> list[str]:
    headers = [_text(item) for item in ws.row_values(1)]
    missing = [item for item in required if item not in headers]
    if missing:
        headers += missing
        ws.resize(cols=len(headers))
        ws.update("1:1", [headers])
    return headers


def _queue_updates(
    pending: dict[int, dict[str, str]],
    row_number: int,
    values: dict[str, str],
) -> None:
    pending.setdefault(row_number, {}).update(values)


def _apply_updates(
    ws,
    headers: list[str],
    pending: dict[int, dict[str, str]],
    *,
    batch_size: int = 500,
) -> None:
    cells: list[dict[str, Any]] = []
    for row_number in sorted(pending):
        for key, value in pending[row_number].items():
            if key not in headers:
                continue
            column = headers.index(key) + 1
            cells.append({"range": rowcol_to_a1(row_number, column), "values": [[value]]})
    for start in range(0, len(cells), batch_size):
        ws.batch_update(cells[start : start + batch_size], raw=True)


def _payload_matches(readback: dict[str, Any], payload: dict[str, str]) -> bool:
    return all(_text(readback.get(key)) == _text(value) for key, value in payload.items())


def _usable_secret_ref(value: Any) -> bool:
    """Accept only canonical Secret Manager resource names for this project."""
    match = _SECRET_REF_RE.fullmatch(_text(value))
    if not match:
        return False
    expected_project = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    return not expected_project or match.group("project") == expected_project


def _migration_secret_ref(value: Any, count: MigrationCount | None = None) -> str:
    """Drop malformed placeholders without ever treating them as secret names."""
    ref = _text(value)
    if not ref:
        return ""
    if _usable_secret_ref(ref):
        return ref
    if count is not None:
        count.invalid_refs += 1
    return ""


def _verified_store(
    kind: str,
    identifiers: list[str],
    payload: dict[str, str],
    existing_ref: str = "",
) -> str:
    """Reuse a matching secret version; otherwise store and verify a new version."""
    existing_ref = _migration_secret_ref(existing_ref)
    if existing_ref:
        readback = load_json_secret(existing_ref)
        if _payload_matches(readback, payload):
            return existing_ref
    ref = store_json_secret(kind, identifiers, payload, existing_ref=existing_ref)
    readback = load_json_secret(ref)
    if not _payload_matches(readback, payload):
        raise RuntimeError(f"Secret Manager readback verification failed for {kind}")
    return ref


def _has_plaintext(row: dict[str, Any], fields: list[str]) -> bool:
    return any(_text(row.get(field)) for field in fields)


def _latest_payload(rows: list[SheetRow], fields: list[str]) -> dict[str, str]:
    """Use the newest non-empty value for each field in an active row group."""
    payload: dict[str, str] = {}
    for field in fields:
        payload[field] = next(
            (_text(item.values.get(field)) for item in reversed(rows) if _text(item.values.get(field))),
            "",
        )
    return payload


def _latest_reference(rows: list[SheetRow], ref_field: str) -> str:
    return next(
        (_text(item.values.get(ref_field)) for item in reversed(rows) if _text(item.values.get(ref_field))),
        "",
    )


def _migrate_grouped_sheet(
    workbook,
    *,
    tab: str,
    secret_kind: str,
    identity_fields: list[str],
    required_identity_fields: list[str],
    identity_prefix: list[str] | None = None,
    secret_fields: list[str],
    ref_field: str,
    active_predicate: Callable[[dict[str, Any]], bool],
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

    headers = [_text(item) for item in ws.row_values(1)]
    if apply:
        headers = _ensure_headers(ws, [ref_field])
    records = [SheetRow(number, row) for number, row in enumerate(ws.get_all_records(), start=2)]
    groups: dict[tuple[str, ...], list[SheetRow]] = defaultdict(list)

    for item in records:
        if not _has_plaintext(item.values, secret_fields):
            count.skipped += 1
        identity = tuple(_text(item.values.get(field)) for field in identity_fields)
        missing_required = any(
            not _text(item.values.get(field)) for field in required_identity_fields
        )
        if missing_required:
            if _has_plaintext(item.values, secret_fields):
                count.candidates += 1
                count.unresolved += 1
            continue
        groups[identity].append(item)

    pending: dict[int, dict[str, str]] = {}
    for identity, rows in groups.items():
        plaintext_rows = [item for item in rows if _has_plaintext(item.values, secret_fields)]
        if not plaintext_rows:
            continue
        count.candidates += len(plaintext_rows)
        count.groups += 1
        active_rows = [item for item in rows if active_predicate(item.values)]

        if not active_rows:
            count.inactive += 1
            if apply and clear_plaintext:
                for item in plaintext_rows:
                    _queue_updates(
                        pending,
                        item.number,
                        {field: "" for field in secret_fields},
                    )
                count.cleared += len(plaintext_rows)
            continue

        selected = active_rows[-1]
        payload = _latest_payload(active_rows, secret_fields)
        existing_ref = _migration_secret_ref(
            _latest_reference(active_rows, ref_field),
            count,
        )
        if not any(payload.values()):
            # The active row can already be reference-only while older inactive
            # history still contains plaintext. Verify that reference before
            # allowing the history to be cleared.
            if not existing_ref:
                count.unresolved += len(plaintext_rows)
                continue
            if apply:
                readback = load_json_secret(existing_ref)
                if not any(_text(readback.get(field)) for field in secret_fields):
                    raise RuntimeError(f"Secret {existing_ref} has no {tab} token values")
            ref = existing_ref
        elif apply:
            identifiers = [*(identity_prefix or []), *identity]
            ref = _verified_store(
                secret_kind,
                identifiers,
                payload,
                existing_ref=existing_ref,
            )
        else:
            ref = existing_ref

        if apply:
            _queue_updates(pending, selected.number, {ref_field: ref})
            count.migrated += 1
            if clear_plaintext:
                for item in plaintext_rows:
                    _queue_updates(
                        pending,
                        item.number,
                        {field: "" for field in secret_fields},
                    )
                count.cleared += len(plaintext_rows)

    if apply and pending:
        _apply_updates(ws, headers, pending)
    return count


def _is_health_active(row: dict[str, Any]) -> bool:
    value = row.get("is_active")
    return _text("TRUE" if value in (None, "") else value).upper() == "TRUE"


def _is_fitbit_active(row: dict[str, Any]) -> bool:
    return _text(row.get("status") or "connected").casefold() == "connected"


def _is_registry_active(row: dict[str, Any]) -> bool:
    value = row.get("isActive")
    if value in (None, ""):
        value = row.get("is_active")
    return _text("TRUE" if value in (None, "") else value).upper() == "TRUE"


def _client_secret(row: dict[str, Any]) -> str:
    secret = _text(row.get("client_secret"))
    raw = _text(row.get("credentials_json_raw"))
    if secret or not raw:
        return secret
    try:
        parsed = json.loads(raw)
        return _text((parsed.get("web") or parsed.get("installed") or {}).get("client_secret"))
    except Exception:
        return ""


def _migrate_clients(workbook, *, apply: bool, clear_plaintext: bool) -> MigrationCount:
    count = MigrationCount()
    try:
        ws = workbook.worksheet("health_oauth_clients")
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return count
        raise
    headers = [_text(item) for item in ws.row_values(1)]
    if apply:
        headers = _ensure_headers(ws, ["client_secret_ref"])
    pending: dict[int, dict[str, str]] = {}

    for row_number, row in enumerate(ws.get_all_records(), start=2):
        raw = _text(row.get("credentials_json_raw"))
        client_secret = _client_secret(row)
        if not client_secret and not raw:
            count.skipped += 1
            continue
        count.candidates += 1
        identity = [
            _text(row.get("provider") or "google_health"),
            _text(row.get("enviroment") or row.get("environment") or "production"),
            _text(row.get("client_key")),
        ]
        if not identity[-1] or not client_secret:
            count.unresolved += 1
            continue
        status = _text(row.get("status") or "active").casefold()
        if status != "active":
            count.inactive += 1
            if apply and clear_plaintext:
                _queue_updates(
                    pending,
                    row_number,
                    {"client_secret": "", "credentials_json_raw": ""},
                )
                count.cleared += 1
            continue

        count.groups += 1
        payload = {
            "client_id": _text(row.get("client_id")),
            "client_secret": client_secret,
            "auth_uri": _text(row.get("auth_uri")),
            "token_uri": _text(row.get("token_uri")),
        }
        if apply:
            ref = _verified_store(
                "oauth-client",
                identity,
                payload,
                existing_ref=_text(row.get("client_secret_ref")),
            )
            updates = {"client_secret_ref": ref}
            if clear_plaintext:
                updates.update({"client_secret": "", "credentials_json_raw": ""})
                count.cleared += 1
            _queue_updates(pending, row_number, updates)
            count.migrated += 1

    if apply and pending:
        _apply_updates(ws, headers, pending)
    return count


def _active_fitbit_registry_names(workbook) -> set[str]:
    try:
        rows = workbook.worksheet("fitbit").get_all_records()
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return set()
        raise
    return {
        _text(row.get("name"))
        for row in rows
        if _text(row.get("name")) and _is_registry_active(row)
    }


def _fitbit_oauth_groups(workbook) -> dict[str, list[SheetRow]]:
    try:
        rows = workbook.worksheet("fitbit_oauth_tokens").get_all_records()
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return {}
        raise
    groups: dict[str, list[SheetRow]] = defaultdict(list)
    for row_number, row in enumerate(rows, start=2):
        watch_name = _text(row.get("watchName"))
        if watch_name and _is_fitbit_active(row):
            groups[watch_name].append(SheetRow(row_number, row))
    return groups


def _migrate_fitbit_registry(
    workbook,
    *,
    apply: bool,
    clear_plaintext: bool,
) -> MigrationCount:
    """Link registry rows to OAuth secrets and migrate only legacy-only watches."""
    count = MigrationCount()
    try:
        ws = workbook.worksheet("fitbit")
    except Exception as exc:
        if "not found" in str(exc).casefold():
            return count
        raise
    headers = [_text(item) for item in ws.row_values(1)]
    if apply:
        headers = _ensure_headers(ws, ["token_secret_ref"])
    records = [SheetRow(number, row) for number, row in enumerate(ws.get_all_records(), start=2)]
    groups: dict[str, list[SheetRow]] = defaultdict(list)
    for item in records:
        if not _has_plaintext(item.values, ["token"]):
            count.skipped += 1
        watch_name = _text(item.values.get("name"))
        if not watch_name:
            if _has_plaintext(item.values, ["token"]):
                count.candidates += 1
                count.unresolved += 1
            continue
        groups[watch_name].append(item)

    oauth_groups = _fitbit_oauth_groups(workbook)
    pending: dict[int, dict[str, str]] = {}
    for watch_name, rows in groups.items():
        plaintext_rows = [item for item in rows if _has_plaintext(item.values, ["token"])]
        if not plaintext_rows:
            continue
        count.candidates += len(plaintext_rows)
        count.groups += 1
        active_rows = [item for item in rows if _is_registry_active(item.values)]
        if not active_rows:
            count.inactive += 1
            if apply and clear_plaintext:
                for item in plaintext_rows:
                    _queue_updates(pending, item.number, {"token": ""})
                count.cleared += len(plaintext_rows)
            continue

        selected = active_rows[-1]
        existing_ref = _migration_secret_ref(
            _latest_reference(active_rows, "token_secret_ref"),
            count,
        )
        oauth_rows = oauth_groups.get(watch_name, [])
        ref = ""
        if oauth_rows:
            ref = _migration_secret_ref(
                _latest_reference(oauth_rows, "token_secret_ref"),
                count,
            )
        if not ref:
            payload = _latest_payload(active_rows, ["token"])
            if not payload["token"]:
                count.unresolved += len(plaintext_rows)
                continue
            if apply:
                ref = _verified_store(
                    "participant-oauth",
                    ["fitbit", watch_name],
                    {"access_token": payload["token"], "refresh_token": ""},
                    existing_ref=existing_ref,
                )
            else:
                ref = existing_ref

        if apply:
            _queue_updates(pending, selected.number, {"token_secret_ref": ref})
            count.migrated += 1
            if clear_plaintext:
                for item in plaintext_rows:
                    _queue_updates(pending, item.number, {"token": ""})
                count.cleared += len(plaintext_rows)

    if apply and pending:
        _apply_updates(ws, headers, pending)
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write verified Secret Manager references")
    parser.add_argument(
        "--clear-plaintext",
        action="store_true",
        help="Blank all identified plaintext rows after active-secret readback verification",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.clear_plaintext and not args.apply:
        raise SystemExit("--clear-plaintext requires --apply")
    if args.apply and not secret_manager_enabled():
        raise SystemExit("Set SECRET_MANAGER_ENABLED=true before applying the migration")

    secrets = get_secrets()
    key = _text(secrets.get("spreadsheet_key"))
    if not key:
        raise SystemExit("Missing spreadsheet_key")
    workbook = Spreadsheet(name="Fitbit Database", api_key=key).get_gspread_connection()
    active_fitbit_watches = _active_fitbit_registry_names(workbook)

    results = {
        "health_oauth_clients": _migrate_clients(
            workbook, apply=args.apply, clear_plaintext=args.clear_plaintext
        ),
        "health_oauth_tokens": _migrate_grouped_sheet(
            workbook,
            tab="health_oauth_tokens",
            secret_kind="participant-oauth",
            identity_fields=["provider", "project", "watchName"],
            required_identity_fields=["provider", "watchName"],
            secret_fields=["access_token", "refresh_token"],
            ref_field="token_secret_ref",
            active_predicate=_is_health_active,
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
        "fitbit_oauth_tokens": _migrate_grouped_sheet(
            workbook,
            tab="fitbit_oauth_tokens",
            secret_kind="participant-oauth",
            identity_fields=["watchName"],
            required_identity_fields=["watchName"],
            identity_prefix=["fitbit"],
            secret_fields=["access_token", "refresh_token"],
            ref_field="token_secret_ref",
            active_predicate=lambda row: (
                _is_fitbit_active(row)
                and _text(row.get("watchName")) in active_fitbit_watches
            ),
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
        "fitbit_registry": _migrate_fitbit_registry(
            workbook,
            apply=args.apply,
            clear_plaintext=args.clear_plaintext,
        ),
    }
    mode = "APPLIED" if args.apply else "DRY RUN"
    print(mode)
    for tab, count in results.items():
        print(
            f"{tab}: plaintext_rows={count.candidates}, groups={count.groups}, "
            f"referenced={count.migrated}, cleared={count.cleared}, "
            f"inactive_groups={count.inactive}, unresolved_rows={count.unresolved}, "
            f"skipped_rows={count.skipped}, invalid_refs={count.invalid_refs}"
        )
    if not args.apply:
        print("Re-run with --apply, verify live OAuth/refresh, then use --apply --clear-plaintext.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
