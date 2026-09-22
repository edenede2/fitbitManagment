#!/usr/bin/env python3
"""Idempotently migrate sanitized operational records from Sheets to Firestore.

The command is a dry run unless ``--apply`` is supplied.  OAuth/client secret
values are excluded unconditionally.  Existing Sheets records are never edited
or deleted by this command.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gspread

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from entity.Sheet import Spreadsheet
from model.config import get_secrets
from utils.firestore_schema import (
    GLOBAL_SECRET_FIELDS,
    MigrationPlan,
    build_migration_plan,
    canonical_documents_hash,
    specs_for_profile,
)
from utils.firestore_store import GoogleFirestoreStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_rows(workbook, sheet_name: str) -> list[dict[str, Any]] | None:
    try:
        worksheet = workbook.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return None
    try:
        return worksheet.get_all_records()
    except gspread.exceptions.GSpreadException:
        values = worksheet.get_all_values()
        if not values:
            return []
        headers = [str(value or "").strip() for value in values[0]]
        return [
            {
                header: row[index] if index < len(row) else ""
                for index, header in enumerate(headers)
                if header
            }
            for row in values[1:]
            if any(str(value or "").strip() for value in row)
        ]


def _has_forbidden_fields(documents: dict[str, dict[str, Any]]) -> list[str]:
    forbidden = {field.casefold() for field in GLOBAL_SECRET_FIELDS}
    found: set[str] = set()
    for document in documents.values():
        for field in document:
            if field.casefold() in forbidden:
                found.add(field)
    return sorted(found)


def _verify_plan(store: GoogleFirestoreStore, plan: MigrationPlan) -> tuple[bool, str]:
    actual = store.read_documents(
        plan.spec.collection,
        source_sheet=plan.spec.source_sheet,
    )
    forbidden = _has_forbidden_fields(actual)
    expected_hash = canonical_documents_hash(plan.documents)
    actual_hash = canonical_documents_hash(actual)
    ok = (
        len(actual) == len(plan.documents)
        and expected_hash == actual_hash
        and not forbidden
    )
    detail = (
        f"expected={len(plan.documents)} actual={len(actual)} "
        f"hash_match={expected_hash == actual_hash} "
        f"forbidden_fields={','.join(forbidden) if forbidden else 'none'}"
    )
    return ok, detail


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write sanitized documents")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Do not write; compare Sheets with the existing Firestore import",
    )
    parser.add_argument(
        "--profile",
        choices=("core", "all"),
        default="core",
        help="Core omits high-volume historical API/webhook logs",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated source sheet names",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.apply and args.verify_only:
        raise SystemExit("Choose either --apply or --verify-only")

    secrets = get_secrets()
    spreadsheet_key = str(secrets.get("spreadsheet_key") or "").strip()
    if not spreadsheet_key:
        raise SystemExit("Missing spreadsheet_key")
    spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
    workbook = spreadsheet.get_gspread_connection()
    only = {item.strip() for item in args.only.split(",") if item.strip()} or None
    specs = specs_for_profile(args.profile, only=only)
    migrated_at = _utc_now()

    plans: list[MigrationPlan] = []
    for spec in specs:
        rows = _load_rows(workbook, spec.source_sheet)
        if rows is None:
            print(f"{spec.source_sheet}: missing (skipped)")
            continue
        plan = build_migration_plan(spec, rows, migrated_at=migrated_at)
        forbidden = _has_forbidden_fields(plan.documents)
        if forbidden:
            raise RuntimeError(
                f"Secret-field guard failed for {spec.source_sheet}: {', '.join(forbidden)}"
            )
        plans.append(plan)
        print(
            f"{spec.source_sheet} -> {spec.collection}: input={plan.input_rows} "
            f"documents={len(plan.documents)} excluded={plan.excluded_rows} "
            f"compacted={plan.compacted_rows} "
            f"secret_values_omitted={plan.redacted_nonempty_fields}"
        )

    if not args.apply and not args.verify_only:
        print(
            f"DRY RUN: {len(plans)} collections, "
            f"{sum(len(plan.documents) for plan in plans)} documents. "
            "Re-run with --apply to write Firestore."
        )
        return 0

    store = GoogleFirestoreStore.from_environment()
    run_id = str(uuid.uuid4())
    failures: list[str] = []
    written = 0

    if args.apply:
        for plan in plans:
            count = store.write_documents(plan.spec.collection, plan.documents)
            written += count
            print(f"WROTE {plan.spec.collection}: {count}")

    for plan in plans:
        ok, detail = _verify_plan(store, plan)
        print(f"{'VERIFIED' if ok else 'MISMATCH'} {plan.spec.collection}: {detail}")
        if not ok:
            failures.append(plan.spec.collection)

    if args.apply:
        store.write_migration_run(
            run_id,
            {
                "run_id": run_id,
                "version": "sheets-to-firestore-v1",
                "profile": args.profile,
                "started_and_finished_at": migrated_at,
                "collections": [plan.spec.collection for plan in plans],
                "documents_written": written,
                "status": "verified" if not failures else "mismatch",
                "mismatches": failures,
            },
        )

    if failures:
        print("Migration verification failed: " + ", ".join(failures), file=sys.stderr)
        return 1
    print(
        f"{'APPLIED AND VERIFIED' if args.apply else 'VERIFIED'}: "
        f"{len(plans)} collections, {sum(len(plan.documents) for plan in plans)} documents"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
