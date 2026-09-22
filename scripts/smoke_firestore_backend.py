#!/usr/bin/env python3
"""Read-only smoke test for the live Firestore repository path.

This deliberately disables the Sheets fallback for the process so a passing
result proves the records came from Firestore.  It never prints record values.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from entity.Sheet import GoogleSheetsAdapter, Spreadsheet
from model.config import get_secrets
from utils.firestore_schema import GLOBAL_SECRET_FIELDS


DEFAULT_TABS = (
    "user",
    "fitbit",
    "health_oauth_clients",
    "health_oauth_tokens",
    "fitbit_oauth_tokens",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--watch",
        default="",
        help="Optionally require a device record with this exact watch name.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    os.environ["DATA_BACKEND"] = "firestore"
    os.environ["SHEETS_READ_FALLBACK"] = "false"

    secrets = get_secrets()
    spreadsheet_key = str(secrets.get("spreadsheet_key") or "").strip()
    if not spreadsheet_key:
        raise SystemExit("Missing spreadsheet_key")

    spreadsheet = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
    if spreadsheet.source_kind != "firestore":
        raise SystemExit("Primary spreadsheet did not select the Firestore backend")

    forbidden = {field.casefold() for field in GLOBAL_SECRET_FIELDS}
    failures: list[str] = []
    for tab in DEFAULT_TABS:
        rows = GoogleSheetsAdapter.get_all_reords(spreadsheet, tab)
        leaked_fields = sorted(
            {
                str(field)
                for row in rows
                for field in row
                if str(field).casefold() in forbidden
            }
        )
        if leaked_fields:
            failures.append(f"{tab}: forbidden secret fields present")
        if tab in {"user", "fitbit"} and not rows:
            failures.append(f"{tab}: expected at least one record")
        print(f"{tab}: records={len(rows)} secret_fields=none" if not leaked_fields else f"{tab}: FAILED")

    if args.watch:
        matches = GoogleSheetsAdapter.get_rows(
            spreadsheet,
            "fitbit",
            "name",
            name=args.watch,
        )
        if not matches:
            failures.append("requested watch was not found")
        print(f"requested_watch: {'found' if matches else 'missing'}")

    if failures:
        for failure in failures:
            print(f"FAILED: {failure}")
        return 1
    print("Firestore backend smoke test passed without Sheets fallback")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
