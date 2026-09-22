#!/usr/bin/env python3
"""Verify the current sanitized Sheets-to-Firestore import without writing."""

from __future__ import annotations

import sys

from migrate_sheets_to_firestore import main


if __name__ == "__main__":
    raise SystemExit(main(["--verify-only", *sys.argv[1:]]))
