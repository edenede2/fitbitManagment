#!/usr/bin/env python3
import unittest
from types import SimpleNamespace

import pandas as pd
import polars as pl

from entity.Sheet import GoogleSheetsAdapter
from firbitfilesOrgenizer import _normalize_active_watches_for_polars


class FakeWorksheet:
    title = "FitbitLog"

    def __init__(self, used_rows=1):
        self.used_rows = used_rows
        self.row_count = used_rows + 100
        self.deleted = []
        self.resized_to = None

    def col_values(self, col):
        return ["header"] * self.used_rows

    def delete_rows(self, start, end):
        self.deleted.append((start, end))

    def resize(self, rows=None, cols=None):
        self.resized_to = rows


class AppendFailWorksheet(FakeWorksheet):
    def row_values(self, row):
        return ["watchName", "lastCheck"]

    def append_rows(self, rows):
        raise RuntimeError("cell limit")


class FakeWorkbook:
    def __init__(self, worksheet):
        self._worksheet = worksheet

    def worksheet(self, name):
        return self._worksheet


class SheetLimitAndOrganizerTests(unittest.TestCase):
    def test_normalize_active_watches_handles_mixed_types_for_polars(self):
        df = pd.DataFrame({
            "name": ["CWT_093", "CWTt_093"],
            "token": [12345, "token-text"],
            "isActive": [True, "TRUE"],
        })

        normalized = _normalize_active_watches_for_polars(df)
        polars_df = pl.from_pandas(normalized)

        self.assertEqual(polars_df.schema["token"], pl.String)
        self.assertEqual(polars_df.schema["isActive"], pl.String)

    def test_prune_worksheet_to_recent_rows_keeps_room_for_incoming_rows(self):
        worksheet = FakeWorksheet(used_rows=1501)

        GoogleSheetsAdapter._prune_worksheet_to_recent_rows(
            worksheet,
            max_rows=1000,
            incoming_count=100,
        )

        self.assertEqual(worksheet.deleted, [(2, 601)])
        self.assertEqual(worksheet.resized_to, 1001)

    def test_recent_row_limit_defaults_to_history_sheets_only(self):
        self.assertEqual(GoogleSheetsAdapter._recent_row_limit("FitbitLog"), 1000)
        self.assertEqual(GoogleSheetsAdapter._recent_row_limit("suspicious_nums"), 1000)
        self.assertIsNone(GoogleSheetsAdapter._recent_row_limit("log"))

    def test_save_returns_false_when_fitbitlog_append_fails(self):
        worksheet = AppendFailWorksheet()
        spreadsheet = SimpleNamespace(
            sheets={
                "FitbitLog": SimpleNamespace(
                    data=[{"watchName": "CWT_093", "lastCheck": "2026-07-14 09:00:00"}]
                )
            },
            get_gspread_connection=lambda: FakeWorkbook(worksheet),
        )

        result = GoogleSheetsAdapter.save(spreadsheet, "FitbitLog", mode="append")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
