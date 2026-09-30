import unittest
from unittest.mock import Mock, patch

import polars as pl

from view.alerts_config import save_fitbit_config


class AlertConfigurationSaveTests(unittest.TestCase):
    def test_save_aligns_firestore_columns_by_name(self):
        # Firestore does not promise the schema order used by the form. This
        # reproduces the production error where the first columns were email and
        # project respectively.
        existing = pl.DataFrame(
            {
                "email": ["old@example.org"],
                "project": ["Yoga"],
                "manager": ["old@example.org"],
                "watch": ["YN3"],
                "currentSyncThr": ["3"],
                "endDate": ["2026-10-30"],
            }
        )
        sheet = Mock()
        sheet.to_dataframe.return_value = existing
        spreadsheet = Mock()
        spreadsheet.get_sheet.return_value = sheet

        new_config = {
            "project": "Yoga",
            "currentSyncThr": 4,
            "totalSyncThr": 10,
            "currentHrThr": 3,
            "totalHrThr": 10,
            "currentSleepThr": 3,
            "totalSleepThr": 10,
            "currentStepsThr": 3,
            "totalStepsThr": 10,
            "batteryThr": 20,
            "manager": "new@example.org",
            "email": "new@example.org",
            "watch": "YN4",
            "endDate": "2026-10-30",
        }

        with patch("view.alerts_config.require_write_access"), patch(
            "view.alerts_config.GoogleSheetsAdapter.save", return_value=True
        ):
            self.assertTrue(save_fitbit_config(spreadsheet, new_config))

        saved = spreadsheet.update_sheet.call_args.args[1]
        self.assertEqual(saved.height, 2)
        self.assertEqual(saved.filter(pl.col("watch") == "YN4").height, 1)
        row = saved.filter(pl.col("watch") == "YN4").to_dicts()[0]
        self.assertEqual(row["project"], "Yoga")
        self.assertEqual(row["email"], "new@example.org")
        self.assertEqual(row["currentSyncThr"], "4")
        self.assertIsNone(saved.filter(pl.col("watch") == "YN3")["batteryThr"][0])

    def test_save_replaces_matching_configuration_without_duplicate(self):
        existing_config = {
            "email": "manager@example.org",
            "project": "Yoga",
            "manager": "manager@example.org",
            "watch": "YN4",
            "currentSyncThr": "3",
            "batteryThr": "20",
            "endDate": "2026-10-30",
        }
        sheet = Mock()
        sheet.to_dataframe.return_value = pl.DataFrame([existing_config])
        spreadsheet = Mock()
        spreadsheet.get_sheet.return_value = sheet
        replacement = {
            **existing_config,
            "project": "Yoga",
            "currentSyncThr": 5,
        }

        with patch("view.alerts_config.require_write_access"), patch(
            "view.alerts_config.GoogleSheetsAdapter.save", return_value=True
        ):
            save_fitbit_config(spreadsheet, replacement)

        saved = spreadsheet.update_sheet.call_args.args[1]
        self.assertEqual(saved.height, 1)
        self.assertEqual(saved["currentSyncThr"][0], "5")


if __name__ == "__main__":
    unittest.main()
