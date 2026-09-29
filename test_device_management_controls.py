import unittest
from unittest.mock import patch

import pandas as pd
import polars as pl

from view.fitbit_management import (
    DEVICE_LEADING_COLUMNS,
    _apply_activation_metadata,
    _prepare_editor_dataframe,
)


class DeviceManagementControlTests(unittest.TestCase):
    def test_requested_columns_are_first_and_controls_are_boolean(self):
        frame = pl.DataFrame(
            [{
                "project": "Yoga",
                "name": "YN4",
                "token_secret_ref": "secret-ref",
                "isActive": "TRUE",
            }]
        )
        prepared = _prepare_editor_dataframe(frame)
        self.assertEqual(prepared.columns[:5], DEVICE_LEADING_COLUMNS)
        self.assertEqual(prepared.schema["isActive"], pl.Boolean)
        self.assertEqual(prepared.schema["alertsMuted"], pl.Boolean)

    def test_reactivation_stamps_date_and_mute_is_saved(self):
        edited = pd.DataFrame(
            [{
                "project": "Yoga",
                "name": "YN4",
                "isActive": True,
                "alertsMuted": True,
                "lastActivatedDate": "",
            }]
        )
        original = [{"project": "Yoga", "name": "YN4", "isActive": "FALSE"}]
        with patch("view.fitbit_management._today_iso", return_value="2026-09-29"):
            result = _apply_activation_metadata(edited, original)
        self.assertEqual(result.loc[0, "lastActivatedDate"], "2026-09-29")
        self.assertEqual(result.loc[0, "isActive"], "TRUE")
        self.assertEqual(result.loc[0, "alertsMuted"], "TRUE")

    def test_existing_active_watch_does_not_get_an_invented_date(self):
        edited = pd.DataFrame(
            [{
                "project": "Yoga",
                "name": "YN4",
                "isActive": True,
                "alertsMuted": False,
                "lastActivatedDate": "",
            }]
        )
        original = [{"project": "Yoga", "name": "YN4", "isActive": "TRUE"}]
        result = _apply_activation_metadata(edited, original)
        self.assertEqual(result.loc[0, "lastActivatedDate"], "")


if __name__ == "__main__":
    unittest.main()
