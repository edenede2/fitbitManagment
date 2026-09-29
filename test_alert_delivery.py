import datetime
import os
import unittest
from unittest.mock import Mock, patch

import polars as pl

import run_data_collection
from services.alert_delivery import (
    alert_is_due,
    detected_state,
    next_interval_hours,
    resolved_state,
    sent_state,
)


UTC = datetime.timezone.utc


class AlertScheduleTests(unittest.TestCase):
    def test_backoff_grows_to_24_hours_and_stays_there(self):
        self.assertEqual(
            [next_interval_hours(count) for count in range(1, 10)],
            [1, 2, 4, 8, 16, 24, 24, 24, 24],
        )

    def test_first_delivery_is_immediate_then_uses_previous_send_time(self):
        now = datetime.datetime(2026, 9, 29, 8, 0, tzinfo=UTC)
        state = detected_state(
            None,
            project="Yoga",
            watch_name="YN4",
            reasons=["Current Sync"],
            now=now,
        )
        self.assertTrue(alert_is_due(state, now))

        expected_intervals = [1, 2, 4, 8, 16, 24, 24]
        for send_number, interval in enumerate(expected_intervals, start=1):
            state = sent_state(state, reasons=["Current Sync"], now=now)
            self.assertEqual(state["send_count"], send_number)
            self.assertEqual(state["next_interval_hours"], interval)
            self.assertFalse(alert_is_due(state, now + datetime.timedelta(hours=interval) - datetime.timedelta(seconds=1)))
            now += datetime.timedelta(hours=interval)
            self.assertTrue(alert_is_due(state, now))

    def test_resolution_resets_a_future_recurrence_to_immediate(self):
        now = datetime.datetime(2026, 9, 29, 8, 0, tzinfo=UTC)
        state = detected_state(
            None,
            project="Yoga",
            watch_name="YN4",
            reasons=["Battery (10%)"],
            now=now,
        )
        state = sent_state(state, reasons=["Battery (10%)"], now=now)
        state = resolved_state(state, now=now + datetime.timedelta(minutes=30))
        recurrence = detected_state(
            state,
            project="Yoga",
            watch_name="YN4",
            reasons=["Battery (10%)"],
            now=now + datetime.timedelta(hours=2),
        )
        self.assertEqual(recurrence["send_count"], 0)
        self.assertTrue(alert_is_due(recurrence, now + datetime.timedelta(hours=2)))


class EmailSenderTests(unittest.TestCase):
    def test_sender_uses_tls_login_and_each_clean_recipient(self):
        smtp = Mock()
        smtp_context = Mock()
        smtp_context.__enter__ = Mock(return_value=smtp)
        smtp_context.__exit__ = Mock(return_value=False)

        env = {
            "SENDER_EMAIL_ADDRESS": "alerts@example.org",
            "SENDER_EMAIL_PASSWORD": "app-password",
            "SMTP_SERVER": "smtp.example.org",
            "SMTP_PORT": "587",
        }
        with patch.dict(os.environ, env, clear=False), patch.object(
            run_data_collection, "load_runtime_config"
        ), patch.object(
            run_data_collection.smtplib, "SMTP", return_value=smtp_context
        ) as smtp_class:
            result = run_data_collection.send_email_alert(
                "one@example.org, two@example.org\u200f",
                "Fitbit alert test",
                "<p>test</p>",
            )

        self.assertTrue(result)
        self.assertEqual(smtp_class.call_count, 2)
        self.assertEqual(smtp.starttls.call_count, 2)
        self.assertEqual(smtp.login.call_count, 2)
        recipients = [call.args[1] for call in smtp.sendmail.call_args_list]
        self.assertEqual(recipients, ["one@example.org", "two@example.org"])


class AlertJobTests(unittest.TestCase):
    @staticmethod
    def _log(current_failed_sync=1):
        return pl.DataFrame(
            [{
                "project": "Yoga",
                "watchName": "YN4",
                "lastCheck": "2026-09-29 08:00:00",
                "CurrentFailedSync": current_failed_sync,
            }]
        )

    @staticmethod
    def _config():
        return pl.DataFrame(
            [{
                "project": "Yoga",
                "watch": "YN4",
                "currentSyncThr": 1,
                "manager": "manager@example.org",
                "endDate": "",
            }]
        )

    @staticmethod
    def _devices(*, muted=False):
        return pl.DataFrame(
            [{
                "project": "Yoga",
                "name": "YN4",
                "isActive": "TRUE",
                "alertsMuted": "TRUE" if muted else "FALSE",
                "currentStudent": "",
            }]
        )

    @staticmethod
    def _spreadsheet():
        user_sheet = Mock()
        user_sheet.to_dataframe.return_value = pl.DataFrame(
            schema={"name": pl.Utf8, "email": pl.Utf8}
        )
        spreadsheet = Mock()
        spreadsheet.get_sheet.return_value = user_sheet
        return spreadsheet

    def test_detected_problem_sends_now_and_persists_next_interval(self):
        spreadsheet = self._spreadsheet()
        with patch.object(
            run_data_collection.GoogleSheetsAdapter, "get_rows", return_value=[]
        ), patch.object(
            run_data_collection, "_append"
        ) as append, patch.object(
            run_data_collection, "_update_row_by_keys", return_value=True
        ) as update, patch.object(
            run_data_collection, "send_email_alert", return_value=True
        ) as send:
            result = run_data_collection.check_fitbit_alerts(
                spreadsheet,
                self._log(),
                self._config(),
                self._devices(),
            )

        send.assert_called_once()
        append.assert_called_once()
        self.assertEqual(update.call_args.kwargs["updates"]["send_count"], 1)
        self.assertEqual(update.call_args.kwargs["updates"]["next_interval_hours"], 1)
        self.assertEqual(result["Yoga"]["watches"], ["YN4"])

    def test_muted_watch_does_not_send_or_start_schedule(self):
        spreadsheet = self._spreadsheet()
        with patch.object(
            run_data_collection.GoogleSheetsAdapter, "get_rows", return_value=[]
        ), patch.object(run_data_collection, "_append") as append, patch.object(
            run_data_collection, "send_email_alert"
        ) as send:
            result = run_data_collection.check_fitbit_alerts(
                spreadsheet,
                self._log(),
                self._config(),
                self._devices(muted=True),
            )

        send.assert_not_called()
        append.assert_not_called()
        self.assertEqual(result, {})

    def test_legacy_quota_failure_cannot_skip_wearable_alert_check(self):
        frames = {
            "fitbit_alerts_config": self._config(),
            "fitbit": self._devices(),
            "FitbitLog": self._log(),
        }
        spreadsheet = Mock()
        spreadsheet.get_sheet.side_effect = lambda name, **kwargs: Mock(
            to_dataframe=Mock(return_value=frames[name])
        )

        with patch.dict(os.environ, {"SPREADSHEET_KEY": "test-sheet"}), patch.object(
            run_data_collection, "load_runtime_config"
        ), patch.object(
            run_data_collection, "Spreadsheet", return_value=spreadsheet
        ), patch.object(
            run_data_collection.GoogleSheetsAdapter, "connect"
        ), patch.object(
            run_data_collection, "get_watch_details", return_value=pl.DataFrame()
        ), patch.object(
            run_data_collection, "get_watch_status_history", return_value={}
        ), patch.object(
            run_data_collection, "check_fitbit_alerts", return_value={}
        ) as wearable_check, patch.object(
            run_data_collection,
            "analyze_whatsapp_messages",
            side_effect=RuntimeError("Sheets quota reached"),
        ):
            run_data_collection.hourly_data_collection()

        wearable_check.assert_called_once()


if __name__ == "__main__":
    unittest.main()
