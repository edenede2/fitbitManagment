import datetime
import os
import unittest
from contextlib import contextmanager
from copy import deepcopy
from email import message_from_string
from unittest.mock import Mock, patch

import polars as pl

import run_data_collection
from services.alert_delivery import (
    alert_is_due,
    detected_state,
    next_interval_hours,
    recipient_fingerprint,
    resolved_state,
    sent_state,
)
from services.wearable_alert_email import (
    DASHBOARD_URL,
    build_wearable_alert_message,
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
        state = sent_state(
            state,
            reasons=["Battery (10%)"],
            now=now,
            thread_recipient_fingerprint="recipient-hash",
            thread_root_message_id="<root@admontracker.online>",
            thread_last_message_id="<last@admontracker.online>",
        )
        state = resolved_state(state, now=now + datetime.timedelta(minutes=30))
        self.assertEqual(state["thread_recipient_fingerprint"], "")
        self.assertEqual(state["thread_root_message_id"], "")
        self.assertEqual(state["thread_last_message_id"], "")
        recurrence = detected_state(
            state,
            project="Yoga",
            watch_name="YN4",
            reasons=["Battery (10%)"],
            now=now + datetime.timedelta(hours=2),
        )
        self.assertEqual(recurrence["send_count"], 0)
        self.assertTrue(alert_is_due(recurrence, now + datetime.timedelta(hours=2)))

    def test_recipient_fingerprint_is_stable_and_does_not_expose_addresses(self):
        first = recipient_fingerprint(["Manager@Example.org", "student@example.org"])
        second = recipient_fingerprint(["student@example.org", "manager@example.org"])

        self.assertEqual(first, second)
        self.assertNotIn("manager", first)
        self.assertNotIn("@", first)

    def test_send_count_overrides_a_stale_cached_interval(self):
        last_sent = datetime.datetime(2026, 10, 11, 8, tzinfo=UTC)
        for cached_interval in (0, -1, 1, 2, 999, "bad", float("inf")):
            with self.subTest(cached_interval=cached_interval):
                state = {
                    "active": "TRUE", "send_count": 3,
                    "last_sent_at": last_sent.isoformat(),
                    "next_interval_hours": cached_interval,
                }
                self.assertFalse(alert_is_due(state, last_sent + datetime.timedelta(hours=4, seconds=-1)))
                self.assertTrue(alert_is_due(state, last_sent + datetime.timedelta(hours=4)))

    def test_legacy_intervals_are_bounded_when_send_count_is_missing(self):
        last_sent = datetime.datetime(2026, 10, 11, 8, tzinfo=UTC)
        for cached_interval, gap in ((2, 2), (0, 1), (-1, 1), (999, 24), ("bad", 1), (float("inf"), 1)):
            with self.subTest(cached_interval=cached_interval):
                state = {
                    "active": "TRUE", "last_sent_at": last_sent.isoformat(),
                    "next_interval_hours": cached_interval,
                }
                self.assertFalse(alert_is_due(state, last_sent + datetime.timedelta(hours=gap, seconds=-1)))
                self.assertTrue(alert_is_due(state, last_sent + datetime.timedelta(hours=gap)))


class AlertMessageTests(unittest.TestCase):
    def test_message_is_concise_escaped_and_contains_only_triggered_conditions(self):
        watches = [{
            "watch_name": "YN4 <demo>",
            "alert_reasons": ["Current Sync", "Battery (9%)"],
            "log_row": {
                "CurrentFailedSync": 3,
                "lastBattaryVal": 9,
                "lastCheck": "2026-09-30 09:00:00",
                "lastSynced": "2026-09-29 21:00:00",
                "lastHRVal": "SENSITIVE_HEALTH_VALUE",
                "lastSleepDur": "SENSITIVE_SLEEP_VALUE",
            },
            "config": {
                "currentSyncThr": 2,
                "batteryThr": 10,
                "currentHrThr": 1,
                "currentSleepThr": 1,
            },
        }]

        plain, html = build_wearable_alert_message(
            project="Yoga <study>",
            watches=watches,
            evaluated_at=datetime.datetime(2026, 9, 30, 7, 0, tzinfo=UTC),
        )

        for message in (plain, html):
            self.assertIn("Current sync failures: 3", message)
            self.assertIn("Battery level is 9%", message)
            self.assertIn(DASHBOARD_URL, message)
            self.assertNotIn("SENSITIVE_HEALTH_VALUE", message)
            self.assertNotIn("SENSITIVE_SLEEP_VALUE", message)
            self.assertNotIn("fitbitestapipy.streamlit.app", message)
            self.assertNotIn("edenede2", message)
        self.assertIn("YN4 &lt;demo&gt;", html)
        self.assertIn("Yoga &lt;study&gt;", html)
        self.assertIn("Alert generated: 2026-09-30 10:00 IDT", plain)
        self.assertIn("Device status checked: 2026-09-30 09:00 IDT", plain)

    def test_message_includes_latest_available_device_snapshot_without_history(self):
        watches = [{
            "watch_name": "YN4",
            "alert_reasons": ["Current Sync"],
            "log_row": {
                "CurrentFailedSync": 2,
                "lastBattaryVal": "82%",
                "lastHRVal": "71",
                "lastCheck": "2026-09-30T09:00:00+03:00",
                "lastSynced": "2026-09-30T08:45:00+03:00",
            },
            "config": {"currentSyncThr": 2},
        }]

        plain, html = build_wearable_alert_message(
            project="Yoga",
            watches=watches,
            evaluated_at=datetime.datetime(2026, 9, 30, 7, 0, tzinfo=UTC),
        )

        expected = "Latest device snapshot: Battery 82%"
        self.assertIn(expected, plain)
        self.assertIn("Latest device snapshot:</strong> Battery 82%", html)
        self.assertIn("Heart rate 71 bpm", plain)
        self.assertIn("Heart rate 71 bpm", html)
        self.assertIn("1 device</strong> currently requires attention", html)
        self.assertNotIn("24-hour diagnostics", html)


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
                "one@example.org, two@example.org\u200f, ONE@example.org",
                "Fitbit alert test",
                "<p>test</p>",
                plain_text_body="Plain test",
                message_id="<current@admontracker.online>",
                in_reply_to="<previous@admontracker.online>",
                references=[
                    "<root@admontracker.online>",
                    "<previous@admontracker.online>",
                ],
            )

        self.assertTrue(result)
        self.assertEqual(smtp_class.call_count, 2)
        for call in smtp_class.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 30)
        self.assertEqual(smtp.starttls.call_count, 2)
        self.assertEqual(smtp.login.call_count, 2)
        recipients = [call.args[1] for call in smtp.sendmail.call_args_list]
        self.assertEqual(recipients, ["one@example.org", "two@example.org"])
        for send_call in smtp.sendmail.call_args_list:
            message = message_from_string(send_call.args[2])
            self.assertEqual(message["Subject"], "Fitbit alert test")
            self.assertIsNotNone(message["Date"])
            self.assertEqual(message["Message-ID"], "<current@admontracker.online>")
            self.assertEqual(message["In-Reply-To"], "<previous@admontracker.online>")
            self.assertEqual(
                message["References"],
                "<root@admontracker.online> <previous@admontracker.online>",
            )
            self.assertEqual(
                [part.get_content_type() for part in message.get_payload()],
                ["text/plain", "text/html"],
            )


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

    @contextmanager
    def _persistent_job(self):
        """Run the real alert job against persisted synthetic state and SMTP."""
        records = {}
        clock = {"now": datetime.datetime(2026, 10, 11, 8, tzinfo=UTC)}
        spreadsheet = self._spreadsheet()

        def append(_spreadsheet, tab, _columns, row):
            self.assertEqual(tab, run_data_collection.ALERT_STATE_TAB)
            records[(row["project"], row["watchName"])] = deepcopy(row)

        def update(_spreadsheet, tab, *, keys, updates):
            self.assertEqual(tab, run_data_collection.ALERT_STATE_TAB)
            key = (keys["project"], keys["watchName"])
            if key not in records:
                return False
            records[key].update(deepcopy(updates))
            return True

        def run(*, failing=True, muted=False, active=True, provider="fitbit"):
            devices = self._devices(muted=muted).with_columns(
                pl.lit(provider).alias("provider"),
                pl.lit("TRUE" if active else "FALSE").alias("isActive"),
            )
            return run_data_collection.check_fitbit_alerts(
                spreadsheet,
                self._log(current_failed_sync=1 if failing else 0),
                self._config(),
                devices,
            )

        with patch.object(
            run_data_collection.GoogleSheetsAdapter, "get_rows",
            side_effect=lambda *args, **kwargs: deepcopy(list(records.values())),
        ), patch.object(
            run_data_collection, "_append", side_effect=append,
        ), patch.object(
            run_data_collection, "_update_row_by_keys", side_effect=update,
        ), patch.object(
            run_data_collection, "_alert_now_utc", side_effect=lambda: clock["now"],
        ), patch.object(
            run_data_collection, "send_email_alert", return_value=True,
        ) as send:
            yield run, records, clock, send

    def test_full_persisted_cadence_for_both_wearable_providers(self):
        # These are cumulative offsets. Each reminder gap is measured from the
        # preceding successful mail, not from the first detected threshold.
        expected_hours = [0, 1, 3, 7, 15, 31, 55, 79, 103]
        for provider in ("fitbit", "google_health"):
            with self.subTest(provider=provider), self._persistent_job() as (run, records, clock, send):
                start = clock["now"]
                for send_index, elapsed in enumerate(expected_hours):
                    if send_index:
                        clock["now"] = start + datetime.timedelta(hours=elapsed, seconds=-1)
                        self.assertEqual(run(provider=provider), {})
                        self.assertEqual(send.call_count, send_index)
                    clock["now"] = start + datetime.timedelta(hours=elapsed)
                    result = run(provider=provider)
                    state = records[("Yoga", "YN4")]
                    self.assertEqual(result["Yoga"]["count"], 1)
                    self.assertEqual(send.call_count, send_index + 1)
                    self.assertEqual(state["send_count"], send_index + 1)
                    self.assertEqual(state["next_interval_hours"], next_interval_hours(send_index + 1))
                    self.assertEqual(state["last_sent_at"], clock["now"].isoformat())
                    # A duplicate job/restart using persisted state must not mail again.
                    self.assertEqual(run(provider=provider), {})
                    self.assertEqual(send.call_count, send_index + 1)

    def test_failed_smtp_does_not_advance_or_delay_the_sequence(self):
        with self._persistent_job() as (run, records, clock, send):
            send.return_value = False
            self.assertEqual(run(), {})
            state = records[("Yoga", "YN4")]
            self.assertEqual(state["send_count"], 0)
            self.assertEqual(state["last_sent_at"], "")

            send.return_value = True
            clock["now"] += datetime.timedelta(minutes=5)
            run()
            first_sent = records[("Yoga", "YN4")]["last_sent_at"]
            clock["now"] += datetime.timedelta(hours=1)
            send.return_value = False
            self.assertEqual(run(), {})
            self.assertEqual(records[("Yoga", "YN4")]["send_count"], 1)
            self.assertEqual(records[("Yoga", "YN4")]["last_sent_at"], first_sent)
            send.return_value = True
            clock["now"] += datetime.timedelta(minutes=1)
            run()
            self.assertEqual(records[("Yoga", "YN4")]["send_count"], 2)
            self.assertEqual(records[("Yoga", "YN4")]["next_interval_hours"], 2)

    def test_gap_starts_after_smtp_completion_not_evaluation(self):
        with self._persistent_job() as (run, records, clock, send):
            evaluated_at = clock["now"]

            def smtp_delay(*args, **kwargs):
                clock["now"] += datetime.timedelta(minutes=5)
                return True

            send.side_effect = smtp_delay
            run()
            self.assertEqual(records[("Yoga", "YN4")]["last_sent_at"], clock["now"].isoformat())
            clock["now"] = evaluated_at + datetime.timedelta(hours=1)
            self.assertEqual(run(), {})
            self.assertEqual(send.call_count, 1)
            clock["now"] += datetime.timedelta(minutes=5)
            run()
            self.assertEqual(send.call_count, 2)

    def test_saved_reminder_job_uses_latest_status_without_provider_collection(self):
        frames = {
            "fitbit_alerts_config": self._config(),
            "fitbit": self._devices(),
            "log": self._log(),
        }
        spreadsheet = Mock()
        spreadsheet.get_sheet.side_effect = lambda name, **kwargs: Mock(
            to_dataframe=Mock(return_value=frames[name])
        )
        with patch.dict(os.environ, {"SPREADSHEET_KEY": "synthetic-sheet"}), patch.object(
            run_data_collection, "load_runtime_config",
        ), patch.object(
            run_data_collection, "Spreadsheet", return_value=spreadsheet,
        ), patch.object(
            run_data_collection.GoogleSheetsAdapter, "connect",
        ), patch.object(
            run_data_collection, "check_fitbit_alerts", return_value={"Yoga": {"count": 1}},
        ) as alert, patch.object(run_data_collection, "get_watch_details") as collect:
            self.assertEqual(run_data_collection.check_saved_wearable_alerts(), {"alert_projects": 1})
        collect.assert_not_called()
        self.assertEqual([call.args[0] for call in spreadsheet.get_sheet.call_args_list], list(frames))
        self.assertEqual(alert.call_args.args[1].to_dicts(), self._log().to_dicts())

    def test_recovery_mute_and_deactivation_reset_the_episode(self):
        for reset_arguments in ({"failing": False}, {"muted": True}, {"active": False}):
            with self.subTest(reset_arguments=reset_arguments), self._persistent_job() as (run, records, clock, send):
                run()
                clock["now"] += datetime.timedelta(hours=1)
                run()
                prior_thread = records[("Yoga", "YN4")]["thread_root_message_id"]
                clock["now"] += datetime.timedelta(minutes=1)
                self.assertEqual(run(**reset_arguments), {})
                reset_state = records[("Yoga", "YN4")]
                self.assertEqual(reset_state["active"], "FALSE")
                self.assertEqual(reset_state["send_count"], 0)
                self.assertEqual(send.call_count, 2)
                clock["now"] += datetime.timedelta(minutes=1)
                run()
                state = records[("Yoga", "YN4")]
                self.assertEqual(send.call_count, 3)
                self.assertEqual(state["send_count"], 1)
                self.assertEqual(state["next_interval_hours"], 1)
                self.assertNotEqual(state["thread_root_message_id"], prior_thread)
                self.assertEqual(send.call_args.kwargs["in_reply_to"], "")

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
        send_kwargs = send.call_args.kwargs
        self.assertIn("AdmonTracker Fitbit/Google Health alert - Yoga", send.call_args.args)
        self.assertIn("Current sync failures: 1", send_kwargs["plain_text_body"])
        self.assertTrue(send_kwargs["message_id"].endswith("@admontracker.online>"))
        self.assertEqual(send_kwargs["in_reply_to"], "")
        append.assert_called_once()
        self.assertEqual(update.call_args.kwargs["updates"]["send_count"], 1)
        self.assertEqual(update.call_args.kwargs["updates"]["next_interval_hours"], 1)
        self.assertEqual(
            update.call_args.kwargs["updates"]["thread_recipient_fingerprint"],
            recipient_fingerprint(["manager@example.org"]),
        )
        self.assertEqual(
            update.call_args.kwargs["updates"]["thread_root_message_id"],
            send_kwargs["message_id"],
        )
        self.assertEqual(
            update.call_args.kwargs["updates"]["thread_last_message_id"],
            send_kwargs["message_id"],
        )
        self.assertEqual(result["Yoga"]["watches"], ["YN4"])

    def test_reminder_replies_to_existing_alert_conversation(self):
        spreadsheet = self._spreadsheet()
        existing_state = {
            "project": "Yoga",
            "watchName": "YN4",
            "active": "TRUE",
            "muted": "FALSE",
            "first_detected_at": "2026-09-29T08:00:00+00:00",
            "last_sent_at": "2000-01-01T00:00:00+00:00",
            "next_interval_hours": 1,
            "send_count": 1,
            "alert_reasons": "Current Sync",
            "thread_recipient_fingerprint": recipient_fingerprint(["manager@example.org"]),
            "thread_root_message_id": "<root@admontracker.online>",
            "thread_last_message_id": "<previous@admontracker.online>",
            "resolved_at": "",
            "updated_at": "2026-09-29T08:00:00+00:00",
        }

        with patch.object(
            run_data_collection.GoogleSheetsAdapter,
            "get_rows",
            return_value=[existing_state],
        ), patch.object(
            run_data_collection, "_update_row_by_keys", return_value=True
        ), patch.object(
            run_data_collection, "send_email_alert", return_value=True
        ) as send:
            run_data_collection.check_fitbit_alerts(
                spreadsheet,
                self._log(),
                self._config(),
                self._devices(),
            )

        send.assert_called_once()
        self.assertEqual(
            send.call_args.args[1],
            "AdmonTracker Fitbit/Google Health alert - Yoga",
        )
        self.assertEqual(
            send.call_args.kwargs["in_reply_to"],
            "<previous@admontracker.online>",
        )
        self.assertEqual(
            send.call_args.kwargs["references"],
            ["<root@admontracker.online>", "<previous@admontracker.online>"],
        )

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
