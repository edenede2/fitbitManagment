"""Synthetic reminder timers: no scheduler thread, cloud calls or email sends."""

import datetime
import unittest
from unittest.mock import Mock, patch

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler

import clock
from services.alert_delivery import next_alert_due_at


UTC = datetime.timezone.utc


class ReminderDeadlineTests(unittest.TestCase):
    def test_deadline_uses_successful_delivery_and_capped_sequence(self):
        sent = datetime.datetime(2026, 10, 11, 8, 5, tzinfo=UTC)
        for count, gap in enumerate((1, 2, 4, 8, 16, 24, 24), 1):
            with self.subTest(count=count):
                state = {
                    "active": "TRUE", "muted": "FALSE", "send_count": count,
                    "last_sent_at": sent.isoformat(), "next_interval_hours": 999,
                }
                self.assertEqual(next_alert_due_at(state), sent + datetime.timedelta(hours=gap))

    def test_unsent_detection_and_closed_episodes(self):
        detected = "2026-10-11T08:00:00+00:00"
        state = {"active": "TRUE", "first_detected_at": detected, "last_sent_at": ""}
        self.assertEqual(next_alert_due_at(state), datetime.datetime.fromisoformat(detected))
        self.assertIsNone(next_alert_due_at({**state, "active": "FALSE"}))
        self.assertIsNone(next_alert_due_at({**state, "muted": "TRUE"}))
        self.assertIsNone(next_alert_due_at({"active": "TRUE"}))


class ReminderClockTests(unittest.TestCase):
    def setUp(self):
        self.scheduler = Mock()
        self.now = datetime.datetime(2026, 10, 11, 8, 10, tzinfo=UTC)
        for target, options in (
            ("_scheduler", {"new": self.scheduler}),
            ("_enabled", {"return_value": True}),
            ("_spreadsheet", {"return_value": Mock()}),
            ("_clock_now_utc", {"return_value": self.now}),
        ):
            patcher = patch.object(clock, target, **options)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _scheduled_time(self):
        call = self.scheduler.add_job.call_args
        self.assertIs(call.args[0], clock.run_alert_reminders)
        self.assertEqual(call.kwargs["id"], clock.ALERT_REMINDER_JOB_ID)
        self.assertTrue(call.kwargs["replace_existing"])
        return call.args[1].get_next_fire_time(None, self.now)

    def test_earliest_pending_mail_gets_a_single_one_shot_timer(self):
        rows = [
            {"active": "TRUE", "send_count": 3, "last_sent_at": "2026-10-11T08:05:00+00:00"},
            {"active": "TRUE", "send_count": 1, "last_sent_at": "2026-10-11T08:06:00+00:00"},
            {"active": "FALSE", "send_count": 1, "last_sent_at": "2026-10-11T07:00:00+00:00"},
        ]
        with patch.object(clock.GoogleSheetsAdapter, "get_rows", return_value=rows) as read:
            clock._schedule_alert_reminder()
        self.scheduler.add_job.assert_called_once()
        self.assertEqual(read.call_args.args[1], clock.ALERT_STATE_TAB)
        self.assertEqual(self._scheduled_time(), datetime.datetime(2026, 10, 11, 9, 6, tzinfo=UTC))

    def test_overdue_or_failed_mail_retries_without_a_catchup_burst(self):
        rows = [{"active": "TRUE", "first_detected_at": "2026-10-11T07:00:00+00:00"}]
        with patch.object(clock.GoogleSheetsAdapter, "get_rows", return_value=rows):
            clock._schedule_alert_reminder()
        self.scheduler.add_job.assert_called_once()
        self.assertEqual(self._scheduled_time(), self.now + datetime.timedelta(minutes=5))

    def test_state_store_failure_schedules_a_safe_retry(self):
        with patch.object(clock.GoogleSheetsAdapter, "get_rows", side_effect=RuntimeError("synthetic outage")):
            clock._schedule_alert_reminder()
        self.assertEqual(self._scheduled_time(), self.now + datetime.timedelta(minutes=5))

    def test_no_active_episode_removes_the_old_timer(self):
        self.scheduler.remove_job.side_effect = JobLookupError(clock.ALERT_REMINDER_JOB_ID)
        with patch.object(clock.GoogleSheetsAdapter, "get_rows", return_value=[]):
            clock._schedule_alert_reminder()
        self.scheduler.remove_job.assert_called_once_with(clock.ALERT_REMINDER_JOB_ID)
        self.scheduler.add_job.assert_not_called()

    def test_disabled_jobs_do_not_read_or_schedule(self):
        with patch.object(clock, "_enabled", return_value=False), patch.object(
            clock.GoogleSheetsAdapter, "get_rows",
        ) as read:
            clock._schedule_alert_reminder()
        read.assert_not_called()
        self.scheduler.add_job.assert_not_called()

    def test_busy_clock_defers_the_reminder_without_running_collection(self):
        lock = Mock()
        lock.acquire.return_value = False
        rows = [{"active": "TRUE", "send_count": 1, "last_sent_at": "2026-10-11T07:00:00+00:00"}]
        with patch.object(clock, "_job_lock", lock), patch.object(
            clock.GoogleSheetsAdapter, "get_rows", return_value=rows,
        ), patch.object(clock, "check_saved_wearable_alerts") as operation:
            clock.run_alert_reminders()
        operation.assert_not_called()
        self.assertEqual(self._scheduled_time(), self.now + datetime.timedelta(minutes=5))

    def test_monitor_and_reminder_rearm_even_after_a_job_failure(self):
        for runner, operation in (
            (clock.run_monitoring, clock.hourly_data_collection),
            (clock.run_alert_reminders, clock.check_saved_wearable_alerts),
        ):
            with self.subTest(runner=runner.__name__), patch.object(
                clock, "_run_audited", side_effect=RuntimeError("synthetic failure"),
            ) as audited, patch.object(clock, "_schedule_alert_reminder") as rearm:
                with self.assertRaises(RuntimeError):
                    runner()
                self.assertIs(audited.call_args.args[1], operation)
                rearm.assert_called_once()

    def test_startup_restores_reminders_even_without_monitoring_catchup(self):
        with patch.dict("os.environ", {"CLOCK_CATCH_UP_ON_START": "false"}), patch.object(
            clock, "build_scheduler", return_value=self.scheduler,
        ), patch.object(clock, "_schedule_alert_reminder") as restore, patch.object(
            clock, "run_monitoring",
        ) as monitor:
            self.assertEqual(clock.main(), 0)
        restore.assert_called_once()
        self.scheduler.start.assert_called_once()
        monitor.assert_not_called()

    def test_real_paused_scheduler_replaces_and_removes_only_the_reminder(self):
        # Exercise actual APScheduler APIs without executing any cloud job.
        with patch.object(clock, "BlockingScheduler", BackgroundScheduler):
            scheduler = clock.build_scheduler()
        try:
            scheduler.start(paused=True)
            row = {"active": "TRUE", "send_count": 1, "last_sent_at": "2026-10-11T08:06:00+00:00"}
            with patch.object(clock.GoogleSheetsAdapter, "get_rows", return_value=[row]):
                clock._schedule_alert_reminder()
                clock._schedule_alert_reminder()
            self.assertEqual(len(scheduler.get_jobs()), 4)
            job = scheduler.get_job(clock.ALERT_REMINDER_JOB_ID)
            self.assertEqual(job.trigger.run_date, datetime.datetime(2026, 10, 11, 9, 6, tzinfo=UTC))
            with patch.object(clock.GoogleSheetsAdapter, "get_rows", return_value=[]):
                clock._schedule_alert_reminder()
            self.assertIsNone(scheduler.get_job(clock.ALERT_REMINDER_JOB_ID))
            self.assertEqual(len(scheduler.get_jobs()), 3)
        finally:
            scheduler.shutdown()


if __name__ == "__main__":
    unittest.main()
