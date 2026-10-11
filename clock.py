#!/usr/bin/env python3
"""Dedicated Heroku clock process for monitoring and Shared Drive archives."""

from __future__ import annotations

import os
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from model.config import get_secrets
from run_data_collection import (
    ALERT_STATE_TAB, check_saved_wearable_alerts, hourly_data_collection, load_runtime_config,
)
from run_drive_archive_collection import collect_drive_archives
from services.archive_manifest import finish_job, heartbeat, start_job
from services.alert_delivery import next_alert_due_at


TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Jerusalem")
_job_lock = threading.Lock()
_scheduler = None
ALERT_REMINDER_JOB_ID = "wearable-alert-reminder"
ALERT_RETRY_SECONDS = 300


def _enabled() -> bool:
    return os.getenv("CLOCK_RUN_JOBS", "false").strip().casefold() in {"1", "true", "yes", "on"}


def _archive_enabled() -> bool:
    return os.getenv("ARCHIVE_RUN_JOBS", "true").strip().casefold() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _spreadsheet() -> Spreadsheet:
    load_runtime_config()
    key = os.getenv("SPREADSHEET_KEY", "").strip()
    if not key:
        key = str(get_secrets().get("spreadsheet_key") or "").strip()
    if not key:
        raise RuntimeError("SPREADSHEET_KEY is required for the clock process")
    return Spreadsheet(name="FitbitData", api_key=key)


def _retry_delays() -> list[float]:
    raw = os.getenv("CLOCK_RETRY_DELAYS_SECONDS", "5,20")
    delays: list[float] = []
    for value in raw.split(","):
        try:
            delays.append(max(0.0, min(float(value.strip()), 60.0)))
        except ValueError:
            raise ValueError("CLOCK_RETRY_DELAYS_SECONDS must be comma-separated numbers")
    return delays


def _run_with_backoff(
    job_name: str,
    operation,
    *,
    retry_reported_failures: bool = True,
):
    delays = _retry_delays()
    for attempt in range(len(delays) + 1):
        try:
            result = operation()
            if (
                retry_reported_failures
                and isinstance(result, dict)
                and result.get("failed")
            ):
                raise RuntimeError(f"{result['failed']} archive operations failed")
            return result
        except Exception as exc:
            if attempt >= len(delays):
                raise
            delay = delays[attempt]
            print(
                f"{job_name}: attempt {attempt + 1} failed with "
                f"{type(exc).__name__}; retrying in {delay:g}s"
            )
            time.sleep(delay)


def _run_audited(
    job_name: str,
    operation,
    *,
    retry_reported_failures: bool = True,
) -> None:
    if not _enabled():
        print(f"{job_name}: skipped because CLOCK_RUN_JOBS is false")
        return
    if not _job_lock.acquire(blocking=False):
        print(f"{job_name}: skipped because another clock job is running")
        return
    spreadsheet = None
    run_id = ""
    try:
        spreadsheet = _spreadsheet()
        run_id, _ = start_job(spreadsheet, job_name)
        result = _run_with_backoff(
            job_name,
            operation,
            retry_reported_failures=retry_reported_failures,
        )
        status = (
            "partial_failure"
            if isinstance(result, dict) and result.get("failed")
            else "success"
        )
        finish_job(spreadsheet, run_id, status=status, message=str(result or ""))
    except Exception as exc:
        if spreadsheet is not None and run_id:
            finish_job(spreadsheet, run_id, status="failed", message=type(exc).__name__)
        traceback.print_exc()
        raise
    finally:
        _job_lock.release()


def _clock_now_utc():
    return datetime.now(timezone.utc)


def _schedule_alert_reminder() -> None:
    """One earliest-deadline timer, restored from persistent state on startup.

    This is not a polling loop. SMTP/clock failures get a bounded five-minute
    retry, while successful deliveries get the next 1/2/4/8/16/24-hour deadline.
    """
    if _scheduler is None or not _enabled():
        return
    try:
        rows = GoogleSheetsAdapter.get_rows(_spreadsheet(), ALERT_STATE_TAB)
        due_times = [due for row in rows if (due := next_alert_due_at(row)) is not None]
        if not due_times:
            try:
                _scheduler.remove_job(ALERT_REMINDER_JOB_ID)
            except JobLookupError:
                pass
            return
        now = _clock_now_utc()
        due = min(due_times)
        if due <= now:
            due = now + timedelta(seconds=ALERT_RETRY_SECONDS)
        _scheduler.add_job(
            run_alert_reminders,
            DateTrigger(run_date=due, timezone=TIMEZONE),
            id=ALERT_REMINDER_JOB_ID,
            replace_existing=True,
        )
    except Exception as exc:
        # A schedule read failure must not kill the hourly monitor. Restore the
        # timer after five minutes rather than silently waiting another hour.
        print(f"Wearable reminder scheduling needs retry: {type(exc).__name__}")
        _scheduler.add_job(
            run_alert_reminders,
            DateTrigger(
                run_date=_clock_now_utc() + timedelta(seconds=ALERT_RETRY_SECONDS),
                timezone=TIMEZONE,
            ),
            id=ALERT_REMINDER_JOB_ID,
            replace_existing=True,
        )


def run_monitoring() -> None:
    try:
        _run_audited("hourly-monitoring", hourly_data_collection)
    finally:
        _schedule_alert_reminder()


def run_alert_reminders() -> None:
    try:
        _run_audited(ALERT_REMINDER_JOB_ID, check_saved_wearable_alerts)
    finally:
        _schedule_alert_reminder()


def run_archive() -> None:
    if not _archive_enabled():
        print("hourly-drive-archive: skipped because ARCHIVE_RUN_JOBS is false")
        return
    # collect_drive_archives retries only its failed items.  Do not repeat the
    # entire successful archive batch when a small subset remains unavailable.
    _run_audited(
        "hourly-drive-archive",
        collect_drive_archives,
        retry_reported_failures=False,
    )


def write_heartbeat() -> None:
    try:
        heartbeat(_spreadsheet())
    except Exception:
        traceback.print_exc()


def build_scheduler() -> BlockingScheduler:
    global _scheduler
    scheduler = BlockingScheduler(
        timezone=TIMEZONE,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 1800,
        },
    )
    scheduler.add_job(
        run_monitoring,
        CronTrigger(minute=0, timezone=TIMEZONE),
        id="hourly-monitoring",
        replace_existing=True,
    )
    scheduler.add_job(
        run_archive,
        CronTrigger(minute=30, timezone=TIMEZONE),
        id="hourly-drive-archive",
        replace_existing=True,
    )
    scheduler.add_job(
        write_heartbeat,
        IntervalTrigger(minutes=5, timezone=TIMEZONE),
        id="clock-heartbeat",
        replace_existing=True,
    )
    _scheduler = scheduler
    return scheduler


def main() -> int:
    print(f"Starting AdmonTracker clock in timezone {TIMEZONE}; jobs_enabled={_enabled()}")
    scheduler = build_scheduler()
    catch_up = os.getenv("CLOCK_CATCH_UP_ON_START", "true").strip().casefold() in {
        "1", "true", "yes", "on",
    }
    if _enabled() and catch_up:
        # One bounded catch-up of each idempotent job covers periods missed while
        # the dyno was restarting; APScheduler then owns the regular cadence.
        for catch_up_job in (run_monitoring, run_archive):
            try:
                catch_up_job()
            except Exception as exc:
                print(f"Clock catch-up failed with {type(exc).__name__}; regular scheduling will continue")
    _schedule_alert_reminder()
    scheduler.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
