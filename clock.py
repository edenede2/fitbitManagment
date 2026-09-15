#!/usr/bin/env python3
"""Dedicated Heroku clock process for monitoring and Shared Drive archives."""

from __future__ import annotations

import os
import threading
import time
import traceback

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from entity.Sheet import Spreadsheet
from model.config import get_secrets
from run_data_collection import hourly_data_collection, load_runtime_config
from run_drive_archive_collection import collect_drive_archives
from services.archive_manifest import finish_job, heartbeat, start_job


TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Jerusalem")
_job_lock = threading.Lock()


def _enabled() -> bool:
    return os.getenv("CLOCK_RUN_JOBS", "false").strip().casefold() in {"1", "true", "yes", "on"}


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


def _run_with_backoff(job_name: str, operation):
    delays = _retry_delays()
    for attempt in range(len(delays) + 1):
        try:
            result = operation()
            if isinstance(result, dict) and result.get("failed"):
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


def _run_audited(job_name: str, operation) -> None:
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
        result = _run_with_backoff(job_name, operation)
        finish_job(spreadsheet, run_id, status="success", message=str(result or ""))
    except Exception as exc:
        if spreadsheet is not None and run_id:
            finish_job(spreadsheet, run_id, status="failed", message=type(exc).__name__)
        traceback.print_exc()
        raise
    finally:
        _job_lock.release()


def run_monitoring() -> None:
    _run_audited("hourly-monitoring", hourly_data_collection)


def run_archive() -> None:
    _run_audited("hourly-drive-archive", collect_drive_archives)


def write_heartbeat() -> None:
    try:
        heartbeat(_spreadsheet())
    except Exception:
        traceback.print_exc()


def build_scheduler() -> BlockingScheduler:
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
    return scheduler


def main() -> int:
    print(f"Starting AdmonTracker clock in timezone {TIMEZONE}; jobs_enabled={_enabled()}")
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
    build_scheduler().start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
