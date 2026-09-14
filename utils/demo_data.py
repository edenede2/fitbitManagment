"""Bundled synthetic data used exclusively by the public guest demonstration."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pandas as pd

from entity.Sheet import SheetFactory, Spreadsheet


DEMO_PROJECT = "Demo Research Project"
DEMO_PROJECT_SECONDARY = "Demo Sleep Study"

DEMO_SHEETS: dict[str, list[dict[str, Any]]] = {
    "project": [
        {"id": "DEMO-PROJECT-001", "name": DEMO_PROJECT},
        {"id": "DEMO-PROJECT-002", "name": DEMO_PROJECT_SECONDARY},
    ],
    "user": [
        {
            "id": "DEMO-USER-001",
            "name": "Demo Participant Alpha",
            "email": "participant-alpha@example.invalid",
            "role": "Student",
            "project": DEMO_PROJECT,
            "projects": DEMO_PROJECT,
        },
        {
            "id": "DEMO-USER-002",
            "name": "Demo Participant Beta",
            "email": "participant-beta@example.invalid",
            "role": "Student",
            "project": DEMO_PROJECT_SECONDARY,
            "projects": DEMO_PROJECT_SECONDARY,
        },
    ],
    "fitbit": [
        {
            "project": DEMO_PROJECT,
            "name": "DEMO-WATCH-001",
            "token": "",
            "oauth_type": "synthetic",
            "provider": "synthetic",
            "oauth_client_key": "",
            "auth_status": "example_connected",
            "health_user_id": "",
            "legacy_fitbit_user_id": "",
            "last_successful_fetch_at": "2026-08-15 08:30:00",
            "last_data_timestamp": "2026-08-15 08:25:00",
            "last_auth_error": "",
            "reauth_link": "",
            "reauth_link_created_at": "",
            "user": "Demo Participant Alpha",
            "isActive": True,
            "currentStudent": "Demo Participant Alpha",
        },
        {
            "project": DEMO_PROJECT_SECONDARY,
            "name": "DEMO-WATCH-002",
            "token": "",
            "oauth_type": "synthetic",
            "provider": "synthetic",
            "oauth_client_key": "",
            "auth_status": "example_attention_needed",
            "health_user_id": "",
            "legacy_fitbit_user_id": "",
            "last_successful_fetch_at": "2026-08-15 07:45:00",
            "last_data_timestamp": "2026-08-15 07:40:00",
            "last_auth_error": "Synthetic example only",
            "reauth_link": "",
            "reauth_link_created_at": "",
            "user": "Demo Participant Beta",
            "isActive": True,
            "currentStudent": "Demo Participant Beta",
        },
    ],
    "FitbitLog": [
        {
            "project": DEMO_PROJECT,
            "watchName": "DEMO-WATCH-001",
            "lastCheck": "2026-08-15 08:30:00",
            "lastSynced": "2026-08-15 08:25:00",
            "lastBattary": "2026-08-15 08:25:00",
            "lastHR": "2026-08-15 08:24:00",
            "lastSleepStartDateTime": "2026-08-14 23:10:00",
            "lastSleepEndDateTime": "2026-08-15 06:55:00",
            "lastSteps": "2026-08-15 08:20:00",
            "lastBattaryVal": "82",
            "lastHRVal": "71",
            "lastSleepDur": "465",
            "lastStepsVal": "3240",
            "CurrentFailedSync": "0",
            "TotalFailedSync": "1",
            "CurrentFailedHR": "0",
            "TotalFailedHR": "0",
            "CurrentFailedSleep": "0",
            "TotalFailedSleep": "0",
            "CurrentFailedSteps": "0",
            "TotalFailedSteps": "0",
            "ID": "DEMO-LOG-001",
        },
        {
            "project": DEMO_PROJECT_SECONDARY,
            "watchName": "DEMO-WATCH-002",
            "lastCheck": "2026-08-15 08:30:00",
            "lastSynced": "2026-08-15 07:40:00",
            "lastBattary": "2026-08-15 07:40:00",
            "lastHR": "2026-08-15 07:38:00",
            "lastSleepStartDateTime": "2026-08-14 22:50:00",
            "lastSleepEndDateTime": "2026-08-15 06:10:00",
            "lastSteps": "2026-08-15 07:35:00",
            "lastBattaryVal": "18",
            "lastHRVal": "76",
            "lastSleepDur": "440",
            "lastStepsVal": "2110",
            "CurrentFailedSync": "1",
            "TotalFailedSync": "3",
            "CurrentFailedHR": "0",
            "TotalFailedHR": "1",
            "CurrentFailedSleep": "0",
            "TotalFailedSleep": "0",
            "CurrentFailedSteps": "1",
            "TotalFailedSteps": "2",
            "ID": "DEMO-LOG-002",
        },
    ],
    "log": [],
    "fitbit_alerts_config": [
        {
            "project": DEMO_PROJECT,
            "currentSyncThr": "3",
            "totalSyncThr": "10",
            "currentHrThr": "3",
            "totalHrThr": "10",
            "currentSleepThr": "3",
            "totalSleepThr": "10",
            "currentStepsThr": "3",
            "totalStepsThr": "10",
            "batteryThr": "20",
            "manager": "demo-manager@example.invalid",
            "email": "demo-alerts@example.invalid",
            "watch": "",
            "endDate": "2026-12-31",
        }
    ],
    "qualtrics_alerts_config": [
        {"hoursThr": "48", "project": DEMO_PROJECT, "manager": "demo-manager@example.invalid"}
    ],
    "appsheet_alerts_config": [
        {"email": "demo-manager@example.invalid", "user": "DEMO-PARTICIPANT-001", "missingThr": "3"}
    ],
    "EMA": [
        {
            "num": "DEMO-NUMBER-001",
            "currentDate": "2026-08-15",
            "startDate": "2026-08-15 08:00:00",
            "endDate": "2026-08-15 08:06:00",
            "status": "Complete",
            "finished": "TRUE",
        },
        {
            "num": "DEMO-NUMBER-002",
            "currentDate": "2026-08-15",
            "startDate": "2026-08-15 09:00:00",
            "endDate": "2026-08-15 09:03:00",
            "status": "Pending",
            "finished": "FALSE",
        },
    ],
    "late_nums": [
        {
            "nums": "DEMO-NUMBER-002",
            "sentTime": "2026-08-13 09:00:00",
            "hoursLate": "48",
            "lastUpdated": "2026-08-15 09:00:00",
            "accepted": "FALSE",
        }
    ],
    "suspicious_nums": [
        {
            "nums": "DEMO-NUMBER-003",
            "filledTime": "2026-08-15 03:15:00",
            "lastUpdated": "2026-08-15 09:00:00",
            "accepted": "FALSE",
        }
    ],
    "for_analysis": [
        {
            "User Id": "DEMO-PARTICIPANT-001",
            "KEY": "DEMO-EMA-001",
            "Date Time": "2026-08-13 08:00:00",
            "Pain Level": 4,
            "Fatigue Level": 5,
            "Mood": 7,
            "Sleep Quality": 6,
        },
        {
            "User Id": "DEMO-PARTICIPANT-001",
            "KEY": "DEMO-EMA-002",
            "Date Time": "2026-08-14 08:00:00",
            "Pain Level": 3,
            "Fatigue Level": 4,
            "Mood": 8,
            "Sleep Quality": 7,
        },
        {
            "User Id": "DEMO-PARTICIPANT-002",
            "KEY": "DEMO-EMA-003",
            "Date Time": "2026-08-15 08:00:00",
            "Pain Level": 6,
            "Fatigue Level": 7,
            "Mood": 5,
            "Sleep Quality": 4,
        },
    ],
    "chats": [
        {
            "watchName": "DEMO-WATCH-001",
            "user": "Demo Researcher",
            "content": "Synthetic example message",
            "timestamp": "2026-08-15 08:35:00",
        }
    ],
}


DEMO_HEALTH_SERIES = pd.DataFrame(
    [
        {"timestamp": f"2026-08-15 {hour:02d}:00:00", "watch": watch, "heart_rate": hr, "steps": steps}
        for watch, values in {
            "DEMO-WATCH-001": [(8, 68, 250), (9, 72, 720), (10, 78, 1260), (11, 74, 1780), (12, 70, 2230)],
            "DEMO-WATCH-002": [(8, 73, 190), (9, 79, 510), (10, 81, 980), (11, 77, 1430), (12, 75, 2010)],
        }.items()
        for hour, hr, steps in values
    ]
)


_SHEET_TYPES = {
    "project": "project",
    "user": "user",
    "fitbit": "fitbit",
    "FitbitLog": "log",
    "log": "log",
    "fitbit_alerts_config": "fitbit_alerts_config",
    "qualtrics_alerts_config": "qualtrics_alerts_config",
    "EMA": "EMA",
    "late_nums": "late_nums",
    "suspicious_nums": "suspicious_nums",
    "for_analysis": "for_analysis",
    "chats": "chats",
}


def create_demo_spreadsheet() -> Spreadsheet:
    """Return a new read-only spreadsheet backed only by bundled fake records."""
    spreadsheet = Spreadsheet(
        name="Synthetic Guest Dataset",
        api_key="",
        read_only=True,
        source_kind="demo",
    )
    for sheet_name, rows in DEMO_SHEETS.items():
        sheet = SheetFactory.create_sheet(_SHEET_TYPES.get(sheet_name, "generic"), sheet_name)
        sheet.data = deepcopy(rows)
        spreadsheet.sheets[sheet_name] = sheet
    return spreadsheet


def demo_dataframe(sheet_name: str) -> pd.DataFrame:
    return pd.DataFrame(deepcopy(DEMO_SHEETS.get(sheet_name, [])))
