import unittest
from datetime import date
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from entity.Watch import ProcessorFactory, RequestBuilder, Watch
from run_drive_archive_collection import PeriodWindow, _google_filter
from services.google_health_client import GoogleHealthClient
from utils.fitbit_oauth import build_authorize_url
from view.dashboard import SIGNAL_MAP, SIGNAL_OPTIONS


class GoogleHealthDashboardSignalTests(unittest.TestCase):
    def setUp(self):
        self.client = GoogleHealthClient(access_token_provider=lambda: "not-used")

    def test_sleep_uses_supported_type_filter_and_normalizes_summary(self):
        point = {
            "sleep": {
                "interval": {
                    "startTime": "2026-09-20T20:00:00Z",
                    "endTime": "2026-09-21T04:00:00Z",
                },
                "metadata": {"nap": False},
                "summary": {
                    "minutesInSleepPeriod": "480",
                    "minutesAsleep": "430",
                    "minutesAwake": "50",
                    "stagesSummary": [
                        {"type": "DEEP", "minutes": "80", "count": "3"},
                        {"type": "REM", "minutes": "90", "count": "4"},
                    ],
                },
            }
        }
        with patch.object(self.client, "list_data_points", return_value=[point]) as list_points:
            payload = self.client.fetch_raw(
                "Sleep", start_date="2026-09-20", end_date="2026-09-21"
            )

        self.assertEqual(payload["sleep"][0]["minutesAsleep"], 430)
        self.assertEqual(payload["sleep"][0]["levels"]["summary"]["deep"]["minutes"], 80)
        self.assertEqual(list_points.call_args.args[0], "sleep")
        self.assertIn("sleep.interval.civil_end_time", list_points.call_args.kwargs["filter_expr"])

    def test_respiratory_rate_uses_camel_case_daily_filter_and_normalizes_value(self):
        point = {
            "dailyRespiratoryRate": {
                "date": {"year": 2026, "month": 9, "day": 21},
                "breathsPerMinute": 14.25,
            }
        }
        with patch.object(self.client, "list_data_points", return_value=[point]) as list_points:
            payload = self.client.fetch_raw(
                "Breathing Rate", start_date="2026-09-21", end_date="2026-09-21"
            )

        self.assertEqual(payload["br"][0]["dateTime"], "2026-09-21")
        self.assertEqual(payload["br"][0]["value"]["breathingRate"], 14.25)
        self.assertEqual(list_points.call_args.args[0], "daily-respiratory-rate")
        self.assertIn("dailyRespiratoryRate.date", list_points.call_args.kwargs["filter_expr"])

    def test_physical_activity_normalizes_workout_without_notes_or_location(self):
        point = {
            "exercise": {
                "interval": {
                    "startTime": "2026-09-21T07:00:00Z",
                    "endTime": "2026-09-21T07:30:00Z",
                },
                "displayName": "Morning Walk",
                "exerciseType": "WALKING",
                "activeDuration": "1500s",
                "metricsSummary": {"steps": "2300", "caloriesKcal": 120},
                "notes": "must not reach the dashboard",
            }
        }
        with patch.object(self.client, "list_data_points", return_value=[point]) as list_points:
            payload = self.client.fetch_raw(
                "Physical Activity", start_date="2026-09-21", end_date="2026-09-21"
            )

        activity = payload["activities"][0]
        self.assertEqual(activity["activityName"], "Morning Walk")
        self.assertEqual(activity["duration"], 1_500_000)
        self.assertEqual(activity["steps"], "2300")
        self.assertNotIn("notes", activity)
        self.assertEqual(list_points.call_args.args[0], "exercise")
        self.assertIn("exercise.interval.civil_start_time", list_points.call_args.kwargs["filter_expr"])


class FitbitDashboardSignalTests(unittest.TestCase):
    def test_fitbit_request_builder_supports_respiratory_rate_range(self):
        request = (
            RequestBuilder("Breathing Rate", "token")
            .with_date_range("2026-09-01", "2026-09-21")
            .build()
        )
        self.assertEqual(
            request["url"],
            "https://api.fitbit.com/1/user/-/br/date/2026-09-01/2026-09-21.json",
        )

    def test_fitbit_request_builder_supports_daily_physical_activity(self):
        request = (
            RequestBuilder("Physical Activity", "token")
            .with_date_range("2026-09-21", "2026-09-21")
            .build()
        )
        self.assertEqual(
            request["url"],
            "https://api.fitbit.com/1/user/-/activities/date/2026-09-21.json",
        )

    def test_fitbit_respiratory_processor_accepts_documented_nested_shape(self):
        processor = ProcessorFactory.get_processor("Breathing Rate")
        frame = processor.to_dataframe(processor.process({
            "br": [{
                "dateTime": "2026-09-21",
                "value": {"fullSleepSummary": {"breathingRate": 13.8}},
            }]
        }))
        self.assertEqual(frame.iloc[0]["respiratory_rate"], 13.8)

    def test_fitbit_authorization_preserves_configured_scopes_and_adds_respiratory(self):
        config = {
            "FITBIT_CLIENT_ID": "client",
            "FITBIT_CLIENT_SECRET": "secret",
            "FITBIT_REDIRECT_URI": "https://app.admontracker.online/?fitbit_callback=1",
            "FITBIT_SCOPES": "activity heartrate sleep profile settings",
        }
        with patch.dict("os.environ", {}, clear=True), patch(
            "utils.fitbit_oauth.get_secrets", return_value=config
        ):
            query = parse_qs(urlsplit(build_authorize_url("state-1")).query)
        scopes = set(query["scope"][0].split())
        self.assertTrue({"activity", "heartrate", "sleep", "profile", "settings"} <= scopes)
        self.assertIn("respiratory_rate", scopes)

    def test_fitbit_missing_coverage_checks_every_requested_day(self):
        watch = Watch(name="FB-1", project="Study", token="token")
        with patch.object(watch, "hr_minutes_one_day", side_effect=[1440, 720]) as minutes:
            rows = watch.find_bad_days("2026-09-20", "2026-09-21")

        self.assertEqual(minutes.call_count, 2)
        self.assertEqual(rows, [{
            "date": "2026-09-21",
            "available_minutes": 720,
            "missing_count": 720,
            "percentage_available": 50.0,
            "percentage_missing": 50.0,
        }])


class DashboardSignalContractTests(unittest.TestCase):
    def test_dashboard_exposes_all_approved_research_signals(self):
        expected = {
            "Heart Rate",
            "Steps",
            "Sleep",
            "Physical Activity",
            "Respiratory Rate",
            "Missing Heart Rate Coverage",
        }
        self.assertEqual(set(SIGNAL_OPTIONS), expected)
        self.assertEqual(set(SIGNAL_MAP), expected)

    def test_google_archive_respiratory_filter_uses_api_field_name(self):
        window = PeriodWindow("2026-09", date(2026, 9, 1), date(2026, 9, 21), "monthly")
        filter_expr = _google_filter("daily-respiratory-rate", "daily", window)
        self.assertEqual(
            filter_expr,
            'dailyRespiratoryRate.date >= "2026-09-01" '
            'AND dailyRespiratoryRate.date < "2026-09-22"',
        )

    def test_google_missing_coverage_uses_google_client_not_fitbit_requests(self):
        health_client = Mock()
        health_client.fetch_raw.return_value = {
            "activities-heart": [{"dateTime": "2026-09-21"}],
            "activities-heart-intraday": {
                "dataset": [
                    {"time": "00:00:00", "value": 70, "datetime": "2026-09-21T00:00:00"},
                    {"time": "00:01:00", "value": 71, "datetime": "2026-09-21T00:01:00"},
                ]
            },
        }
        watch = Watch(name="YN4", project="Yoga", token="", health_client=health_client)
        with patch("entity.Watch.requests.get") as fitbit_get:
            rows = watch.find_bad_days("2026-09-21", "2026-09-21")

        fitbit_get.assert_not_called()
        health_client.fetch_raw.assert_called_once()
        self.assertEqual(rows[0]["available_minutes"], 2)
        self.assertEqual(rows[0]["missing_count"], 1438)


if __name__ == "__main__":
    unittest.main()
