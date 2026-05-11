from __future__ import annotations

from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests


class GoogleHealthClient:
    provider = "google_health"
    BASE_URL = "https://health.googleapis.com/v4"
    DEFAULT_TZ = ZoneInfo("Asia/Jerusalem")

    def __init__(self, *, access_token_provider: Callable[[], str]):
        self.access_token_provider = access_token_provider

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token_provider()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        response = requests.request(
            method=method.upper(),
            url=f"{self.BASE_URL}/{path.lstrip('/')}",
            headers=self._headers(),
            params=params,
            json=json_body,
            timeout=timeout,
        )
        if not response.ok:
            raise RuntimeError(f"Google Health API failed: {response.status_code} {response.text}")
        return response.json() if response.text else {}

    def list_data_points(
        self,
        data_type: str,
        *,
        filter_expr: str | None = None,
        page_size: int = 10000,
    ) -> list[dict[str, Any]]:
        params = {"pageSize": min(page_size, 10000)}
        if filter_expr:
            params["filter"] = filter_expr

        points: list[dict[str, Any]] = []
        while True:
            payload = self.request(
                "GET",
                f"users/me/dataTypes/{data_type}/dataPoints",
                params=params,
            )
            points.extend(payload.get("dataPoints", []))
            next_token = payload.get("nextPageToken")
            if not next_token:
                break
            params["pageToken"] = next_token
        return points

    def rollup(
        self,
        data_type: str,
        *,
        start_time: str,
        end_time: str,
        window_size: str,
        page_size: int = 10000,
        data_source_family: str | None = "users/me/dataSourceFamilies/google-wearables",
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "range": {"startTime": start_time, "endTime": end_time},
            "windowSize": window_size,
            "pageSize": min(page_size, 10000),
        }
        if data_source_family:
            body["dataSourceFamily"] = data_source_family

        points: list[dict[str, Any]] = []
        while True:
            payload = self.request(
                "POST",
                f"users/me/dataTypes/{data_type}/dataPoints:rollUp",
                json_body=body,
            )
            points.extend(payload.get("rollupDataPoints", []))
            next_token = payload.get("nextPageToken")
            if not next_token:
                break
            body["pageToken"] = next_token
        return points

    def daily_rollup(
        self,
        data_type: str,
        *,
        start_date: dict[str, int],
        end_date: dict[str, int],
        window_size_days: int = 1,
        page_size: int = 10000,
        data_source_family: str | None = "users/me/dataSourceFamilies/google-wearables",
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "range": {"start": {"date": start_date}, "end": {"date": end_date}},
            "windowSizeDays": window_size_days,
            "pageSize": min(page_size, 10000),
        }
        if data_source_family:
            body["dataSourceFamily"] = data_source_family

        points: list[dict[str, Any]] = []
        while True:
            payload = self.request(
                "POST",
                f"users/me/dataTypes/{data_type}/dataPoints:dailyRollUp",
                json_body=body,
            )
            points.extend(payload.get("rollupDataPoints", []))
            next_token = payload.get("nextPageToken")
            if not next_token:
                break
            body["pageToken"] = next_token
        return points

    def get_current_hourly_steps(self) -> int | None:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=24)
        points = self.rollup(
            "steps",
            start_time=start_dt.isoformat().replace("+00:00", "Z"),
            end_time=end_dt.isoformat().replace("+00:00", "Z"),
            window_size="60s",
        )
        for point in points:
            value = self._extract_numeric(point, "countSum", "steps")
            if value and value > 0:
                return int(value)
        return None

    def get_current_hourly_hr(self) -> int | float | None:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=24)
        points = self.rollup(
            "heart-rate",
            start_time=start_dt.isoformat().replace("+00:00", "Z"),
            end_time=end_dt.isoformat().replace("+00:00", "Z"),
            window_size="60s",
        )
        for point in points:
            value = self._extract_numeric(
                point,
                "beatsPerMinuteAvg",
                "averageBeatsPerMinute",
                "averageHeartRateBeatsPerMinute",
                "heartRateBeatsPerMinute",
                "beatsPerMinute",
            )
            if value is not None:
                return value
        return None

    def get_last_sleep_start_end(self) -> tuple[str | None, str | None]:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=3)
        filter_expr = (
            f'sleep.interval.start_time >= "{start_dt.isoformat().replace("+00:00", "Z")}" '
            f'AND sleep.interval.end_time <= "{end_dt.isoformat().replace("+00:00", "Z")}"'
        )
        points = self.list_data_points("sleep", filter_expr=filter_expr, page_size=1000)
        sleep_points = sorted(
            points,
            key=lambda item: self._extract_interval(item)[0] or "",
            reverse=True,
        )
        if not sleep_points:
            return None, None
        return self._extract_interval(sleep_points[0])

    def get_last_sleep_duration(self) -> float | None:
        start_time, end_time = self.get_last_sleep_start_end()
        if not start_time or not end_time:
            return None
        try:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            return (end_dt - start_dt).total_seconds() / 3600
        except ValueError:
            return None

    def get_current_battery(self) -> int | None:
        return None

    def fetch_raw(self, data_type: str, **kwargs: Any) -> dict:
        if data_type in {"Heart Rate Intraday", "Steps Intraday"}:
            google_data_type = {
                "Heart Rate Intraday": "heart-rate",
                "Steps Intraday": "steps",
            }[data_type]
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date") or start_date
            start_time = kwargs.get("start_time", "00:00")
            end_time = kwargs.get("end_time", "23:59:59")
            start_iso, end_iso = self._local_interval_to_utc_rfc3339(
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
            )
            points = self.rollup(
                google_data_type,
                start_time=start_iso,
                end_time=end_iso,
                window_size=kwargs.get("window_size", "60s"),
            )
            if data_type == "Heart Rate Intraday":
                return self._fitbit_heart_intraday_payload(points)
            return self._fitbit_steps_intraday_payload(points)

        if data_type in {"steps", "heart-rate"} and {"start_time", "end_time"} <= set(kwargs):
            return {
                "rollupDataPoints": self.rollup(
                    data_type,
                    start_time=kwargs["start_time"],
                    end_time=kwargs["end_time"],
                    window_size=kwargs.get("window_size", "3600s"),
                )
            }
        if data_type == "sleep":
            return {"dataPoints": self.list_data_points("sleep", filter_expr=kwargs.get("filter_expr"))}
        return {"dataPoints": self.list_data_points(data_type, filter_expr=kwargs.get("filter_expr"))}

    @classmethod
    def _as_date(cls, value: str | date | datetime) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value))

    @staticmethod
    def _parse_clock(value: str | dtime | datetime) -> tuple[dtime, int]:
        if isinstance(value, datetime):
            return value.time(), 0
        if isinstance(value, dtime):
            return value, 0

        value = str(value).strip()
        if value == "24:00":
            return dtime(0, 0, 0), 1

        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time(), 0
            except ValueError:
                pass
        raise ValueError(f"Invalid time value: {value}. Use HH:MM or HH:MM:SS.")

    @classmethod
    def _local_interval_to_utc_rfc3339(
        cls,
        *,
        start_date: str | date | datetime,
        start_time: str | dtime | datetime,
        end_date: str | date | datetime | None = None,
        end_time: str | dtime | datetime = "23:59:59",
    ) -> tuple[str, str]:
        s_date = cls._as_date(start_date)
        e_date = cls._as_date(end_date or start_date)
        s_time, s_day_offset = cls._parse_clock(start_time)
        e_time, e_day_offset = cls._parse_clock(end_time)

        s_dt = datetime.combine(s_date + timedelta(days=s_day_offset), s_time, tzinfo=cls.DEFAULT_TZ)
        e_dt = datetime.combine(e_date + timedelta(days=e_day_offset), e_time, tzinfo=cls.DEFAULT_TZ)
        if e_dt <= s_dt:
            e_dt += timedelta(days=1)

        return (
            s_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            e_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        )

    @classmethod
    def _point_local_datetime(cls, point: dict[str, Any]) -> datetime | None:
        start, end = cls._extract_interval(point)
        timestamp = end or start
        if not timestamp:
            return None
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(cls.DEFAULT_TZ)
        except ValueError:
            return None

    @classmethod
    def _fitbit_heart_intraday_payload(cls, points: list[dict[str, Any]]) -> dict[str, Any]:
        dataset = []
        date_time = None
        for point in points:
            value = cls._extract_numeric(
                point,
                "beatsPerMinuteAvg",
                "beatsPerMinute",
                "averageBeatsPerMinute",
                "heartRateBeatsPerMinute",
            )
            local_dt = cls._point_local_datetime(point)
            if value is None or local_dt is None:
                continue
            date_time = date_time or local_dt.date().isoformat()
            dataset.append({
                "time": local_dt.strftime("%H:%M:%S"),
                "value": value,
                "datetime": local_dt.replace(tzinfo=None).isoformat(timespec="seconds"),
            })

        return {
            "activities-heart": [{"dateTime": date_time or ""}],
            "activities-heart-intraday": {
                "dataset": dataset,
                "datasetInterval": 60,
                "datasetType": "minute",
            },
            "_google_health": {
                "rollup_count": len(points),
                "parsed_count": len(dataset),
                "first_keys": list(points[0].keys()) if points else [],
            },
        }

    @classmethod
    def _fitbit_steps_intraday_payload(cls, points: list[dict[str, Any]]) -> dict[str, Any]:
        dataset = []
        date_time = None
        for point in points:
            value = cls._extract_numeric(point, "countSum", "steps")
            local_dt = cls._point_local_datetime(point)
            if value is None or local_dt is None:
                continue
            date_time = date_time or local_dt.date().isoformat()
            dataset.append({
                "time": local_dt.strftime("%H:%M:%S"),
                "value": int(value),
                "datetime": local_dt.replace(tzinfo=None).isoformat(timespec="seconds"),
            })

        return {
            "activities-steps": [{"dateTime": date_time or ""}],
            "activities-steps-intraday": {
                "dataset": dataset,
                "datasetInterval": 60,
                "datasetType": "minute",
            },
            "_google_health": {
                "rollup_count": len(points),
                "parsed_count": len(dataset),
                "first_keys": list(points[0].keys()) if points else [],
            },
        }

    @staticmethod
    def _extract_numeric(payload: dict[str, Any], *preferred_keys: str) -> int | float | None:
        candidates = [payload]
        candidates.extend(value for value in payload.values() if isinstance(value, dict))
        for candidate in candidates:
            for key in preferred_keys:
                value = candidate.get(key)
                if value in (None, ""):
                    continue
                try:
                    return int(value)
                except (TypeError, ValueError):
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        continue
        for candidate in candidates:
            for value in candidate.values():
                if isinstance(value, (int, float)):
                    return value
                if isinstance(value, str):
                    try:
                        return int(value)
                    except ValueError:
                        continue
        return None

    @staticmethod
    def _extract_interval(payload: dict[str, Any]) -> tuple[str | None, str | None]:
        containers = [payload]
        containers.extend(value for value in payload.values() if isinstance(value, dict))
        for container in containers:
            interval = container.get("interval")
            if isinstance(interval, dict):
                start = interval.get("startTime") or interval.get("start_time")
                end = interval.get("endTime") or interval.get("end_time")
                if start or end:
                    return start, end
            start = container.get("startTime") or container.get("start_time")
            end = container.get("endTime") or container.get("end_time")
            if start or end:
                return start, end
        return None, None
