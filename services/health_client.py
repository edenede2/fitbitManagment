from __future__ import annotations

from typing import Any, Protocol


class HealthClient(Protocol):
    provider: str

    def get_current_hourly_hr(self) -> int | float | None:
        ...

    def get_current_hourly_steps(self) -> int | None:
        ...

    def get_last_sleep_start_end(self) -> tuple[str | None, str | None]:
        ...

    def get_last_sleep_duration(self) -> float | None:
        ...

    def get_current_battery(self) -> int | None:
        ...

    def get_device_details(self) -> dict[str, Any]:
        ...

    def fetch_raw(self, data_type: str, **kwargs: Any) -> dict:
        ...


class FitbitHealthClient:
    provider = "fitbit"

    def __init__(self, *, watch):
        self.watch = watch

    def get_current_hourly_hr(self) -> int | float | None:
        return self.watch.get_current_hourly_HR()

    def get_current_hourly_steps(self) -> int | None:
        return self.watch.get_current_hourly_steps()

    def get_last_sleep_start_end(self) -> tuple[str | None, str | None]:
        return self.watch.get_last_sleep_start_end()

    def get_last_sleep_duration(self) -> float | None:
        return self.watch.get_last_sleep_duration()

    def get_current_battery(self) -> int | None:
        return self.watch.get_current_battery()

    def fetch_raw(self, data_type: str, **kwargs: Any) -> dict:
        return self.watch.fetch_data(data_type, **kwargs)
