#!/usr/bin/env python3
"""Tests for Fitbit Web API endpoint and token recovery behavior."""

import unittest
import sys
import types
from unittest.mock import Mock, patch


def _install_module_stub(name, **attrs):
    module = types.ModuleType(name)
    for attr_name, attr_value in attrs.items():
        setattr(module, attr_name, attr_value)
    sys.modules.setdefault(name, module)
    return sys.modules[name]


try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    _install_module_stub("requests", get=Mock(), post=Mock())

try:
    import pandas  # noqa: F401
except ModuleNotFoundError:
    _install_module_stub("pandas", DataFrame=object, to_datetime=Mock())

try:
    import streamlit  # noqa: F401
except ModuleNotFoundError:
    _install_module_stub("streamlit", session_state={}, secrets={})


class FakeSpreadsheet:
    pass


class FakeGoogleSheetsAdapter:
    pass


_install_module_stub(
    "entity.Sheet",
    Spreadsheet=FakeSpreadsheet,
    GoogleSheetsAdapter=FakeGoogleSheetsAdapter,
)
_install_module_stub("controllers.auth_controller", AuthenticationController=Mock())

from entity.Watch import ApiRequestError, URL_DICT, Watch, WatchFactory
from utils.fitbit_oauth import FitbitOAuthError, refresh_tokens
import utils.fitbit_token_store as fitbit_token_store


class FakeResponse:
    def __init__(self, status_code, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    @property
    def ok(self):
        return 200 <= self.status_code < 400


class FitbitWebApiRecoveryTests(unittest.TestCase):
    def test_confirmed_fitbit_endpoints_use_v1_urls(self):
        self.assertEqual(URL_DICT["device"], "https://api.fitbit.com/1/user/-/devices.json")
        self.assertEqual(
            URL_DICT["Heart Rate Intraday"],
            "https://api.fitbit.com/1/user/-/activities/heart/date/{}/1d/1sec/time/{}/{}.json",
        )

    def test_fetch_data_refreshes_token_once_after_401(self):
        token_refresher = Mock(return_value="new-token")
        watch = Watch(
            name="watch-1",
            project="demo",
            token="old-token",
            token_refresher=token_refresher,
        )

        with patch(
            "entity.Watch.requests.get",
            side_effect=[
                FakeResponse(401, text='{"errors":[{"message":"token expired"}]}'),
                FakeResponse(200, payload=[{"id": "device-1"}]),
            ],
        ) as request_get:
            result = watch.fetch_data("device", force_fetch=True)

        self.assertEqual(result, [{"id": "device-1"}])
        token_refresher.assert_called_once_with()
        self.assertEqual(request_get.call_count, 2)
        self.assertEqual(request_get.call_args_list[0].kwargs["headers"]["Authorization"], "Bearer old-token")
        self.assertEqual(request_get.call_args_list[1].kwargs["headers"]["Authorization"], "Bearer new-token")

    def test_400_raises_clear_sanitized_error(self):
        watch = Watch(name="watch-1", project="demo", token="secret-token-value")

        with patch(
            "entity.Watch.requests.get",
            return_value=FakeResponse(
                400,
                text='{"errors":[{"errorType":"invalid_request","message":"bad endpoint"}]}',
            ),
        ):
            with self.assertRaises(ApiRequestError) as raised:
                watch.fetch_data("device", force_fetch=True)

        error = raised.exception
        self.assertEqual(error.status_code, 400)
        self.assertEqual(error.url_path, "/1/user/-/devices.json")
        self.assertIn("bad endpoint", str(error))
        self.assertNotIn("secret-token-value", str(error))

    def test_force_refresh_ignores_future_expires_at(self):
        with patch.object(
            fitbit_token_store,
            "get_latest_tokens",
            return_value={
                "access_token": "old-access",
                "refresh_token": "refresh-token",
                "expires_at": "9999999999",
            },
        ), patch.object(
            fitbit_token_store,
            "refresh_tokens",
            return_value={
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 28800,
            },
        ) as refresh_tokens, patch.object(
            fitbit_token_store,
            "save_tokens_for_watch",
        ) as save_tokens:
            token = fitbit_token_store.get_valid_access_token(
                object(),
                "watch-1",
                force_refresh=True,
            )

        self.assertEqual(token, "new-access")
        refresh_tokens.assert_called_once_with("refresh-token")
        save_tokens.assert_called_once()

    def test_refresh_token_error_includes_sanitized_fitbit_body(self):
        with patch(
            "utils.fitbit_oauth._cfg",
            return_value=("client-id", "client-secret", "redirect-uri", "settings heartrate"),
        ), patch(
            "utils.fitbit_oauth.requests.post",
            return_value=FakeResponse(
                400,
                text='{"errors":[{"errorType":"invalid_grant","message":"Refresh token invalid: refresh-secret"}]}',
            ),
        ):
            with self.assertRaises(FitbitOAuthError) as raised:
                refresh_tokens("refresh-secret")

        message = str(raised.exception)
        self.assertIn("invalid_grant", message)
        self.assertIn("Refresh token invalid", message)
        self.assertNotIn("refresh-secret", message)
        self.assertNotIn("client-secret", message)

    def test_factory_uses_legacy_token_only_when_no_oauth_row_exists(self):
        fake_spreadsheet = object()

        with patch("controllers.auth_controller.AuthenticationController") as auth_controller, patch(
            "utils.fitbit_token_store.get_latest_tokens",
            return_value=None,
        ) as get_latest_tokens, patch(
            "utils.fitbit_token_store.get_legacy_fitbit_token",
            return_value="legacy-token",
        ) as get_legacy_fitbit_token, patch(
            "utils.fitbit_token_store.get_valid_access_token",
        ) as get_valid_access_token:
            auth_controller.return_value.get_spreadsheet.return_value = fake_spreadsheet
            watch = WatchFactory.create_from_details({"name": "watch-1", "project": "demo"})

        self.assertEqual(watch.token, "legacy-token")
        get_latest_tokens.assert_called_once_with(fake_spreadsheet, "watch-1")
        get_legacy_fitbit_token.assert_called_once_with(fake_spreadsheet, "watch-1")
        get_valid_access_token.assert_not_called()

    def test_factory_uses_supplied_spreadsheet_for_oauth_lookup(self):
        supplied_spreadsheet = object()

        with patch("controllers.auth_controller.AuthenticationController") as auth_controller, patch(
            "utils.fitbit_token_store.get_latest_tokens",
            return_value={"access_token": "old", "refresh_token": "refresh"},
        ) as get_latest_tokens, patch(
            "utils.fitbit_token_store.get_valid_access_token",
            return_value="oauth-token",
        ) as get_valid_access_token:
            watch = WatchFactory.create_from_details(
                {"name": "watch-1", "project": "demo", "token": "stale-token"},
                spreadsheet=supplied_spreadsheet,
            )

        self.assertEqual(watch.token, "oauth-token")
        get_latest_tokens.assert_called_once_with(supplied_spreadsheet, "watch-1")
        get_valid_access_token.assert_called_once_with(supplied_spreadsheet, "watch-1")
        auth_controller.assert_not_called()

    def test_factory_does_not_hide_broken_oauth_row_with_legacy_token(self):
        fake_spreadsheet = object()

        with patch("controllers.auth_controller.AuthenticationController") as auth_controller, patch(
            "utils.fitbit_token_store.get_latest_tokens",
            return_value={"access_token": "old", "refresh_token": "refresh"},
        ), patch(
            "utils.fitbit_token_store.get_valid_access_token",
            side_effect=ValueError("Tokens incomplete for watch 'watch-1'"),
        ), patch(
            "utils.fitbit_token_store.get_legacy_fitbit_token",
            return_value="legacy-token",
        ) as get_legacy_fitbit_token:
            auth_controller.return_value.get_spreadsheet.return_value = fake_spreadsheet
            with self.assertRaises(ValueError):
                WatchFactory.create_from_details(
                    {"name": "watch-1", "project": "demo", "token": "stale-legacy-token"}
                )

        get_legacy_fitbit_token.assert_not_called()


if __name__ == "__main__":
    unittest.main()
