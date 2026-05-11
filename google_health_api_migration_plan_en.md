# Gradual Migration from Fitbit Web API to Google Health API for `fitbitManagment`

**Date:** 2026-05-11  
**Audience:** Eden + the LLM/agent that will modify the codebase.  
**Goal:** Keep the current Fitbit Web API configuration working, while adding a gradual path for Google Health API OAuth, token storage, refresh, re-consent, and data collection.

> Terminology used in this document:
>
> - **Fitbit legacy** = the current Fitbit Web API implementation in the app.
> - **Google Health API** = the new Google API for Fitbit / Google Health data.
> - **Health Connect** is not the target here. Health Connect is mainly for Android on-device integrations. This app is a web / Streamlit / Google Sheets pipeline.

---

## 1. Executive summary

This migration is not a simple URL replacement. Google Health API uses:

```text
Google OAuth
Google Cloud OAuth clients
New base URL
New data type names
New response schemas
Short-lived access tokens
Provider-specific refresh-token behavior
```

Existing Fitbit access tokens and refresh tokens **cannot** be migrated to Google Health API. Each participant / watch / demo account must complete a new OAuth consent flow using the correct Google/Fitbit account.

The correct architecture is a **dual-provider architecture**:

```text
Dashboard / Data update jobs / Watch entity
        ↓
HealthClient interface
        ├── FitbitHealthClient          # existing Fitbit Web API behavior
        └── GoogleHealthClient          # new Google Health API behavior
        ↓
Provider-aware token store
        ├── existing Fitbit token storage
        └── new health_oauth_tokens storage
```

Each participant/watch should have a provider field:

```text
fitbit          → use FitbitHealthClient
google_health   → use GoogleHealthClient
```

The old flow remains active for existing users. New users, demo users, or migrated users will connect through the Google Health API OAuth flow.

---

## 2. Critical facts to design around

### 2.1 Re-consent is mandatory

The old Fitbit tokens do not work with Google Health API.

Every user must open a new connect link, log in with the correct Google/Fitbit account, and approve the requested Google Health API scopes.

Important implication:

```text
The participant name typed inside the app is only your internal identifier.
The actual source of data is the Google account that completes OAuth.
```

Therefore, the app must not assume that `watchName = TW18` means the connected account is correct. The app should store and validate the `healthUserId` returned by Google Health API.

---

### 2.2 Testing mode has a 7-day refresh-token limit

When the OAuth consent screen is in **Testing** mode:

```text
refresh_token expires after about 7 days
```

This applies to demo users and real users. The relevant distinction is not demo vs. real; it is:

```text
OAuth app in Testing mode vs OAuth app in Production mode
```

When the refresh token expires, refresh will usually fail with an error such as:

```text
invalid_grant
Token has been expired or revoked
```

The app must then:

```text
1. Mark the participant as reauth_required
2. Generate a new connect link
3. Show/administer that link to the participant/demo account
4. Save the newly issued tokens after OAuth completes
```

---

### 2.3 There is also a 100-user limit before full review

New/unverified OAuth clients for Google Health API are limited to about 100 users. Supporting more users requires the appropriate Google verification / security review process.

Do not design the system around artificially splitting participants across many Google Cloud projects to bypass this limit. It complicates token management, creates operational risk, and may conflict with Google API terms.

Correct environment split:

```text
dev        → local notebooks and a few test users
staging    → pilot users / demo users
production → verified app and long-term data collection
```

---

### 2.4 Access tokens are short-lived

Google access tokens are short-lived, usually around one hour.

Therefore, the app must not store an access token in the participant registry and assume it will remain valid.

Every API call should use this pattern:

```text
1. Load the latest token row for participant/watch
2. Check access_expires_at
3. If still valid → use access_token
4. If expired → refresh with refresh_token
5. If refresh succeeds → update token storage
6. If refresh fails with invalid_grant → mark reauth_required
```

---

### 2.5 The API does not physically sync the watch

Google Health API reads data that has already reached the user’s Google/Fitbit account.

It does not trigger Bluetooth/Wi-Fi/LTE sync from the physical watch.

Therefore, even when OAuth is valid, data can be missing if:

```text
The watch did not sync to the phone/app
The phone had no internet
The Fitbit/Pixel Watch app was blocked in the background
The wrong Google account completed OAuth
The requested date range is wrong
The requested data type is not available
```

For dashboard monitoring, prefer fields such as:

```text
last_successful_fetch_at
last_data_timestamp
last_auth_error
auth_status
```

Do not assume the API has a direct equivalent of the old Fitbit device sync/battery behavior.

---

### 2.6 Some Fitbit endpoints have no direct replacement

Known practical gaps:

```text
Fitbit /devices.json battery level → no direct stable REST replacement in Google Health API
EDA / stress / skin conductance    → no public Google Health API data type currently
```

Recommended handling:

```text
Fitbit users:
    keep using /devices.json for battery if it works

Google Health users:
    show N/A for battery, or replace battery with last_data_timestamp / last_successful_fetch_at
```

---

## 3. Current app structure to preserve

Based on the current `fitbitManagment` app, the important existing pieces are:

```text
app.py
utils/fitbit_oauth.py
utils/fitbit_token_store.py
entity/Watch.py
model/dataUpdateControl.py
Google Sheets used by the app
```

### 3.1 `app.py`

The app already has a Fitbit OAuth callback flow. Conceptually, it does this:

```text
1. Detect fitbit_callback query parameter
2. Read code + state
3. Verify that state exists and was not used
4. Resolve state → watchName
5. Exchange code for Fitbit tokens
6. Save tokens for watchName
7. Mark state as used
```

This is the correct pattern. Do not delete it. Add a similar Google Health callback path.

---

### 3.2 `utils/fitbit_oauth.py`

Current Fitbit OAuth endpoints look like:

```python
AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"
```

Do not overwrite this module immediately.

Add a new module:

```text
utils/google_health_oauth.py
```

---

### 3.3 `utils/fitbit_token_store.py`

The current storage has Fitbit-oriented tabs such as:

```python
OAUTH_STATES_TAB = "oauth_states"
OAUTH_USED_TAB = "oauth_state_used"
TOKENS_TAB = "fitbit_oauth_tokens"
FITBIT_SHEET = "fitbit"
```

The new system should either:

```text
Option A: add a new utils/health_token_store.py
Option B: extend fitbit_token_store.py to support provider='fitbit' / provider='google_health'
```

Recommended: **Option A**, because it avoids breaking legacy behavior.

---

### 3.4 `entity/Watch.py`

Currently, `Watch.py` contains direct Fitbit endpoint URLs, for example:

```python
URL_DICT = {
    "Sleep": "https://api.fitbit.com/1.2/user/-/sleep/date/{}/{}.json",
    "Steps": "https://api.fitbit.com/1.2/user/-/activities/steps/date/{}/{}.json",
    "Steps Intraday": "https://api.fitbit.com/1/user/-/activities/steps/date/{}/1d/1min/time/{}/{}.json",
    "Heart Rate Intraday": "https://api.fitbit.com/1.2/user/-/activities/heart/date/{}/1d/1sec/time/{}/{}.json",
    "Heart Rate": "https://api.fitbit.com/1/user/-/activities/heart/date/{}/{}.json",
    "HRV Daily": "https://api.fitbit.com/1/user/-/hrv/date/{}/{}/all.json",
    "Breathing Rate": "https://api.fitbit.com/1/user/-/br/date/{}/{}.json",
    "device": "https://api.fitbit.com/1.2/user/-/devices.json",
}
```

This creates strong coupling between the app and Fitbit Web API.

Do not remove this logic in the first step. Wrap it inside a `FitbitHealthClient`, then add a separate `GoogleHealthClient`.

---

## 4. Google Sheets to create or update

Do not delete existing sheets.

Add new sheets with a `health_` prefix, and add a few provider-related columns to the existing `fitbit` sheet.

Recommended sheets:

```text
fitbit                         # existing participant/watch registry; add columns
health_oauth_clients            # Google OAuth client credentials/configuration
health_oauth_states             # one-time OAuth states for connect links
health_oauth_state_used         # replay protection for OAuth states
health_oauth_tokens             # provider-aware user tokens
health_reauth_queue             # users that need re-consent
health_api_logs                 # API/refresh/fetch diagnostics
health_webhook_events           # optional future webhook event log
```

---

## 4.1 Update existing sheet: `fitbit`

The `fitbit` sheet remains the main internal registry for watches/participants.

Add these columns without removing old columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `name` | string | yes | Existing internal watch/participant identifier. |
| `project` | string | yes | Existing project name. |
| `token` | string | no | Legacy Fitbit field. Keep it for old Fitbit users only. Do not use it for Google Health users. |
| `oauth_type` | enum | no | `fitbit` or `google_health`. If empty, treat as `fitbit`. |
| `provider` | enum | no | Optional duplicate of `oauth_type`. Prefer using only one of them. |
| `oauth_client_key` | string | no | Key pointing to `health_oauth_clients`, e.g. `google_health_staging`. |
| `auth_status` | enum | no | `not_connected`, `connected`, `expired`, `reauth_required`, `revoked`, `error`. |
| `health_user_id` | string | no | Google Health API user ID returned by `users/me/identity`. |
| `legacy_fitbit_user_id` | string | no | Fitbit legacy user ID, if available. |
| `last_successful_fetch_at` | ISO datetime | no | Last successful API fetch. |
| `last_data_timestamp` | ISO datetime | no | Timestamp of the newest data point received. |
| `last_auth_error` | string | no | Last OAuth/refresh error message. |
| `reauth_link` | string | no | Latest generated re-consent link. |
| `reauth_link_created_at` | ISO datetime | no | When the reauth link was generated. |

Backward compatibility rule:

```python
oauth_type = row.get("oauth_type") or "fitbit"
```

This ensures old rows keep working.

---

## 4.2 New sheet: `health_oauth_clients`

This sheet stores app-level OAuth client configuration.

It is used so the app can support:

```text
dev / staging / production environments
possibly multiple OAuth clients
legacy Fitbit client and Google Health client in the same app
```

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `client_key` | string | yes | Internal key, e.g. `google_health_staging`. |
| `provider` | enum | yes | `fitbit` or `google_health`. |
| `environment` | enum | yes | `dev`, `staging`, `production`. |
| `client_id` | string | yes | OAuth client ID. |
| `client_secret` | string | yes | OAuth client secret. Sensitive. |
| `credentials_json_raw` | long string | optional | Raw downloaded OAuth client JSON, if you decide to store it as text. Sensitive. |
| `redirect_uri` | string | yes | Redirect URI configured in Google Cloud. |
| `auth_uri` | string | yes | Google: `https://accounts.google.com/o/oauth2/v2/auth`. |
| `token_uri` | string | yes | Google: `https://oauth2.googleapis.com/token`. |
| `scopes` | string | yes | Space-separated scopes. |
| `status` | enum | yes | `active`, `disabled`, `rotated`. |
| `created_at` | ISO datetime | no | Created timestamp. |
| `updated_at` | ISO datetime | no | Last update timestamp. |
| `notes` | string | no | Free text. |

### Security note about `credentials_json_raw`

Storing raw OAuth client credentials in Google Sheets is operationally convenient but risky.

If you choose this approach:

```text
1. Restrict access to the sheet.
2. Use a dedicated service account with least-privilege access.
3. Never print client_secret or credentials_json_raw in logs.
4. Never expose this sheet to regular app users.
5. Rotate client secrets if they are leaked.
6. Prefer production secrets management when moving beyond a pilot.
```

Also distinguish clearly between:

```text
OAuth client credentials:
    identify your application to Google
    normally one per environment, not one per participant

User OAuth tokens:
    access_token / refresh_token issued after a specific user grants consent
    one per participant/account connection
```

You do **not** normally need one OAuth client JSON per participant. Participants produce user tokens after they complete OAuth.

Example row:

```text
client_key: google_health_staging
provider: google_health
environment: staging
client_id: 1234567890-xxxxx.apps.googleusercontent.com
client_secret: GOCSPX-xxxxx
redirect_uri: https://your-streamlit-app.example.com/?google_health_callback=1
auth_uri: https://accounts.google.com/o/oauth2/v2/auth
token_uri: https://oauth2.googleapis.com/token
status: active
```

---

## 4.3 Extended sheet: `oauth_states`

This sheet stores one-time connect-link states.

Each state connects an OAuth callback to a specific internal watch/participant/project.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `state` | string | yes | Random UUID or cryptographically random state. |
| `provider` | enum | yes | `fitbit` or `google_health`. |
| `watchName` | string | yes | Internal watch/participant identifier. |
| `project` | string | yes | Internal project. |
| `oauth_client_key` | string | yes | Which OAuth client should be used. |
| `purpose` | enum | yes | `connect`, `reauth`, `test`. |
| `created_by` | string | no | Admin/user who generated the link. |
| `created_at` | ISO datetime | yes | Creation time. |
| `expires_at` | ISO datetime | yes | State expiration time. Recommended: 24-72 hours. |
| `used` | bool | yes | `TRUE` or `FALSE`. |
| `used_at` | ISO datetime | no | When callback consumed the state. |
| `callback_error` | string | no | Error if callback failed. |

Important rules:

```text
1. A state can be used once only.
2. A state must expire.
3. The callback must reject unknown/expired/used states.
4. Never accept OAuth callback without verifying state.
```

---

## 4.4 New sheet: `health_oauth_state_used`

This is an additional replay-protection table.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `state` | string | yes | Consumed OAuth state. |
| `provider` | enum | yes | `fitbit` or `google_health`. |
| `watchName` | string | yes | Internal participant/watch. |
| `used_at` | ISO datetime | yes | When the state was consumed. |
| `code_hash` | string | no | Optional hash of OAuth code for diagnostics. Do not store raw code. |

You can keep this separate from `health_oauth_states` because it makes replay checks simple.

---

## 4.5 New central sheet: `health_oauth_tokens`

This is the most important new sheet.

It stores provider-aware user tokens.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `token_id` | string | yes | UUID for this token row. |
| `watchName` | string | yes | Internal watch/participant identifier. |
| `project` | string | yes | Internal project. |
| `provider` | enum | yes | `fitbit` or `google_health`. |
| `oauth_client_key` | string | yes | Which OAuth client issued this token. |
| `health_user_id` | string | no | Google Health API user ID. |
| `legacy_fitbit_user_id` | string | no | Fitbit legacy ID, if known. |
| `access_token` | long string | yes | Current access token. Sensitive. |
| `refresh_token` | long string | yes | Refresh token. Highly sensitive. |
| `access_expires_at` | ISO datetime | yes | When access token expires. |
| `refresh_expires_at` | ISO datetime | no | For Google Testing mode, usually about 7 days if provided. |
| `refresh_token_expires_in` | int | no | Raw value from token response if available. |
| `scope` | string | no | Granted scopes. |
| `token_type` | string | no | Usually `Bearer`. |
| `auth_status` | enum | yes | `connected`, `expired`, `reauth_required`, `revoked`, `error`. |
| `last_refresh_at` | ISO datetime | no | Last successful refresh. |
| `last_refresh_error` | string | no | Last refresh error. |
| `created_at` | ISO datetime | yes | Created time. |
| `updated_at` | ISO datetime | yes | Updated time. |
| `is_active` | bool | yes | Active token row for this participant/provider. |

Recommended storage policy:

```text
Option A: update in place
    one active row per watchName + provider

Option B: append-only
    keep old rows, mark old rows is_active = FALSE
    create a new row on each OAuth reauth
```

For debugging and auditability, append-only is safer. For simplicity, update-in-place is easier.

Recommended for your current app: **update-in-place** initially.

---

## 4.6 New sheet: `health_reauth_queue`

This sheet tracks participants that need to reconnect.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `queue_id` | string | yes | UUID. |
| `watchName` | string | yes | Participant/watch. |
| `project` | string | yes | Project. |
| `provider` | enum | yes | Usually `google_health`. |
| `reason` | enum | yes | `refresh_expired`, `invalid_grant`, `revoked`, `missing_refresh_token`, `manual`. |
| `detected_at` | ISO datetime | yes | When issue was detected. |
| `reauth_link` | string | yes | New connect/reauth link. |
| `reauth_state` | string | yes | State associated with link. |
| `status` | enum | yes | `open`, `sent`, `completed`, `ignored`. |
| `completed_at` | ISO datetime | no | When new OAuth completed. |
| `last_error` | string | no | Error details. |

This makes it easy for the admin UI to show:

```text
These users need to reconnect
Here is their current reauth link
```

---

## 4.7 New sheet: `health_api_logs`

This sheet stores operational diagnostics.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `log_id` | string | yes | UUID. |
| `timestamp` | ISO datetime | yes | Time of event. |
| `watchName` | string | no | Participant/watch. |
| `project` | string | no | Project. |
| `provider` | enum | no | `fitbit` or `google_health`. |
| `operation` | string | yes | `oauth_callback`, `refresh`, `fetch_steps`, `fetch_sleep`, etc. |
| `status` | enum | yes | `success`, `error`, `warning`. |
| `http_status` | int | no | API HTTP status. |
| `data_type` | string | no | `steps`, `heart-rate`, `sleep`, etc. |
| `start_time` | string | no | Request start time/range. |
| `end_time` | string | no | Request end time/range. |
| `message` | string | no | Summary. |
| `error` | string | no | Error text. Do not include tokens. |

Never log:

```text
access_token
refresh_token
client_secret
credentials_json_raw
raw OAuth authorization code
```

---

## 4.8 Optional future sheet: `health_webhook_events`

Only needed if you implement Google Health API webhooks later.

Suggested columns:

| Column | Type | Required | Description |
|---|---:|---:|---|
| `event_id` | string | yes | UUID. |
| `received_at` | ISO datetime | yes | When webhook arrived. |
| `provider` | enum | yes | `google_health`. |
| `health_user_id` | string | yes | User ID from webhook. |
| `data_type` | string | yes | Changed data type. |
| `raw_payload` | string | no | JSON string. Sensitive-ish; store carefully. |
| `processed` | bool | yes | Whether the event was processed. |
| `processed_at` | ISO datetime | no | Processing time. |
| `processing_error` | string | no | Error details. |

Start with polling before implementing webhooks.

---

## 5. Recommended Google Health API scopes

For the current app functionality, start with minimal read-only scopes:

```python
GOOGLE_HEALTH_SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
    "https://www.googleapis.com/auth/googlehealth.profile.readonly",
    "https://www.googleapis.com/auth/googlehealth.settings.readonly",
]
```

Do not request scopes that are not needed.

If the app only starts with steps, heart rate, and sleep, then begin with:

```text
activity_and_fitness.readonly
health_metrics_and_measurements.readonly
sleep.readonly
```

Add profile/settings only if you actually use identity/profile/settings calls.

---

## 6. Fitbit data type mapping to Google Health API

| Current Fitbit concept | Google Health API data type | Notes |
|---|---|---|
| Steps daily | `steps` | Use `dailyRollUp`. |
| Steps intraday | `steps` | Use `rollUp` with `windowSize`, e.g. `60s` or `3600s`. |
| Heart Rate daily | `heart-rate` | Use `dailyRollUp` or `rollUp`. |
| Heart Rate intraday | `heart-rate` | Use `rollUp`. |
| Sleep | `sleep` | Use `list`; sleep is a session data type. |
| Sleep levels | `sleep` | Stages are nested in the sleep data point. |
| HRV daily / RMSSD | `daily-heart-rate-variability` | Use `list`. |
| HRV intraday | `heart-rate-variability` | Use `list`. |
| SpO2 daily | `daily-oxygen-saturation` | Use `list`. |
| SpO2 intraday | `oxygen-saturation` | Use `list`. |
| Breathing rate | `daily-respiratory-rate` or `respiratory-rate-sleep-summary` | Choose based on required granularity. |
| Sleep skin temperature | `daily-sleep-temperature-derivations` | Use `list`. |
| Distance | `distance` | Use `rollUp` / `dailyRollUp`. |
| Floors | `floors` | Use `rollUp` / `dailyRollUp`. |
| Active Zone Minutes | `active-zone-minutes` | Use `rollUp` / `dailyRollUp`. |
| Device battery | No direct replacement | Show N/A or last data timestamp for Google users. |
| EDA / Stress | No public data type | Not supported through current public API. |

---

## 7. Recommended file structure

Add these files without deleting the old Fitbit files:

```text
utils/
    fitbit_oauth.py                 # keep existing
    google_health_oauth.py          # new
    health_oauth_clients.py         # reads OAuth client config from Sheets
    health_token_store.py           # provider-aware token/state storage

services/
    health_client.py                # shared Protocol/interface
    fitbit_health_client.py         # wrapper around existing Fitbit behavior
    google_health_client.py         # new Google Health API client
    health_client_factory.py        # chooses provider per participant

model/
    dataUpdateControl.py            # update to use HealthClientFactory

entity/
    Watch.py                        # make it thinner; delegate API calls to client
```

Implementation rule:

```text
Do not remove Fitbit code first.
Add Google support next to it.
Then move callers gradually to the provider-aware abstraction.
```

---

## 8. `HealthClient` interface

Create:

```text
services/health_client.py
```

Example:

```python
from __future__ import annotations

from typing import Protocol, Any


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

    def fetch_raw(self, data_type: str, **kwargs: Any) -> dict:
        ...
```

Then `Watch` can become provider-agnostic:

```python
class Watch:
    def __init__(self, name: str, project: str, health_client):
        self.name = name
        self.project = project
        self.health_client = health_client

    def get_current_hourly_HR(self, force_fetch: bool = False):
        return self.health_client.get_current_hourly_hr()

    def get_current_hourly_steps(self, force_fetch: bool = False):
        return self.health_client.get_current_hourly_steps()

    def get_current_battery(self, force_fetch: bool = False):
        return self.health_client.get_current_battery()

    def get_last_sleep_start_end(self, force_fetch: bool = False):
        return self.health_client.get_last_sleep_start_end()

    def get_last_sleep_duration(self, force_fetch: bool = False):
        return self.health_client.get_last_sleep_duration()
```

---

## 9. OAuth client configuration from Google Sheets

Create:

```text
utils/health_oauth_clients.py
```

Purpose:

```text
Read health_oauth_clients
Select active OAuth client by client_key/provider/environment
Return client_id/client_secret/redirect_uri/scopes/token_uri/auth_uri
```

Skeleton:

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class OAuthClientConfig:
    client_key: str
    provider: str
    environment: str
    client_id: str
    client_secret: str
    redirect_uri: str
    auth_uri: str
    token_uri: str
    scopes: list[str]
    credentials_json_raw: str | None = None


def parse_scopes(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return value
    return [s.strip() for s in str(value).split() if s.strip()]


def get_oauth_client_config(spreadsheet, client_key: str) -> OAuthClientConfig:
    rows = spreadsheet.get_sheet_as_records("health_oauth_clients")

    matches = [
        row for row in rows
        if row.get("client_key") == client_key and row.get("status", "active") == "active"
    ]

    if not matches:
        raise ValueError(f"No active OAuth client config found for client_key={client_key}")

    row = matches[0]

    # If raw JSON is stored, it can be parsed here. Otherwise use explicit columns.
    raw = row.get("credentials_json_raw") or None
    client_id = row.get("client_id")
    client_secret = row.get("client_secret")
    auth_uri = row.get("auth_uri") or "https://accounts.google.com/o/oauth2/v2/auth"
    token_uri = row.get("token_uri") or "https://oauth2.googleapis.com/token"

    if raw:
        parsed = json.loads(raw)
        web = parsed.get("web") or parsed.get("installed") or {}
        client_id = client_id or web.get("client_id")
        client_secret = client_secret or web.get("client_secret")
        auth_uri = auth_uri or web.get("auth_uri")
        token_uri = token_uri or web.get("token_uri")

    return OAuthClientConfig(
        client_key=row["client_key"],
        provider=row["provider"],
        environment=row.get("environment", "staging"),
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=row["redirect_uri"],
        auth_uri=auth_uri,
        token_uri=token_uri,
        scopes=parse_scopes(row["scopes"]),
        credentials_json_raw=raw,
    )
```

Replace `spreadsheet.get_sheet_as_records` with the actual helper used in your app.

---

## 10. Google OAuth module

Create:

```text
utils/google_health_oauth.py
```

Skeleton:

```python
from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlencode

import requests


GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_HEALTH_BASE_URL = "https://health.googleapis.com/v4"


def now_ts() -> int:
    return int(time.time())


def build_google_health_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    state: str,
    prompt: str = "consent",
) -> str:
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        "state": state,
        "access_type": "offline",
        "prompt": prompt,
    }

    return f"{GOOGLE_AUTH_URI}?{urlencode(params)}"


def exchange_code_for_google_health_tokens(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> dict[str, Any]:
    response = requests.post(
        GOOGLE_TOKEN_URI,
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(f"Google token exchange failed: {response.status_code} {response.text}")

    token_response = response.json()
    return normalize_google_token_response(token_response)


def refresh_google_health_tokens(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
) -> dict[str, Any]:
    response = requests.post(
        GOOGLE_TOKEN_URI,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(f"Google token refresh failed: {response.status_code} {response.text}")

    token_response = response.json()
    normalized = normalize_google_token_response(token_response)

    # Google refresh responses may omit refresh_token. Keep the existing one.
    if not normalized.get("refresh_token"):
        normalized["refresh_token"] = refresh_token

    return normalized


def normalize_google_token_response(token_response: dict[str, Any]) -> dict[str, Any]:
    now = now_ts()
    expires_in = int(token_response.get("expires_in", 3599))
    refresh_expires_in = token_response.get("refresh_token_expires_in")

    refresh_expires_at = None
    if refresh_expires_in is not None:
        refresh_expires_at = now + int(refresh_expires_in)

    return {
        "access_token": token_response.get("access_token"),
        "refresh_token": token_response.get("refresh_token"),
        "access_expires_at": now + expires_in,
        "refresh_expires_at": refresh_expires_at,
        "refresh_token_expires_in": refresh_expires_in,
        "scope": token_response.get("scope"),
        "token_type": token_response.get("token_type", "Bearer"),
        "raw": token_response,
    }


def get_google_health_identity(access_token: str) -> dict[str, Any]:
    response = requests.get(
        f"{GOOGLE_HEALTH_BASE_URL}/users/me/identity",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(f"Google Health identity failed: {response.status_code} {response.text}")

    return response.json()
```

Important: do not pass Python `True` into `include_granted_scopes`; Google expects lowercase `true`/`false` in the URL. For this app, you can omit `include_granted_scopes` entirely.

---

## 11. Provider-aware token store

Create:

```text
utils/health_token_store.py
```

### 11.1 Basic helpers

```python
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def new_uuid() -> str:
    return str(uuid.uuid4())


def ts_to_iso(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
```

---

### 11.2 Saving OAuth state

```python
def save_oauth_state(
    spreadsheet,
    *,
    state: str,
    provider: str,
    watchName: str,
    project: str,
    oauth_client_key: str,
    purpose: str = "connect",
    created_by: str | None = None,
    ttl_hours: int = 48,
) -> None:
    now = utc_now()
    expires_at = now + timedelta(hours=ttl_hours)

    row = {
        "state": state,
        "provider": provider,
        "watchName": watchName,
        "project": project,
        "oauth_client_key": oauth_client_key,
        "purpose": purpose,
        "created_by": created_by or "",
        "created_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "used": "FALSE",
        "used_at": "",
        "callback_error": "",
    }

    spreadsheet.append_row("health_oauth_states", row)
```

---

### 11.3 Resolving state and preventing replay

```python
def resolve_oauth_state(spreadsheet, *, state: str, provider: str) -> dict[str, Any]:
    rows = spreadsheet.get_sheet_as_records("health_oauth_states")
    matches = [row for row in rows if row.get("state") == state and row.get("provider") == provider]

    if not matches:
        raise ValueError("Unknown OAuth state")

    row = matches[-1]

    if str(row.get("used", "FALSE")).upper() == "TRUE":
        raise ValueError("OAuth state was already used")

    expires_at = iso_to_dt(row.get("expires_at"))
    if expires_at and utc_now() > expires_at:
        raise ValueError("OAuth state expired")

    used_rows = spreadsheet.get_sheet_as_records("health_oauth_state_used")
    if any(r.get("state") == state and r.get("provider") == provider for r in used_rows):
        raise ValueError("OAuth state replay detected")

    return row


def mark_oauth_state_used(spreadsheet, *, state_row: dict[str, Any]) -> None:
    used_row = {
        "state": state_row["state"],
        "provider": state_row["provider"],
        "watchName": state_row["watchName"],
        "used_at": utc_now_iso(),
        "code_hash": "",
    }

    spreadsheet.append_row("health_oauth_state_used", used_row)

    # Also update health_oauth_states.used = TRUE if your sheet helper supports row updates.
```

---

### 11.4 Saving tokens

```python
def save_google_health_tokens_for_watch(
    spreadsheet,
    *,
    watchName: str,
    project: str,
    oauth_client_key: str,
    token_data: dict[str, Any],
    identity: dict[str, Any] | None = None,
) -> None:
    health_user_id = None
    legacy_fitbit_user_id = None

    if identity:
        health_user_id = identity.get("healthUserId")
        legacy_fitbit_user_id = identity.get("legacyUserId")

    row = {
        "token_id": new_uuid(),
        "watchName": watchName,
        "project": project,
        "provider": "google_health",
        "oauth_client_key": oauth_client_key,
        "health_user_id": health_user_id or "",
        "legacy_fitbit_user_id": legacy_fitbit_user_id or "",
        "access_token": token_data.get("access_token") or "",
        "refresh_token": token_data.get("refresh_token") or "",
        "access_expires_at": ts_to_iso(token_data.get("access_expires_at")),
        "refresh_expires_at": ts_to_iso(token_data.get("refresh_expires_at")),
        "refresh_token_expires_in": token_data.get("refresh_token_expires_in") or "",
        "scope": token_data.get("scope") or "",
        "token_type": token_data.get("token_type") or "Bearer",
        "auth_status": "connected",
        "last_refresh_at": "",
        "last_refresh_error": "",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "is_active": "TRUE",
    }

    # Recommended initial approach: update existing active row if present, otherwise append.
    upsert_active_token_row(spreadsheet, row)

    update_fitbit_registry_after_auth(
        spreadsheet,
        watchName=watchName,
        project=project,
        provider="google_health",
        health_user_id=health_user_id,
        legacy_fitbit_user_id=legacy_fitbit_user_id,
        auth_status="connected",
    )
```

`upsert_active_token_row` should be implemented using your existing Google Sheets helper methods.

---

### 11.5 Getting a valid access token

```python
def get_active_token_row(spreadsheet, *, watchName: str, provider: str) -> dict[str, Any] | None:
    rows = spreadsheet.get_sheet_as_records("health_oauth_tokens")

    matches = [
        row for row in rows
        if row.get("watchName") == watchName
        and row.get("provider") == provider
        and str(row.get("is_active", "TRUE")).upper() == "TRUE"
    ]

    if not matches:
        return None

    return matches[-1]


def is_access_token_valid(token_row: dict[str, Any], safety_margin_minutes: int = 5) -> bool:
    expires_at = iso_to_dt(token_row.get("access_expires_at"))
    if not expires_at:
        return False

    return utc_now() < expires_at - timedelta(minutes=safety_margin_minutes)
```

Provider-aware valid token function:

```python
def get_valid_access_token(
    spreadsheet,
    *,
    watchName: str,
    provider: str,
    oauth_client_config,
) -> str:
    token_row = get_active_token_row(spreadsheet, watchName=watchName, provider=provider)

    if not token_row:
        mark_reauth_required(
            spreadsheet,
            watchName=watchName,
            project="",
            provider=provider,
            reason="missing_token",
        )
        raise RuntimeError(f"No active token found for {watchName}/{provider}")

    if is_access_token_valid(token_row):
        return token_row["access_token"]

    refresh_token = token_row.get("refresh_token")
    if not refresh_token:
        mark_reauth_required(
            spreadsheet,
            watchName=watchName,
            project=token_row.get("project", ""),
            provider=provider,
            reason="missing_refresh_token",
        )
        raise RuntimeError("Missing refresh token")

    try:
        if provider == "google_health":
            from utils.google_health_oauth import refresh_google_health_tokens

            new_token_data = refresh_google_health_tokens(
                refresh_token=refresh_token,
                client_id=oauth_client_config.client_id,
                client_secret=oauth_client_config.client_secret,
            )
        else:
            raise NotImplementedError(f"Refresh not implemented for provider={provider}")

        update_token_row_after_refresh(spreadsheet, token_row, new_token_data)
        return new_token_data["access_token"]

    except Exception as e:
        error_text = str(e)

        if "invalid_grant" in error_text or "expired" in error_text or "revoked" in error_text:
            mark_reauth_required(
                spreadsheet,
                watchName=watchName,
                project=token_row.get("project", ""),
                provider=provider,
                reason="invalid_grant",
                error=error_text,
            )

        raise
```

---

### 11.6 Marking reauth required and creating queue entries

```python
def mark_reauth_required(
    spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    reason: str,
    error: str | None = None,
    reauth_link: str | None = None,
    reauth_state: str | None = None,
) -> None:
    queue_row = {
        "queue_id": new_uuid(),
        "watchName": watchName,
        "project": project,
        "provider": provider,
        "reason": reason,
        "detected_at": utc_now_iso(),
        "reauth_link": reauth_link or "",
        "reauth_state": reauth_state or "",
        "status": "open",
        "completed_at": "",
        "last_error": error or "",
    }

    spreadsheet.append_row("health_reauth_queue", queue_row)

    update_fitbit_registry_auth_status(
        spreadsheet,
        watchName=watchName,
        auth_status="reauth_required",
        last_auth_error=error or reason,
        reauth_link=reauth_link or "",
    )
```

---

### 11.7 Updating the `fitbit` registry

```python
def update_fitbit_registry_after_auth(
    spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    health_user_id: str | None,
    legacy_fitbit_user_id: str | None,
    auth_status: str,
) -> None:
    # Implement using your existing Google Sheets update helper.
    # Match row by name/watchName and project.
    updates = {
        "oauth_type": provider,
        "provider": provider,
        "auth_status": auth_status,
        "health_user_id": health_user_id or "",
        "legacy_fitbit_user_id": legacy_fitbit_user_id or "",
        "last_auth_error": "",
    }

    spreadsheet.update_row_by_keys(
        sheet_name="fitbit",
        keys={"name": watchName, "project": project},
        updates=updates,
    )
```

If your helper does not support `update_row_by_keys`, implement it once and reuse it.

---

## 12. Creating a connect link in the app

The current app has an OAuth Connect UI for Fitbit. Extend it to support provider selection.

### 12.1 Recommended UI fields

```text
watchName / participant name
project
provider: fitbit / google_health
oauth_client_key: google_health_staging / google_health_prod
purpose: connect / reauth
```

For backward compatibility, default provider can remain `fitbit` for existing workflows.

### 12.2 Generic connect-link function

```python
from uuid import uuid4


def create_health_connect_link(
    spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    oauth_client_key: str,
    purpose: str = "connect",
    created_by: str | None = None,
) -> str:
    state = str(uuid4())

    save_oauth_state(
        spreadsheet,
        state=state,
        provider=provider,
        watchName=watchName,
        project=project,
        oauth_client_key=oauth_client_key,
        purpose=purpose,
        created_by=created_by,
    )

    if provider == "google_health":
        from utils.health_oauth_clients import get_oauth_client_config
        from utils.google_health_oauth import build_google_health_authorize_url

        cfg = get_oauth_client_config(spreadsheet, oauth_client_key)

        return build_google_health_authorize_url(
            client_id=cfg.client_id,
            redirect_uri=cfg.redirect_uri,
            scopes=cfg.scopes,
            state=state,
        )

    if provider == "fitbit":
        # Use existing Fitbit connect-link builder.
        return build_fitbit_authorize_url(state=state, watchName=watchName)

    raise ValueError(f"Unsupported provider: {provider}")
```

Important:

```text
Creating the link does not save a user token.
The token is saved only after the participant opens the link, logs in, grants consent, and the callback receives code + state.
```

---

## 13. General callback in `app.py`

### 13.1 Add `handle_google_health_callback`

Do not replace the Fitbit callback at first.

Add a new callback handler:

```python
def handle_google_health_callback(auth_controller):
    query_params = st.query_params

    if "google_health_callback" not in query_params:
        return False

    code = query_params.get("code")
    state = query_params.get("state")
    error = query_params.get("error")

    if error:
        st.error(f"Google Health OAuth failed: {error}")
        return True

    if not code or not state:
        st.error("Missing Google Health OAuth code/state")
        return True

    try:
        state_row = resolve_oauth_state(
            SP,
            state=state,
            provider="google_health",
        )

        oauth_client_key = state_row["oauth_client_key"]
        watchName = state_row["watchName"]
        project = state_row["project"]

        cfg = get_oauth_client_config(SP, oauth_client_key)

        token_data = exchange_code_for_google_health_tokens(
            code=code,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            redirect_uri=cfg.redirect_uri,
        )

        identity = get_google_health_identity(token_data["access_token"])

        save_google_health_tokens_for_watch(
            SP,
            watchName=watchName,
            project=project,
            oauth_client_key=oauth_client_key,
            token_data=token_data,
            identity=identity,
        )

        mark_oauth_state_used(SP, state_row=state_row)

        st.success(f"Google Health connected successfully for {watchName}")
        st.query_params.clear()
        return True

    except Exception as e:
        st.error(f"Google Health OAuth callback failed: {e}")
        log_health_api_event(
            SP,
            provider="google_health",
            operation="oauth_callback",
            status="error",
            message=str(e),
        )
        return True
```

Then in `app.py`, call this before normal login/app rendering, similar to the Fitbit callback:

```python
if handle_google_health_callback(auth_controller):
    st.stop()

if handle_fitbit_callback(auth_controller):
    st.stop()
```

### 13.2 Important callback rule

The callback must be accessible to participants who are not logged into the admin UI.

The participant only needs to complete OAuth. They should not need access to the management dashboard.

---

## 14. Google Health API client

Create:

```text
services/google_health_client.py
```

Skeleton:

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests


class GoogleHealthClient:
    provider = "google_health"
    BASE_URL = "https://health.googleapis.com/v4"

    def __init__(self, *, access_token_provider):
        """
        access_token_provider: callable that returns a valid access token.
        It should refresh automatically if needed.
        """
        self.access_token_provider = access_token_provider

    def _headers(self) -> dict[str, str]:
        access_token = self.access_token_provider()
        return {
            "Authorization": f"Bearer {access_token}",
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
        url = f"{self.BASE_URL}/{path.lstrip('/')}"

        response = requests.request(
            method=method.upper(),
            url=url,
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

        points = []

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
        body = {
            "range": {
                "startTime": start_time,
                "endTime": end_time,
            },
            "windowSize": window_size,
            "pageSize": min(page_size, 10000),
        }

        if data_source_family:
            body["dataSourceFamily"] = data_source_family

        points = []

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
        body = {
            "range": {
                "start": {"date": start_date},
                "end": {"date": end_date},
            },
            "windowSizeDays": window_size_days,
            "pageSize": min(page_size, 10000),
        }

        if data_source_family:
            body["dataSourceFamily"] = data_source_family

        points = []

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

    def get_current_battery(self) -> int | None:
        # No direct replacement for Fitbit /devices.json battery at this stage.
        return None
```

Then implement convenience methods:

```python
    def get_current_hourly_steps(self) -> int | None:
        # Use rollup(data_type="steps", window_size="3600s") over recent interval.
        ...

    def get_current_hourly_hr(self) -> float | None:
        # Use rollup(data_type="heart-rate", window_size="3600s") over recent interval.
        ...

    def get_last_sleep_start_end(self) -> tuple[str | None, str | None]:
        # Use list_data_points(data_type="sleep", filter_expr=...).
        ...

    def get_last_sleep_duration(self) -> float | None:
        # Extract sleep.summary.minutesAsleep or compute from interval.
        ...
```

---

## 15. Fitbit client wrapper

Create:

```text
services/fitbit_health_client.py
```

This wrapper should use the existing Fitbit logic from `Watch.py` or the current request functions.

Example outline:

```python
class FitbitHealthClient:
    provider = "fitbit"

    def __init__(self, *, access_token: str):
        self.access_token = access_token

    def get_current_hourly_hr(self):
        # call existing Fitbit implementation
        ...

    def get_current_hourly_steps(self):
        # call existing Fitbit implementation
        ...

    def get_last_sleep_start_end(self):
        # call existing Fitbit implementation
        ...

    def get_last_sleep_duration(self):
        # call existing Fitbit implementation
        ...

    def get_current_battery(self):
        # Fitbit /devices.json is still available for legacy Fitbit users
        ...

    def fetch_raw(self, data_type: str, **kwargs):
        # call existing URL_DICT behavior
        ...
```

Do not refactor all Fitbit parsing immediately. First isolate it behind this wrapper.

---

## 16. HealthClientFactory

Create:

```text
services/health_client_factory.py
```

Purpose:

```text
Given a row from the fitbit registry, return the correct client.
```

Skeleton:

```python
from services.fitbit_health_client import FitbitHealthClient
from services.google_health_client import GoogleHealthClient
from utils.health_oauth_clients import get_oauth_client_config
from utils.health_token_store import get_valid_access_token


class HealthClientFactory:
    @staticmethod
    def from_watch_row(spreadsheet, row: dict):
        watchName = row.get("name") or row.get("watchName")
        provider = row.get("oauth_type") or row.get("provider") or "fitbit"

        if provider == "fitbit":
            # Legacy behavior: use old token column or old Fitbit token store.
            access_token = row.get("token")
            return FitbitHealthClient(access_token=access_token)

        if provider == "google_health":
            oauth_client_key = row.get("oauth_client_key") or "google_health_staging"
            cfg = get_oauth_client_config(spreadsheet, oauth_client_key)

            def token_provider() -> str:
                return get_valid_access_token(
                    spreadsheet,
                    watchName=watchName,
                    provider="google_health",
                    oauth_client_config=cfg,
                )

            return GoogleHealthClient(access_token_provider=token_provider)

        raise ValueError(f"Unsupported provider: {provider}")
```

This is the main compatibility layer.

---

## 17. Updating `dataUpdateControl.py`

Current behavior likely does something like:

```python
watch = Watch(
    name=row.get("name", ""),
    project=row.get("project", ""),
    token=row.get("token", ""),
)
```

Replace it with:

```python
from services.health_client_factory import HealthClientFactory


health_client = HealthClientFactory.from_watch_row(SP, row)

watch = Watch(
    name=row.get("name", ""),
    project=row.get("project", ""),
    health_client=health_client,
)
```

Then these calls remain conceptually the same:

```python
watch.get_current_battery()
watch.get_current_hourly_HR()
watch.get_current_hourly_steps()
watch.get_last_sleep_start_end()
watch.get_last_sleep_duration()
```

But internally they go through the correct provider.

For Google Health users:

```text
battery → None / N/A
steps → GoogleHealthClient
heart rate → GoogleHealthClient
sleep → GoogleHealthClient
```

For Fitbit legacy users:

```text
battery → Fitbit /devices.json
steps → Fitbit Web API
heart rate → Fitbit Web API
sleep → Fitbit Web API
```

---

## 18. Automatic refresh and validation

The app needs a periodic validation job.

This can be triggered by:

```text
A button in the Streamlit admin UI
A scheduled job outside Streamlit
A lightweight cron process
A manual notebook during early development
```

### 18.1 What the validation job does

For each active Google Health token:

```text
1. Load token row
2. Check refresh_expires_at if available
3. If refresh token expires soon → mark reauth_required and generate link
4. If access token expired → try refresh
5. If refresh succeeds → update token row
6. If refresh fails → mark reauth_required and generate link
7. Log result to health_api_logs
```

Skeleton:

```python
def validate_google_health_tokens(spreadsheet):
    rows = spreadsheet.get_sheet_as_records("health_oauth_tokens")

    for row in rows:
        if row.get("provider") != "google_health":
            continue
        if str(row.get("is_active", "TRUE")).upper() != "TRUE":
            continue

        watchName = row["watchName"]
        project = row.get("project", "")
        refresh_expires_at = iso_to_dt(row.get("refresh_expires_at"))

        if refresh_expires_at and utc_now() >= refresh_expires_at - timedelta(hours=12):
            link, state = create_reauth_link_for_watch(
                spreadsheet,
                watchName=watchName,
                project=project,
                provider="google_health",
                oauth_client_key=row.get("oauth_client_key"),
            )

            mark_reauth_required(
                spreadsheet,
                watchName=watchName,
                project=project,
                provider="google_health",
                reason="refresh_expiring",
                reauth_link=link,
                reauth_state=state,
            )
            continue

        # Otherwise attempt token refresh if needed.
```

### 18.2 Automatic reauth-link creation

```python
def create_reauth_link_for_watch(
    spreadsheet,
    *,
    watchName: str,
    project: str,
    provider: str,
    oauth_client_key: str,
) -> tuple[str, str]:
    state = str(uuid4())

    save_oauth_state(
        spreadsheet,
        state=state,
        provider=provider,
        watchName=watchName,
        project=project,
        oauth_client_key=oauth_client_key,
        purpose="reauth",
    )

    cfg = get_oauth_client_config(spreadsheet, oauth_client_key)

    link = build_google_health_authorize_url(
        client_id=cfg.client_id,
        redirect_uri=cfg.redirect_uri,
        scopes=cfg.scopes,
        state=state,
        prompt="consent",
    )

    return link, state
```

### 18.3 When to mark reauth proactively

Mark `reauth_required` when:

```text
refresh_token is missing
refresh_expires_at is within 12-24 hours
refresh fails with invalid_grant
Google returns token revoked / expired
user disconnected consent
required scopes are missing
```

---

## 19. Reauth pipeline for participants / demo users

Recommended pipeline:

```text
1. Validation job detects token issue
2. App creates a new OAuth state
3. App creates a new Google Health connect link
4. App writes the link to:
       fitbit.reauth_link
       health_reauth_queue.reauth_link
5. Admin sends/opens the link for the relevant participant/demo account
6. Participant logs in with the correct account
7. Callback exchanges code for new tokens
8. Token store is updated
9. auth_status becomes connected
10. reauth_queue row becomes completed
```

For demo users, this means logging into the specific demo Google account.

For real/non-demo users, this means the participant must complete the same flow with their own Google account.

Important:

```text
The admin cannot authorize once and get access to all users.
Each account must grant consent separately.
```

---

## 20. Preventing connection to the wrong account

Because the participant name is internal, a user could accidentally connect the wrong Google account.

Add validation after callback:

```text
1. Call users/me/identity
2. Store healthUserId and legacyUserId
3. If this watchName already has a different healthUserId, warn or block
4. If legacy Fitbit ID is known, compare it when possible
```

Recommended policy:

```python
if existing_health_user_id and existing_health_user_id != new_health_user_id:
    # Do not silently overwrite.
    # Mark as warning or require admin confirmation.
```

For demo accounts, maintain a mapping sheet if needed:

```text
watchName → expected_google_email or expected_legacy_fitbit_user_id
```

Do not store personal emails unless necessary and approved by your privacy/data policy.

---

## 21. Polling vs webhooks

### 21.1 Start with polling

For this app, start with polling.

Recommended polling strategy:

```text
Every 1-3 hours:
    For each connected Google Health participant:
        Refresh access token if needed
        Fetch the last 24-48 hours of steps / heart-rate / sleep
        Upsert into your existing data destination
        Update last_successful_fetch_at and last_data_timestamp
```

Why fetch 24-48 hours instead of only “now”?

```text
Health data can arrive late after the watch syncs.
Sleep and HRV can appear after processing delays.
Backfilling recent windows reduces missing data.
```

### 21.2 Add webhooks later

Webhooks can reduce polling, but they require:

```text
Public HTTPS endpoint
Fast 204 No Content response
Mapping healthUserId → participant/watch
Follow-up API call to fetch data
Secure event logging
```

Do not start with webhooks unless polling is already stable.

---

## 22. Google Health request patterns

### 22.1 Base URL

```text
https://health.googleapis.com/v4
```

### 22.2 `list`

Endpoint:

```text
GET /v4/users/me/dataTypes/{dataType}/dataPoints
```

Use for:

```text
sleep
heart-rate-variability
oxygen-saturation
daily-heart-rate-variability
daily-respiratory-rate
daily-sleep-temperature-derivations
```

Example:

```python
points = client.list_data_points(
    "sleep",
    filter_expr='sleep.interval.civil_end_time >= "2026-05-01" AND sleep.interval.civil_end_time < "2026-05-06"',
    page_size=25,
)
```

### 22.3 `rollUp`

Endpoint:

```text
POST /v4/users/me/dataTypes/{dataType}/dataPoints:rollUp
```

Use for intraday/hourly aggregation:

```text
steps
heart-rate
distance
floors
active-zone-minutes
```

Example:

```python
points = client.rollup(
    "steps",
    start_time="2026-05-01T00:00:00Z",
    end_time="2026-05-02T00:00:00Z",
    window_size="60s",
)
```

### 22.4 `dailyRollUp`

Endpoint:

```text
POST /v4/users/me/dataTypes/{dataType}/dataPoints:dailyRollUp
```

Use for daily aggregates.

Important:

```text
start date is inclusive
end date is exclusive
CivilDate fields should be integers, not zero-padded strings
```

Example body:

```python
body = {
    "range": {
        "start": {"date": {"year": 2026, "month": 5, "day": 1}},
        "end": {"date": {"year": 2026, "month": 5, "day": 6}},
    },
    "windowSizeDays": 1,
    "pageSize": 10000,
    "dataSourceFamily": "users/me/dataSourceFamilies/google-wearables",
}
```

For debugging, try omitting `dataSourceFamily` or using:

```text
users/me/dataSourceFamilies/all-sources
```

This helps detect whether data exists from a non-wearable source.

---

## 23. Credential JSON: what to store and what not to store

### 23.1 Important distinction

There are two different credential concepts:

```text
1. OAuth client credentials
   - client_id
   - client_secret
   - redirect_uris
   - auth_uri/token_uri
   - identify the application
   - usually one per environment

2. User OAuth tokens
   - access_token
   - refresh_token
   - access_expires_at
   - refresh_expires_at
   - issued after a specific user completes OAuth
```

You cannot create a user token only by storing the OAuth client JSON.

The user must still open the connect link and approve consent.

### 23.2 Do not create one OAuth client JSON per participant unless necessary

Usually the correct setup is:

```text
One Google Cloud OAuth client for staging
One Google Cloud OAuth client for production
Many participants connect through that same client
Each participant receives their own user tokens
```

The app then stores:

```text
OAuth client config → health_oauth_clients
User token rows     → health_oauth_tokens
```

### 23.3 If you still store raw JSON in a Sheet

Store it in:

```text
health_oauth_clients.credentials_json_raw
```

Do not store it in the participant registry.

Do not log it.

Do not show it in the UI.

Restrict the sheet to the app service account and a minimal number of admins.

---

## 24. Maintaining Fitbit legacy support

The app must support both configurations at the same time.

Rules:

```text
1. Existing rows with empty oauth_type continue as Fitbit legacy.
2. Existing Fitbit token storage remains readable.
3. New Google Health rows use oauth_type = google_health.
4. Watch/DataUpdate code must choose client through HealthClientFactory.
5. Do not make Google Health required for all users during the first migration phase.
```

### 24.1 Migration states

Recommended participant-level migration states:

```text
fitbit_legacy
    Uses old Fitbit Web API token/flow.

google_link_created
    Admin created connect link but participant has not completed OAuth.

google_connected
    Google Health tokens saved and working.

google_reauth_required
    Refresh token expired/revoked or Testing-mode 7 days passed.

google_error
    Connected but API calls are failing for another reason.
```

These states can be stored in `fitbit.auth_status` and/or a separate migration-status column.

---

## 25. Phased implementation plan

### Phase 0 — Preparation

```text
1. Back up all current Google Sheets.
2. Add new columns to fitbit sheet.
3. Create health_* sheets.
4. Add .gitignore entries for local secrets and tokens.
5. Create Google Cloud OAuth client for dev/staging.
6. Add test users to Google Cloud OAuth consent screen.
```

### Phase 1 — Sheets and stores

```text
1. Implement health_oauth_clients.py.
2. Implement health_token_store.py.
3. Test reading/writing states and token rows.
4. Confirm no existing Fitbit flow is broken.
```

### Phase 2 — New OAuth flow

```text
1. Implement google_health_oauth.py.
2. Add handle_google_health_callback in app.py.
3. Add identity call after token exchange.
4. Save tokens into health_oauth_tokens.
5. Update fitbit registry auth_status.
```

### Phase 3 — Connect link UI

```text
1. Add provider selector.
2. Add oauth_client_key selector or hidden default.
3. Generate Google Health connect links.
4. Show link in UI.
5. Test with one demo user.
```

### Phase 4 — Minimal GoogleHealthClient

Implement only:

```text
steps
heart-rate
sleep
```

Do not start with all data types.

### Phase 5 — HealthClientFactory

```text
1. Add FitbitHealthClient wrapper.
2. Add GoogleHealthClient.
3. Add HealthClientFactory.
4. Update dataUpdateControl.py to use factory.
5. Keep old Fitbit behavior working.
```

### Phase 6 — Refresh and reauth

```text
1. Implement get_valid_access_token.
2. Implement refresh flow.
3. Implement invalid_grant handling.
4. Implement health_reauth_queue.
5. Add admin view for reauth links.
```

### Phase 7 — Expand data types

After steps/HR/sleep work:

```text
HRV daily
HRV intraday
SpO2
respiratory rate
sleep temperature
active-zone-minutes
floors
distance
```

### Phase 8 — Monitoring and logs

```text
1. Log every refresh failure.
2. Log every API fetch failure.
3. Track last_successful_fetch_at.
4. Track last_data_timestamp.
5. Show auth_status in dashboard.
```

---

## 26. Recommended manual tests

### 26.1 Demo OAuth test

```text
1. Add demo Google account as test user in Google Cloud.
2. Create connect link for watchName=demo_001.
3. Open link while logged into demo account.
4. Approve consent.
5. Confirm health_oauth_tokens row exists.
6. Confirm fitbit.auth_status = connected.
7. Confirm health_user_id is stored.
```

### 26.2 Backward compatibility test

```text
1. Pick an old Fitbit row with empty oauth_type.
2. Run the dashboard/data update.
3. Confirm it still uses Fitbit legacy API.
4. Confirm battery still works for Fitbit users.
```

### 26.3 Refresh test

```text
1. Manually set access_expires_at to a past time.
2. Run data fetch.
3. Confirm refresh succeeds.
4. Confirm access_expires_at updates.
5. Confirm no reauth is required.
```

### 26.4 Reauth test

```text
1. Corrupt or revoke refresh_token in a test row.
2. Run validation job.
3. Confirm auth_status becomes reauth_required.
4. Confirm health_reauth_queue receives a row.
5. Confirm a new reauth link is created.
6. Open link and reconnect.
7. Confirm status returns to connected.
```

### 26.5 Wrong-account test

```text
1. Create link for demo_001.
2. Open it while logged into a different Google account.
3. Confirm the app detects a changed health_user_id or logs a warning.
4. Do not silently overwrite an existing account mapping.
```

---

## 27. Instructions for the LLM/agent implementing the migration

### 27.1 Do not break legacy

Do not delete or rewrite the old Fitbit flow in the first implementation phase.

Rows with no `oauth_type` must continue to behave as Fitbit users.

```python
provider = row.get("oauth_type") or row.get("provider") or "fitbit"
```

### 27.2 Add abstraction first

The first structural goal is:

```text
Watch/dataUpdateControl no longer knows whether data comes from Fitbit or Google.
```

Use:

```text
HealthClientFactory
FitbitHealthClient
GoogleHealthClient
```

### 27.3 Every callback must validate state

OAuth callback must reject:

```text
missing state
unknown state
expired state
already-used state
provider mismatch
```

Never exchange an OAuth code without first validating state.

### 27.4 Every refresh must handle reauth

If refresh fails with:

```text
invalid_grant
expired
revoked
missing refresh token
```

Then:

```text
mark auth_status = reauth_required
create reauth link
write health_reauth_queue row
log health_api_logs row
```

### 27.5 Credential handling

Never print or log:

```text
client_secret
credentials_json_raw
access_token
refresh_token
OAuth authorization code
```

### 27.6 Date handling

Be careful with dates:

```text
Google dailyRollUp end date is exclusive
Fitbit date ranges may behave differently
Google CivilDate fields should be integers
Intraday rollUp uses physical timestamps, usually RFC3339
Use timezone-aware datetime objects
```

For Israel-based usage, default local timezone may be:

```text
Asia/Jerusalem
```

But API timestamps should be explicit and unambiguous.

---

## 28. Known limitations and open risks

```text
1. Google Health API may still have migration-era changes.
2. Battery/device endpoint replacement is not available in the same way.
3. EDA/stress is not available through public Google Health API data types.
4. Testing-mode refresh tokens expire after 7 days.
5. More than 100 users requires proper Google review/security process.
6. Health data may appear late because watch sync is independent of the API.
7. Wrong-account OAuth is possible unless healthUserId is validated.
8. Google Sheets is convenient but not ideal for storing secrets/tokens long term.
```

---

## 29. Official references

Use these as the primary references when implementing or debugging:

```text
Google Health API main docs
https://developers.google.com/health

Google Health API migration guide
https://developers.google.com/health/migration

Google Health API setup
https://developers.google.com/health/setup

Google Health API data types
https://developers.google.com/health/data-types

Google Health API REST reference
https://developers.google.com/health/reference/rest

Google OAuth 2.0 Web Server flow
https://developers.google.com/identity/protocols/oauth2/web-server

Google OAuth production readiness / verification
https://developers.google.com/identity/protocols/oauth2/production-readiness/policy-compliance
```

---

## 30. Short implementation checklist

```text
[ ] Add new columns to fitbit sheet
[ ] Create health_oauth_clients
[ ] Create health_oauth_states
[ ] Create health_oauth_state_used
[ ] Create health_oauth_tokens
[ ] Create health_reauth_queue
[ ] Create health_api_logs
[ ] Add Google OAuth client config row
[ ] Implement google_health_oauth.py
[ ] Implement health_oauth_clients.py
[ ] Implement health_token_store.py
[ ] Add Google Health callback in app.py
[ ] Add provider selector to connect-link UI
[ ] Save Google tokens after callback
[ ] Store healthUserId / legacyUserId
[ ] Implement GoogleHealthClient for steps
[ ] Implement GoogleHealthClient for heart-rate
[ ] Implement GoogleHealthClient for sleep
[ ] Implement FitbitHealthClient wrapper
[ ] Implement HealthClientFactory
[ ] Update Watch.py to delegate to health_client
[ ] Update dataUpdateControl.py to use HealthClientFactory
[ ] Implement automatic refresh
[ ] Implement invalid_grant → reauth_required
[ ] Implement reauth-link generation
[ ] Add admin view for reauth queue
[ ] Test one Fitbit legacy user
[ ] Test one Google Health demo user
[ ] Test refresh
[ ] Test reauth after broken refresh token
[ ] Add logs and dashboard status fields
```

---

## 31. Minimal success definition

The migration is minimally successful when:

```text
1. Existing Fitbit users still work.
2. A new Google Health demo user can connect through OAuth.
3. The app saves Google Health access/refresh tokens into health_oauth_tokens.
4. The app can fetch steps, heart-rate, and sleep from Google Health API.
5. The app refreshes access tokens automatically.
6. If refresh fails, the app marks reauth_required and creates a new link.
7. dataUpdateControl.py no longer depends directly on Fitbit-only token/URL assumptions.
```

Do not migrate every data type before achieving this minimal success state.
