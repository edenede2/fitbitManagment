#!/usr/bin/env python3
"""Build ignored Heroku config vars from local Streamlit and Google credentials.

This command never prints secret values. Its output files must remain ignored by
Git and should be copied only to the Heroku app's Config Vars settings.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import toml


DEFAULT_OIDC_METADATA_URL = "https://accounts.google.com/.well-known/openid-configuration"
CONFIG_VAR_NAME = "STREAMLIT_SECRETS_TOML_B64"
SECRET_REF_VAR_NAME = "STREAMLIT_SECRETS_SECRET_REF"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def _normalize_base_url(value: str) -> str:
    parsed = urlsplit(str(value or "").strip())
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError("The production base URL must be an absolute HTTPS URL")
    if parsed.query or parsed.fragment:
        raise ValueError("The production base URL cannot contain a query or fragment")
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _toml_assignments(values: dict[str, Any]) -> str:
    rendered = toml.dumps(values).rstrip()
    if "\n[" in rendered or rendered.startswith("["):
        raise ValueError("Credential section contains an unexpected nested value")
    return rendered


def _replace_section(source: str, section: str, values: dict[str, Any]) -> str:
    header = f"[{section}]"
    pattern = re.compile(
        rf"(?ms)^\[{re.escape(section)}\][ \t]*\n.*?(?=^\[|\Z)"
    )
    replacement = f"{header}\n{_toml_assignments(values)}\n\n"
    if pattern.search(source):
        return pattern.sub(lambda _: replacement, source, count=1)
    suffix = "" if source.endswith("\n") else "\n"
    return f"{source}{suffix}\n{replacement}"


def _remove_section(source: str, section: str) -> str:
    pattern = re.compile(
        rf"(?ms)^\[{re.escape(section)}\][ \t]*\n.*?(?=^\[|\Z)"
    )
    return pattern.sub("", source, count=1)


def _upsert_root_value(source: str, key: str, value: Any) -> str:
    first_section = re.search(r"(?m)^\[", source)
    root_end = first_section.start() if first_section else len(source)
    root = source[:root_end]
    remainder = source[root_end:]
    assignment = _toml_assignments({key: value})
    pattern = re.compile(rf"(?m)^{re.escape(key)}[ \t]*=.*$")
    if pattern.search(root):
        root = pattern.sub(lambda _: assignment, root, count=1)
    else:
        if root and not root.endswith("\n"):
            root += "\n"
        root += f"{assignment}\n"
    return root + remainder


def build_updated_secrets(
    source: str,
    oauth_document: dict[str, Any],
    service_account: dict[str, Any],
    base_url: str,
) -> tuple[str, dict[str, str]]:
    """Return updated TOML text and non-secret callback metadata."""
    base_url = _normalize_base_url(base_url)
    oauth = oauth_document.get("web") or oauth_document.get("installed")
    if not isinstance(oauth, dict):
        raise ValueError("OAuth JSON must contain a 'web' or 'installed' object")

    for key in ("client_id", "client_secret"):
        if not oauth.get(key):
            raise ValueError(f"OAuth JSON is missing {key}")
    if service_account.get("type") != "service_account":
        raise ValueError("Credential JSON is not a Google service-account credential")
    for key in ("project_id", "private_key", "client_email"):
        if not service_account.get(key):
            raise ValueError(f"Service-account JSON is missing {key}")

    login_redirect = f"{base_url}/oauth2callback"
    fitbit_redirect = f"{base_url}/?fitbit_callback=1"
    health_redirect = f"{base_url}/?google_health_callback=1"

    updated = _upsert_root_value(source, "APP_BASE_URL", base_url)
    updated = _upsert_root_value(updated, "FITBIT_REDIRECT_URI", fitbit_redirect)

    parsed_source = toml.loads(updated)
    existing_auth = parsed_source.get("auth", {})
    cookie_secret = existing_auth.get("cookie_secret") if isinstance(existing_auth, dict) else None
    if not isinstance(cookie_secret, str) or len(cookie_secret) < 32:
        raise ValueError("[auth].cookie_secret must already contain at least 32 characters")

    updated = _replace_section(
        updated,
        "auth",
        {
            "redirect_uri": login_redirect,
            "cookie_secret": cookie_secret,
        },
    )
    updated = _replace_section(
        updated,
        "auth.google",
        {
            "client_id": oauth["client_id"],
            "client_secret": oauth["client_secret"],
            "server_metadata_url": oauth.get("server_metadata_url")
            or DEFAULT_OIDC_METADATA_URL,
        },
    )

    validated = toml.loads(updated)
    if validated["auth"]["redirect_uri"] != login_redirect:
        raise ValueError("Generated Streamlit login redirect URI did not validate")
    # The Google credential is a single Heroku bootstrap value. Do not duplicate
    # the private key inside Streamlit's secret bundle.
    updated = _remove_section(updated, "gcp_service_account")

    callbacks = {
        "login_redirect": login_redirect,
        "fitbit_redirect": fitbit_redirect,
        "google_health_redirect": health_redirect,
    }
    return updated, callbacks


def build_config_vars(
    secrets_toml: str,
    base_url: str,
    service_account: dict[str, Any] | None = None,
    streamlit_secret_ref: str = "",
    drive_folder_id: str = "",
) -> dict[str, str]:
    encoded = base64.b64encode(secrets_toml.encode("utf-8")).decode("ascii")
    config = {
        "APP_BASE_URL": _normalize_base_url(base_url),
        "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
        "STREAMLIT_SERVER_HEADLESS": "true",
        "SECRET_MANAGER_ENABLED": "true",
        "ALLOW_PLAINTEXT_SECRET_FALLBACK": "true",
        "PARTICIPANT_DISCLOSURE_ENFORCED": "true",
        "PARTICIPANT_DISCLOSURE_VERSION": "385-23-GOOGLE-HEALTH-v1.0",
        "RESEARCH_RETENTION_TEXT_EN": (
            "Measurement data will be retained during the one month collection period and for one additional month for processing and quality checks. No later than one month after measurement ends, Google Health data and the link to the participant's identity will be permanently deleted. The signed consent form and administrative records that do not contain the measurements will be retained according to University and research requirements."
        ),
        "RESEARCH_RETENTION_TEXT_HE": (
            "נתוני המדידה יישמרו במהלך חודש האיסוף ולמשך חודש נוסף לצורך עיבוד ובדיקת איכות. לא יאוחר מחודש לאחר סיום המדידה, נתוני Google Health והקישור לזהות המשתתף/ת יימחקו לצמיתות. טופס ההסכמה ורישומים מנהליים שאינם כוללים את נתוני המדידה יישמרו בהתאם לכללי האוניברסיטה והמחקר."
        ),
        "RESEARCH_DELETION_TEXT_EN": (
            "If a participant withdraws, collection will stop immediately and the application's authorization in the demo account assigned to them will be canceled. All collected data will be permanently deleted unless the participant gives separate and explicit consent at that time to keep them. Summary results already published cannot be removed from an existing publication."
        ),
        "RESEARCH_DELETION_TEXT_HE": (
            "במקרה של פרישה, האיסוף ייפסק מיד והרשאת האפליקציה בחשבון הדמו שהוקצה לך תבוטל. כל הנתונים שנאספו יימחקו לצמיתות, אלא אם תיתן/י באותו מועד הסכמה נפרדת ומפורשת לשמור אותם. לא ניתן להסיר תוצאות מסכמות שכבר פורסמו."
        ),
        "SCHEDULER_TIMEZONE": "Asia/Jerusalem",
        "CLOCK_RUN_JOBS": "false",
        "CLOCK_CATCH_UP_ON_START": "true",
        "ARCHIVE_SHADOW_MODE": "true",
        # Enable Google Health archive collection only after the reviewer flow and
        # operational retention procedure have been smoke-tested in production.
        "ARCHIVE_ENABLED_PROVIDERS": "fitbit",
        "GOOGLE_DRIVE_ARCHIVE_SUBFOLDER": "AdmonTracker Raw Archive",
    }
    if streamlit_secret_ref:
        config[SECRET_REF_VAR_NAME] = streamlit_secret_ref
    else:
        config[CONFIG_VAR_NAME] = encoded
    if service_account:
        service_json = json.dumps(service_account, separators=(",", ":"), sort_keys=True)
        config["GOOGLE_SERVICE_ACCOUNT_JSON_B64"] = base64.b64encode(
            service_json.encode("utf-8")
        ).decode("ascii")
        config["GOOGLE_CLOUD_PROJECT"] = str(service_account.get("project_id") or "")
    if drive_folder_id:
        config["GOOGLE_DRIVE_ARCHIVE_ROOT_ID"] = drive_folder_id.strip()
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secrets", type=Path, required=True)
    parser.add_argument("--oauth-client", type=Path, required=True)
    parser.add_argument("--service-account", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--streamlit-secret-ref", default="")
    parser.add_argument("--drive-folder-id", default="15zlNAbm-0SvmjS-ez0Tw5eGJZ0vU69gC")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".heroku"),
        help="Ignored directory for generated local secret files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = args.secrets.read_text(encoding="utf-8")
    oauth_document = _load_json(args.oauth_client)
    service_account = _load_json(args.service_account)
    updated, callbacks = build_updated_secrets(
        source,
        oauth_document,
        service_account,
        args.base_url,
    )
    config_vars = build_config_vars(
        updated,
        args.base_url,
        service_account,
        streamlit_secret_ref=args.streamlit_secret_ref,
        drive_folder_id=args.drive_folder_id,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    secrets_output = args.output_dir / "streamlit-secrets.toml"
    config_output = args.output_dir / "config-vars.json"
    callbacks_output = args.output_dir / "callback-urls.json"
    runtime_secret_output = args.output_dir / "runtime-secrets-payload.json"
    secrets_output.write_text(updated, encoding="utf-8")
    config_output.write_text(
        json.dumps(config_vars, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    callbacks_output.write_text(
        json.dumps(callbacks, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    runtime_secret_output.write_text(
        json.dumps(
            {"toml_b64": base64.b64encode(updated.encode("utf-8")).decode("ascii")},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    secrets_output.chmod(0o600)
    config_output.chmod(0o600)
    callbacks_output.chmod(0o600)
    runtime_secret_output.chmod(0o600)

    encoded_size = len(
        base64.b64encode(updated.encode("utf-8"))
    )
    total_size = sum(len(key) + len(value) for key, value in config_vars.items())
    print(f"Generated {config_output} with {len(config_vars)} config vars.")
    print(f"Secret bundle size: {encoded_size} bytes; total config payload: {total_size} bytes.")
    print(f"Generated updated local Streamlit secrets at {secrets_output}.")
    print(f"Generated non-secret callback checklist at {callbacks_output}.")
    print(f"Generated Secret Manager payload at {runtime_secret_output}.")
    print("No secret values were printed. Keep the .heroku directory private.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
