#!/usr/bin/env python3
"""Render Streamlit's secrets file from a Heroku config var at dyno startup."""

from __future__ import annotations

import base64
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import toml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


CONFIG_VAR_NAME = "STREAMLIT_SECRETS_TOML_B64"
SECRET_REF_VAR_NAME = "STREAMLIT_SECRETS_SECRET_REF"
DEFAULT_TARGET = Path(".streamlit/secrets.toml")


def decode_secrets(encoded: str) -> str:
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
        text = raw.decode("utf-8")
        parsed = toml.loads(text)
    except Exception as exc:
        raise ValueError(f"{CONFIG_VAR_NAME} is not valid base64-encoded TOML") from exc

    auth = parsed.get("auth")
    google = auth.get("google") if isinstance(auth, dict) else None
    if not isinstance(auth, dict) or not all(auth.get(k) for k in ("redirect_uri", "cookie_secret")):
        raise ValueError("Generated secrets are missing required [auth] values")
    if not isinstance(google, dict) or not all(
        google.get(k) for k in ("client_id", "client_secret", "server_metadata_url")
    ):
        raise ValueError("Generated secrets are missing required [auth.google] values")
    service = parsed.get("gcp_service_account")
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64"):
        if not isinstance(service, dict) or not all(
            service.get(k) for k in ("project_id", "private_key", "client_email")
        ):
            raise ValueError(
                "Generated secrets are missing service-account values and "
                "GOOGLE_SERVICE_ACCOUNT_JSON_B64 is not configured"
            )

    base_url = os.getenv("APP_BASE_URL", "").rstrip("/")
    if base_url:
        redirect = urlsplit(str(auth["redirect_uri"]))
        expected = urlsplit(base_url)
        if (redirect.scheme, redirect.netloc) != (expected.scheme, expected.netloc):
            redirect_host = redirect.hostname or "<invalid>"
            expected_host = expected.hostname or "<invalid>"
            raise ValueError(
                "[auth].redirect_uri host "
                f"({redirect_host}) does not match APP_BASE_URL host "
                f"({expected_host}); replace both Heroku config vars from the same "
                "generated .heroku/config-vars.json file"
            )
    return text


def render_secrets(encoded: str, target: Path = DEFAULT_TARGET) -> None:
    text = decode_secrets(encoded)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=".secrets-",
            suffix=".toml",
            delete=False,
        ) as handle:
            handle.write(text)
            temp_name = handle.name
        os.chmod(temp_name, 0o600)
        os.replace(temp_name, target)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)


def main() -> int:
    encoded = os.getenv(CONFIG_VAR_NAME, "")
    secret_ref = os.getenv(SECRET_REF_VAR_NAME, "").strip()
    if secret_ref:
        from utils.secret_store import GoogleSecretStore

        payload = GoogleSecretStore.from_environment().get_json(secret_ref)
        encoded = str(payload.get("toml_b64") or "")
        if not encoded:
            raise SystemExit(f"Secret {secret_ref} does not contain toml_b64")
    if not encoded:
        if DEFAULT_TARGET.exists():
            print("Using the existing local .streamlit/secrets.toml file.")
            return 0
        raise SystemExit(
            f"Missing required config var: {CONFIG_VAR_NAME} or {SECRET_REF_VAR_NAME}"
        )
    render_secrets(encoded)
    source = "Google Secret Manager" if secret_ref else "the legacy Heroku config var"
    print(f"Rendered .streamlit/secrets.toml from {source}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
