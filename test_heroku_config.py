import base64
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import toml

from scripts.prepare_heroku_config import build_config_vars, build_updated_secrets
from scripts.render_streamlit_secrets import decode_secrets, render_secrets


SOURCE = '''
spreadsheet_key = "sheet"
FITBIT_REDIRECT_URI = "https://old.example/?fitbit_callback=1"
dotted.user = "Manager, Study"

[gcp_service_account]
type = "service_account"
project_id = "old-project"
private_key = "old-key"
client_email = "old@example.invalid"

[auth]
redirect_uri = "https://old.example/oauth2callback"
cookie_secret = "0123456789abcdef0123456789abcdef"

[auth.google]
client_id = "old-client"
client_secret = "old-secret"
server_metadata_url = "https://accounts.google.com/.well-known/openid-configuration"
'''

OAUTH = {
    "web": {
        "client_id": "new-client",
        "client_secret": "new-secret",
        "redirect_uris": ["https://app.admontracker.online/oauth2callback"],
    }
}

SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "new-project",
    "private_key_id": "key-id",
    "private_key": "dummy-private-key-material",
    "client_email": "service@example.invalid",
    "client_id": "123",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


class PrepareHerokuConfigTests(unittest.TestCase):
    def test_credentials_and_redirects_are_replaced(self):
        updated, callbacks = build_updated_secrets(
            SOURCE, OAUTH, SERVICE_ACCOUNT, "https://app.admontracker.online/"
        )
        parsed = toml.loads(updated)

        self.assertEqual(parsed["APP_BASE_URL"], "https://app.admontracker.online")
        self.assertEqual(
            parsed["FITBIT_REDIRECT_URI"],
            "https://app.admontracker.online/?fitbit_callback=1",
        )
        self.assertEqual(parsed["auth"]["redirect_uri"], callbacks["login_redirect"])
        self.assertEqual(parsed["auth"]["google"]["client_id"], "new-client")
        self.assertEqual(parsed["gcp_service_account"]["project_id"], "new-project")
        self.assertIn("dotted.user", updated)

    def test_config_bundle_round_trips_without_exposing_nested_keys(self):
        updated, _ = build_updated_secrets(
            SOURCE, OAUTH, SERVICE_ACCOUNT, "https://app.admontracker.online"
        )
        config = build_config_vars(updated, "https://app.admontracker.online")
        decoded = base64.b64decode(config["STREAMLIT_SECRETS_TOML_B64"]).decode()

        self.assertEqual(decoded, updated)
        self.assertNotIn("auth.google.client_secret", config)
        self.assertEqual(decode_secrets(config["STREAMLIT_SECRETS_TOML_B64"]), updated)

    def test_runtime_renderer_writes_private_file(self):
        updated, _ = build_updated_secrets(
            SOURCE, OAUTH, SERVICE_ACCOUNT, "https://app.admontracker.online"
        )
        encoded = build_config_vars(updated, "https://app.admontracker.online")[
            "STREAMLIT_SECRETS_TOML_B64"
        ]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / ".streamlit" / "secrets.toml"
            with patch.dict(
                "os.environ", {"APP_BASE_URL": "https://app.admontracker.online"}
            ):
                render_secrets(encoded, target)
            self.assertEqual(target.read_text(), updated)
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

    def test_http_base_url_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            build_updated_secrets(SOURCE, OAUTH, SERVICE_ACCOUNT, "http://example.com")


if __name__ == "__main__":
    unittest.main()
