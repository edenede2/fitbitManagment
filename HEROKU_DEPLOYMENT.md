# Heroku configuration for app.admontracker.online

The app keeps its existing `st.secrets` interface. On Heroku, one base64-encoded
config var is decoded into `.streamlit/secrets.toml` before Streamlit starts.
Credential values are never committed to Git.

## Generate the config vars

Run this from the repository root:

```bash
python3 scripts/prepare_heroku_config.py \
  --secrets stSecretsExample.txt \
  --oauth-client client_secret_582419685938-te72d7f7ja835qdu5aqluosmqq9ld4fk.apps.googleusercontent.com.json \
  --service-account admontracker-589cfa4bc941.json \
  --base-url https://app.admontracker.online
```

The command creates three ignored, private files:

- `.heroku/config-vars.json`: the four key/value pairs for Heroku.
- `.heroku/streamlit-secrets.toml`: the merged file for local inspection only.
- `.heroku/callback-urls.json`: the non-secret callback checklist.

In the Heroku Dashboard, open **Settings → Config Vars** and add every key/value
from `.heroku/config-vars.json`. Do not add `PORT`; Heroku provides it. Do not
paste the original JSON credential files into Git or the dashboard separately.

The `Procfile` decodes the secret bundle at dyno startup and then launches
Streamlit on Heroku's assigned port.

The app pins Python 3.11 in `.python-version`. This is a supported Heroku
runtime and is deliberately close to the Python 3.10 environment currently
used for this project. Test the complete application before moving to a newer
major Python release.

## Provider settings outside Heroku

These changes cannot be made by editing downloaded JSON files:

1. In Google Cloud, use `https://app.admontracker.online` as an authorized
   JavaScript origin and register both redirect URIs:
   - `https://app.admontracker.online/oauth2callback` for Streamlit staff login.
   - `https://app.admontracker.online/?google_health_callback=1` for Google Health.
2. Update the production row in the app's `health_oauth_clients` Google Sheet to
   use the Google Health callback URI above. The current app reads this setting
   from the sheet, not from Heroku.
3. In the Fitbit developer console, change the legacy Fitbit callback URL to
   `https://app.admontracker.online/?fitbit_callback=1`.
4. Share every required production Google Sheet with the `client_email` from
   `admontracker-589cfa4bc941.json` using the minimum required permission.
5. Verify the `app.admontracker.online` URL-prefix property (or the parent
   `admontracker.online` domain property) in Google Search Console using an
   account that is an owner/editor of the production Google Cloud project.

After changing a Heroku config var, Heroku creates a new release and restarts the
app. Avoid printing `STREAMLIT_SECRETS_TOML_B64` in application logs or support
messages; base64 is encoding, not encryption.
