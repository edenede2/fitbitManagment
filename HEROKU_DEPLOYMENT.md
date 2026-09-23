# Production deployment for app.admontracker.online

The web and clock processes share Google Sheets, Secret Manager, and a restricted
Google Shared Drive. Heroku's filesystem is used only for temporary files.

## 1. Prepare private deployment values

Run from the repository root:

```bash
python3 scripts/prepare_heroku_config.py \
  --secrets stSecretsExample.txt \
  --oauth-client client_secret_582419685938-te72d7f7ja835qdu5aqluosmqq9ld4fk.apps.googleusercontent.com.json \
  --service-account admontracker-589cfa4bc941.json \
  --base-url https://app.admontracker.online \
  --streamlit-secret-ref projects/admontracker/secrets/admontracker-streamlit-production \
  --drive-folder-id 15zlNAbm-0SvmjS-ez0Tw5eGJZ0vU69gC
```

This creates ignored files under `.heroku/`. Nothing secret is printed. The
service-account key is represented once as `GOOGLE_SERVICE_ACCOUNT_JSON_B64`;
it is not duplicated in the Streamlit TOML bundle.

## 2. Secret Manager IAM and runtime bundle

The API being enabled is not sufficient. The production service account needs:

- `secretmanager.secrets.create` on project `admontracker` for a new participant secret;
- `secretmanager.versions.add`, `secretmanager.versions.access`,
  `secretmanager.versions.list`, `secretmanager.versions.destroy`, and
  `secretmanager.secrets.get` for the secrets
  created by the app.

Prefer a custom least-privilege role. If an administrator creates every secret in
advance, omit `secrets.create` and grant version-adder/accessor access only on those
secrets. Verify create/add/read access before running the token migration.

After IAM is ready:

```bash
export GOOGLE_SERVICE_ACCOUNT_JSON_B64="$(base64 -w0 admontracker-589cfa4bc941.json)"
export GOOGLE_CLOUD_PROJECT=admontracker
python3 scripts/upload_runtime_secrets.py \
  --payload .heroku/runtime-secrets-payload.json \
  --secret-id admontracker-streamlit-production
```

Copy all values from `.heroku/config-vars.json` into Heroku Settings → Config
Vars. Do not configure `PORT`; Heroku supplies it. Do not print or paste the JSON
key, runtime payload, or generated config file into tickets or logs.

## 3. Migrate OAuth secrets and tokens

Keep `ALLOW_PLAINTEXT_SECRET_FALLBACK=true` during the first verified release.

```bash
python3 scripts/migrate_oauth_secrets.py
python3 scripts/migrate_oauth_secrets.py --apply
python3 scripts/migrate_oauth_secrets.py --apply --clear-plaintext
```

For append-only token tables, the migration creates one secret for the latest
active participant/provider group instead of one version for every historical row.
The apply phase writes each secret, reads it back, and only then writes its
reference to the latest active row. The final phase blanks plaintext from all
identified history rows. Unidentified rows are reported and left unchanged for
manual review. After live OAuth and refresh tests pass, set
`ALLOW_PLAINTEXT_SECRET_FALLBACK=false`.

Do not apply the migration while an exposed or superseded service-account key can
still read Secret Manager. Replace the Heroku bootstrap key, verify the new key,
and delete the old key first.

The current branch no longer contains Fitbit bearer-token values in its tracked
working tree, but historical commits contain old notebook output and a legacy code
comment. Revoke or rotate any affected Fitbit credentials before production.
Rewriting shared Git history is a separate, disruptive operator decision and was
not performed by this implementation.

## 3a. Firestore operational-data migration

Firestore is deployed behind flags and Sheets remains authoritative initially:

```text
DATA_BACKEND=sheets
FIRESTORE_PROJECT_ID=admontracker
FIRESTORE_DATABASE_ID=(default)
FIRESTORE_SHADOW_WRITE=false
SHEETS_READ_FALLBACK=true
SHEETS_SHADOW_WRITE=false
```

The importer never writes to Sheets and unconditionally omits plaintext OAuth
tokens, legacy tokens, client secrets, credential JSON, and cookie secrets.
Run the core dry run first:

```bash
heroku run --app admontracker \
  'python3 scripts/render_streamlit_secrets.py && python3 scripts/migrate_sheets_to_firestore.py'
```

Review counts and omitted-secret totals, then import and verify deterministic
documents:

```bash
heroku run --app admontracker \
  'python3 scripts/render_streamlit_secrets.py && python3 scripts/migrate_sheets_to_firestore.py --apply'
```

An independent verification pass is safe to repeat:

```bash
heroku run --app admontracker \
  'python3 scripts/render_streamlit_secrets.py && python3 scripts/verify_firestore_migration.py'
```

The core profile compacts append-only OAuth-token and device-status history to
the latest operational record. The existing Sheet remains the read-only history
during rollout. Do not run `--profile all` until the write count and retention
decision for historical API/webhook logs have been reviewed.

The migration is intentionally not a copy of every worksheet. Staff roles and
project access remain authoritative in `st.secrets`; the redundant `project`
worksheet and unused `student_fitbit` worksheet are excluded. Device records keep
their own `project` value, so a device remains valid even when no matching row
exists in the project worksheet. The `user` collection contains only the
participant-assignment fields used by device management and alert delivery
(`name`, `email`, and `project`), not staff authorization fields. Qualtrics,
AppSheet, Bulldog, and other unrelated tabs are outside the Firestore cutover.

After deploying the live repository routing, prove that the application can read
Firestore directly without silently falling back to Sheets:

```bash
heroku run --app admontracker \
  'python3 scripts/render_streamlit_secrets.py && python3 scripts/smoke_firestore_backend.py --watch YN4'
```

Keep Sheets authoritative while observing mirrored writes:

```text
DATA_BACKEND=sheets
FIRESTORE_SHADOW_WRITE=true
SHEETS_READ_FALLBACK=true
SHEETS_SHADOW_WRITE=false
```

Re-run the migration verification after the shadow period and smoke-test staff
login, the dashboard, device refresh, both OAuth callbacks, token refresh,
disconnect, and deletion requests. Only then make Firestore authoritative:

```text
DATA_BACKEND=firestore
FIRESTORE_SHADOW_WRITE=false
SHEETS_READ_FALLBACK=true
SHEETS_SHADOW_WRITE=true
```

The Sheets fallback and reverse shadow write provide the rollback window. Disable
them only after production observation confirms all operational paths. Legacy tabs
without a Firestore schema continue using Sheets and are never treated as
successful Firestore no-op writes.

## 4. Participant disclosure gate

Leave `PARTICIPANT_DISCLOSURE_ENFORCED=false` until all of these exist:

1. The PI/ethics-approved bilingual addendum is deployed at
   `assets/compliance/google-health-addendum-385-23-en-approved-v1.0.pdf` and
   `assets/compliance/google-health-addendum-385-23-he-approved-v1.0.pdf`.
2. `PARTICIPANT_DISCLOSURE_VERSION` is the approved version, not `DRAFT-*`.
3. `RESEARCH_RETENTION_TEXT_EN`, `RESEARCH_RETENTION_TEXT_HE`,
   `RESEARCH_DELETION_TEXT_EN`, and `RESEARCH_DELETION_TEXT_HE` contain the exact
   approved wording.

The application refuses to enforce an incomplete disclosure. Google verification
must not be submitted while enforcement is off.

## 5. Shared Drive archive and clock rollout

The target folder is not public. It currently inherits 16 user permissions; a
Drive administrator must confirm that all 16 belong to the authorized study team.
The service-account check created/reused this child folder:

```text
AdmonTracker Raw Archive
ID: 1rbntieY5-xwTJD2OLGt7_2mVYd_s7Q6_
```

Set a documented new-data-only cutoff and start in shadow mode:

```text
ARCHIVE_CUTOVER_AT=<approved RFC3339 timestamp in Asia/Jerusalem>
ARCHIVE_RUN_JOBS=false
ARCHIVE_SHADOW_MODE=true
ARCHIVE_ENABLED_PROVIDERS=fitbit
CLOCK_RUN_JOBS=false
GOOGLE_DRIVE_ARCHIVE_SUBFOLDER=AdmonTracker Raw Archive
```

Keep `ARCHIVE_ENABLED_PROVIDERS=fitbit` while the Google Health addendum is
unapproved. After the approved addendum is deployed, disclosure enforcement is
enabled, and a live authorization/refresh test passes, change it to
`fitbit,google_health`.

Scale exactly one clock dyno. Set `CLOCK_RUN_JOBS=true` while leaving archive
collection independently paused with `ARCHIVE_RUN_JOBS=false`. After its smoke
test passes, set `ARCHIVE_RUN_JOBS=true` while leaving shadow mode on; compare the
manifest and monitoring outputs with the old lab cron. Then set
`ARCHIVE_SHADOW_MODE=false` and disable the old cron. Do not migrate the historical
mounted-drive archive.

The clock runs monitoring at minute `00`, archive collection at minute `30`, and
writes a heartbeat every five minutes in `Asia/Jerusalem`. Check `job_runs`,
`clock_status`, and `archive_manifest` after every release.

New archive writes use direct JSON files (historical ZIP files are preserved):

```text
AdmonTracker Raw Archive/
  <project>/
    <watchName>/
      FITBIT/                  # or GOOGLE_HEALTH
        Physical Activity/    # heart rate, steps, respiratory rate, calories
        Sleep/                # sleep, HRV, skin temperature
        Stress/               # intentionally empty
```

Heart-rate files are daily. Steps, sleep, respiratory-rate, and skin-temperature
files are monthly; HRV and calories retain their daily cadence. Google Health
contains only the categories approved for that provider. Files are upserted by a
deterministic period filename, so a retry updates the same Drive file.

## 6. Provider console values

- Google staff redirect: `https://app.admontracker.online/oauth2callback`
- Google Health redirect: `https://app.admontracker.online/?google_health_callback=1`
- Fitbit redirect: `https://app.admontracker.online/?fitbit_callback=1`
- Authorized domain: `admontracker.online`
- Public homepage: `https://admontracker.online/`
- Privacy: `https://admontracker.online/privacy.html`
- Terms: `https://admontracker.online/terms.html`
- Staff application: `https://app.admontracker.online/`

Use separate production web clients for staff OIDC and participant health scopes.
See `docs/compliance/google_publication_checklist.md` before publishing the external
OAuth audience.
