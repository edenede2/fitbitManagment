# AdmonTracker data flow and security description

## Identities and entry points

- Research staff authenticate with a basic OIDC client (`openid profile email`) and
  receive role/project authorization from the production access data.
- Adult participants receive an opaque, expiring, single-use study link. With the
  approved disclosure gate enabled, provider OAuth is unavailable until staff
  consent/age attestation and participant acknowledgement are persisted.
- Participant records use project and watch/study codes; consent audit records do
  not contain participant names, email addresses, or IP addresses.

## Data path

```text
Participant -> public disclosure -> Google Health/Fitbit OAuth
            -> provider callback -> token JSON in Secret Manager
                                  -> token reference/status in Google Sheets

Clock worker -> Secret Manager token -> provider read-only API
             -> temporary JSON/CSV ZIP -> restricted Google Shared Drive
             -> checksum/file reference -> archive_manifest in Google Sheets
```

Heroku temporary files are removed with the temporary directory and are not the
system of record. New archive collection starts at `ARCHIVE_CUTOVER_AT`; the old
lab-mounted archive remains read-only and is not migrated.

## Controls

- HTTPS-only production URLs and exact OAuth redirects.
- Separate staff and participant OAuth clients and least-privilege read-only scopes.
- 48-hour OAuth state expiry, append-only replay record, single-use callbacks, and
  an enforced disclosure version/hash.
- Secrets and participant tokens outside Sheets; Sheets contain references and
  operational metadata only after migration.
- Shared Drive API calls set `supportsAllDrives`; folder and filenames are sanitized;
  deterministic ZIP hashes make retries idempotent.
- One clock process, non-overlapping APScheduler jobs, restart coalescing, bounded
  misfire handling, heartbeats, and job/manifest audit rows.
- Provider error bodies and credential-bearing values are not displayed or logged.
- Public management tokens are random; only their SHA-256 hashes are stored.

## Approved retention and withdrawal

The final bilingual Google Health addendum requires one month of collection and up
to one additional month for processing and quality checks. Google Health data and
the identity link are then permanently deleted. On withdrawal, collection and
authorization stop immediately and all collected data is deleted unless the
participant gives separate, explicit retention consent at that time. The operational
procedure is recorded in `retention_deletion_procedure.md`.

The institutional accreditation package still needs the actual registry identifier
or an official English confirmation if Google requests that evidence during review.
