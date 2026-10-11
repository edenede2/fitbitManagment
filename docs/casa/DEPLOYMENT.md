# Deployment gates, test environment proposal and rollback

## Local batch handoff — no production commands yet

The `casa-readiness` working tree has not been deployed or pushed. Start review
with A1/A2/A3 in [README](README.md), then B/C/D and their dependencies.
Keep private files under ignored `.heroku/` or `.casa-private/`. Use explicit file
staging; do not `git add .` over unknown research documents or credential files.

The current changes are **not a drop-in production deployment**: without approved
bindings/MFA and reviewed enrollments, staff/participant connection creation will
be blocked. Do not follow old deploy instructions as if this branch were ready.

## Concrete isolated-cloud proposal — approval required

Proposed duration: 48 hours of authenticated synthetic testing. No resources have
been created. Candidate Google project: `admontracker-casa-test-2026` (availability
not checked). One default native Firestore database in `me-west1`; a separate test
service account with only test-project database/secret permissions; separate test
staff/participant OAuth clients; ten synthetic enrollments and at most 24 enabled/
disabled test secret versions. Never grant the test account production-project
access or copy real tokens, bootstraps, measurements or staff bindings.

Proposed Heroku app: `admontracker-casa-test-2026` (availability not checked), two
Basic dynos, one web and one clock; default `CLOCK_RUN_JOBS=false` and archive
shadow mode until the synthetic data/source allowlist has been reviewed. Use its
own exact HTTPS redirects, config bundle and bootstrap. A separate synthetic-only
Workspace test Shared Drive/folder must be approved; do not use a production folder
or inherit production data access as a shortcut.

| Item | Incremental estimate / bound |
| --- | --- |
| Local tests/scanners | No cloud compute charge; downloaded tools only |
| Heroku two Basic dynos | $14/month if left running; roughly $0.93 for 48h, before taxes/other resources |
| Test Secret Manager versions | Up to about $1.44/month for 24 automatic-replication active versions; roughly $0.10 over 48h if no free allowance remains |
| Secret access | $0.03 per 10,000 accesses beyond applicable allowance |
| Firestore | Keep synthetic tests within free usage where applicable; confirm billing/database eligibility and regional pricing first |
| Workspace test storage | Existing institutional allocation only if available/approved; a new license/storage purchase is not authorized |

Heroku prorates runtime; Secret Manager counts enabled and disabled versions, and
its free allowance is aggregated across a billing account. Estimates are not a
guaranteed bill. Confirm account discounts, region and current charges at approval.
[Heroku pricing](https://www.heroku.com/pricing/),
[Heroku billing](https://devcenter.heroku.com/articles/usage-and-billing),
[Secret Manager pricing](https://cloud.google.com/secret-manager/pricing),
[Firestore pricing](https://cloud.google.com/firestore/pricing).

Proposed approval ceiling: **$20**, with alerts and a named operator shutting test
dynos down after 48h. Budget alerts are not a hard spending cap. Approval must
identify billing account, resource names, IAM/access owners, test Drive location,
actual permitted OAuth accounts/scopes, duration and who handles cleanup. Paid
assessment/CASA scheduling is a separate proposal; no assessor price is assumed.

## Synthetic acceptance before production approval

1. Reproduce the exact Python 3.11/SDK/runtime graph; collect the full installed
   dependency inventory and a hashed release artifact. Audit transitives/OS too.
2. Exercise SDK transactions/retries on an emulator, including races, dependency
   reads, ambiguous commits and external-call-count assertions. Unit store locks
   alone do not establish Firestore behavior.
3. Demonstrate two differently scoped Managers, unknown/same-prefix accounts,
   denied Student, approved Admin, live role revocation, cache isolation, MFA
   enrollment/recovery, cookie logout and simultaneous tabs in authenticated browsers.
4. Exercise both provider consent/cancel/partial grant/refresh/reconnect, preserved
   deadlines, state races/crashes and safe replacement codes with synthetic subjects.
5. Block collection during provider/Secret Manager outages and mid-refresh/request/
   archive upload. Verify legacy and modern collectors consult the same stop state;
   no stale Sheets recovery may unblock it.
6. Destroy only separately approved **synthetic** enabled/disabled versions, verify
   state readback and delayed-destruction handling; reconcile unknown orphan versions.
7. Verify manual deadlines, delivery failures, escalation, per-enrollment deletion
   ownership/checklist/Admin signoff and copied data across declared stores.
8. Inspect effective IAM, Shared Drive inheritance, GitHub/Heroku members and
   TLS/cookies/CSRF/CORS/WebSocket origins; disposition all scanner findings.
9. Update reviewer evidence to match the deployed release and TAC's actual checklist.

## Production cutover proposal — not authorized here

Obtain a separate operator approval naming release SHA/config changes, short
maintenance window, staff impact, reviewed migrations, irreversible actions and
rollback build. Required prerequisites:

- Privately approved staff bindings and working first Admin/MFA/recovery.
- Keep `GOOGLE_MFA_CLAIMS_ENABLED=false` by default. Enabling claims requires the
  completed time-boxed feasibility test, cookie-signing-key review and approval;
  missing/stale claims still require app MFA, never a bypass.
- Reviewed activation dates/consent references/enrollment token ownership and
  plan for old management links/replacement-code delivery.
- Canonical Firestore `_casa_security` authority, with no Sheets fallback.
  Set `ENROLLMENT_BINDING_REQUIRED=true` **after** reviewed enrollment migration;
  missing enrollment must then fail closed. Keep `DATA_BACKEND=firestore`, Secret
  Manager enabled, plaintext fallback disabled, disclosure enforced.
- Verify the scoped Secret Manager role includes `secretmanager.versions.get`
  for destruction-state readback, in addition to the existing list/destroy rights.
  Missing IAM must leave cleanup pending, not bypass readback or claim success.
- Legacy `main` gate backport/SDK/IAM tested against the same canonical project;
  approving cloud IAM/backport deployment is separate from local implementation.
- Scheduled manual-task reminders/checklist and complete copy inventory.
- Exact installed dependency evidence, complete scans and finding dispositions.
- Authenticated acceptance and a rollback build retaining security checks.

Rehearse additive migration dry runs before any writes. Preserve old configuration
keys for compatibility without letting prefix entries grant access. New enrollment
token secrets/manifest identities must not overwrite historical subjects. Existing
parent/`latest` refs need reviewed, non-destructive pinning/ownership migration.
Never migrate the lab archive wholesale or retroactively relabel historical consent.

## Rollback invariants, per batch

| Batch | Safe rollback behavior | Prohibited rollback |
| --- | --- | --- |
| Staff identity/MFA/session | Disable affected staff feature; keep subject bindings and session epochs/revocations | Restore prefix authorization, cookie-only access, or revoked identities |
| Secret versions | Keep metadata/old compatible readers if approved; verify ownership before operations | Pretend destroyed versions can be restored; destroy unrelated versions |
| Collection/enrollment | Pause affected workers/UI; retain canonical blocks/generations/current ownership | Return collectors to stale Sheet authority or reset a stop/consent date |
| OAuth/codes | Issue fresh links/codes through staff; keep consumed hashes/epochs | Reset used flags or revive year-long bearer links |
| Archives/manual tasks | Pause uploads; preserve old-enrollment manifest/tasks/evidence | Watch-wide deletion, data overwrite or silent deadline extension |

Do **not** blindly `heroku rollback v76`: that build predates these security
controls and could bypass durable blocks. Prepare a compatibility rollback build
that continues to consult the canonical authority, or pause affected services
while fixing forward. Keep all current blocks, revoked identity/session epochs and
consumed states/codes. Real key destruction/data deletion have no Git rollback;
require separate explicit approval and documented verification.

After approved deployment, tie evidence to release number, SHA, dependency graph,
config/identity migration references and authenticated test timestamp. Until then
this package describes local intent and evidence, not production certification.
