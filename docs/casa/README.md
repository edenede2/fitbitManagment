# CASA remediation — local review package

## Status and authority

8 October 2026. Branch: `casa-readiness`, based on `heroku` commit
`f96a9836320bc120ba4ba49887a6da33d050c3f0` (reported deployment: Heroku v76).
The changes here are **local and undeployed**. Existing untracked project-memory
and CASA-prompt documents were preserved. `main` has not been changed.

The three P0 corrections and supporting synthetic regressions are **internal-test
ready**. The full release remains in remediation. It is **not assessor-test ready**
and **not externally validated**. Unit tests are not a security certification.
Use CASA 2.1.1 only as a provisional mapping; TAC must confirm the assigned checklist.
[CASA specification](https://github.com/appdefensealliance/ASA-WG/blob/main/CASA/CASA%20Specification.md).

No production deployment, cloud resource creation, real credential destruction,
real-data deletion, history rewrite, publication change, TAC contact or evidence
upload was performed. Scanner installation used temporary local environments;
public advisory queries transmitted package names/versions only.

## Review batches

Review these as dependency-ordered units, not independently deployable patches.
They share the authoritative security-store adapter. The working tree is deliberately
not merged or pushed before workflow/migration review.

| Batch | Review focus | Main files | Local validation | Deployment/rollback |
| --- | --- | --- | --- | --- |
| A1 | P0 staff-prefix collision | `utils/access_control.py`, `controllers/auth_controller.py`, `scripts/casa_staff_migration.py` | `IdentityTests`, stale-capability test, existing access tests | Approved issuer/subject bindings and first-Admin access first; never restore prefix authorization |
| A2 | P0 numeric secret-version states | `utils/secret_store.py` | `CleanupTests`, pinned-version test | Synthetic enabled/disabled version destruction first; real destruction separately approved and irreversible |
| A3 | P0 stop-before-secret-access; stale refresh publication | `utils/collection_control.py`, `utils/security_store.py`, token stores, provider guards, `utils/connection_management.py` | `DisconnectTests`, atomic fence tests, old-enrollment token test | Canonical gate and legacy backport required; preserve durable stop generations on rollback |
| B | Enrollment-bound access and transactional OAuth | `utils/participant_sessions.py`, `utils/enrollment_security.py`, health-state helpers, callbacks, management page, archive collector | Concurrent callback/code tests, expiry/reassignment/scope tests | Review original dates/token ownership; old bearer links invalid at cutover; consumed states/codes never reopen |
| C | Revocable staff sessions and fallback MFA | `utils/staff_sessions.py`, auth UI, MFA bootstrap script | MFA claims, TOTP/recovery/enrollment/rate-limit/timeout tests | Time-box actual Google claims test; secure Admin bootstrap; no cookie-only or MFA-bypass rollback |
| D | Scoped workflow and evidence | project/device controls, Research Operations page, alert email, `docs/casa/`, inventory/scan scripts | Existing alert/public/readiness tests, redacted scans | Staff/Student changes require approval; manual task scheduler/checklist and browser/cloud evidence still pending |

## What has changed locally

- Authorization requires a single, enabled, explicitly approved Google
  issuer/subject binding, verified matching full email, valid claims, and a valid
  server-side MFA-backed session. Prefix entries never grant access. Students are
  denied production access. A Manager cannot use another project's capability.
- Secret cleanup compares numeric enum values, destroys enabled and disabled
  versions, and verifies their resulting state. Delayed or failed destruction is
  pending, not success.
- Disconnect commits a collection block/generation before any credential lookup
  or provider request. Revocation, credential cleanup and research-data deletion
  have separate status fields. Failures cannot undo the block.
- Provider requests check the authoritative gate before and after calls; token
  publication and monitoring-status writes are generation-fenced. New enrollment
  token references are pinned to concrete versions rather than `latest`. An
  uncertain publication queues Admin review instead of destroying a whole secret.
- A one-use, enrollment-bound 24-hour code replaces management bearer URLs. It
  exchanges for a browser-bound server-side session; replacement and reassignment
  invalidate old access. Codes/recovery values are stored as hashes, not plaintext.
- Firestore acknowledgement/state claim is transactional. Provider and Secret
  Manager calls are outside transaction callbacks. A claimed OAuth state is never
  reopened after failure/crash; the team issues a fresh link.
- Actual scope grants are checked. Arbitrary Google token endpoints and credential
  redirects are rejected. Production collection cannot fall back to raw legacy
  credentials or another provider after a failure.
- New archive filenames/manifest keys include enrollment identity, preserving the
  project/watch/FITBIT/category layout. An upload already in flight at withdrawal
  is recorded for enrollment-specific review, not reported as deleted.
- Future alert emails no longer include heart-rate measurements. Device saves
  update only authorized existing rows, preventing a filtered Manager table from
  replacing the global device registry.
- Authlib is narrowly pinned from 1.6.11 to 1.6.12. See the advisory disposition in
  [findings](FINDINGS.md); this is not an indiscriminate dependency upgrade.

## Consequential workflow decisions — before deployment

Approve the full staff roster and subject bindings, the Student production-access
removal, Manager project restrictions, and how existing staff enroll/recover MFA.
No staff subjects can be inferred from username prefixes or guessed emails.

Session/code values are application design choices, **not confirmed CASA
requirements**: staff idle 30 minutes/absolute 8 hours; participant idle 10 minutes/
absolute 30 minutes; access codes 24 hours; enrollment approvals 15 minutes;
five failed attempts per 15 minutes. The staff identity-token expiry can shorten
the effective session. Confirm usability during authenticated browser tests.
Google-claim MFA acceptance defaults **off** until the time-boxed feasibility and
cookie-key integrity review are approved; app-based MFA is the default path.

The redacted local runtime-bundle inventory contains 3 Admin, 21 Manager and 1
Student scalar entries. These are entry counts, not verified people/current
production grants, and zero identity bindings were inferred. The approved roster
must be checked privately before migration.

Original activation dates, consent coverage and token ownership need explicit
review before enrollment migration. Do not initialize historical subjects using
today's date. Reauthorization does not create a new enrollment or extend dates.
Legacy Fitbit consent coverage remains a PI decision, not an inferred approval.

Routine stopping/deletion remain manual. Managers own tasks, Admin verifies and
backs up, and PI approves exceptions. Current code shows deadline warnings; it
does **not yet deliver scheduled deadline emails or an audited completion checklist**.
These are release gates, not claimed completed controls.

## Next work and approval boundary

1. Finish the local service-boundary inventory, lifecycle task journal/checklist,
   scheduled reminder delivery/failure handling, and reviewed legacy gate backport.
2. Test retries/crashes and races using a Firestore emulator and the production
   Python/SDK versions; then authenticated browser testing. Current unit fakes
   cannot establish deployed SDK/transaction/browser correctness.
3. Obtain approved private staff bindings and reviewed enrollment migration data.
   Perform the time-boxed Google MFA feasibility exercise in [the runbook](RUNBOOK.md).
4. Approve the concrete isolated-cloud proposal in [deployment/rollback](DEPLOYMENT.md)
   before creating resources or running paid dynos. No staging resources exist yet.
5. Close/disposition the [findings](FINDINGS.md), collect exact deployed dependency
   and access-control evidence, then request a separate production cutover approval.
6. Only after deployed evidence matches the release may TAC assessment, a CASA
   Letter of Validation and Google's final approval proceed as separate milestones.

The operator-supplied Google deadline is 28 December 2026. Confirm scheduling
alongside remediation; no TAC contact, purchase or submission is authorized by
this local batch.

Evidence: [validation report](VALIDATION.md), [control matrix](CONTROL_MATRIX.md),
[operations runbook](RUNBOOK.md), [deployment/rollback](DEPLOYMENT.md).
