# Staff access, withdrawal and manual research operations

This is an internal operations/design runbook for the undeployed CASA branch.
It does not change signed consent or public policy. Review actual PI/ethics wording
before historical migration; do not infer legacy Fitbit coverage.

## Staff migration and first Admin

First obtain a redacted **local** inventory (not proof of current production roles):

```bash
python3 scripts/casa_staff_migration.py \
  --payload .heroku/runtime-secrets-payload.json --inventory-only
```

1. The Admin/operator privately verifies each full Google email and existing
   role/project assignment. Obtain the issuer/subject from the actual **validated**
   institutional or personal Google login. Do not guess, use prefix matching,
   upload an unvalidated ID token, or disclose tokens in chat.
2. Record the approval reference and issuer `https://accounts.google.com`, subject,
   matching verified email, role `Admin`/`Manager`, project and `enabled=true` in
   an ignored private JSON list. Cross-project Admin access requires `project="*"`.
   Prefix assignments remain in the private configuration for compatibility but
   **never grant access** on this branch. Preserve the old client/key configuration.
3. Prepare and review without uploading:

   ```bash
   python3 scripts/casa_staff_migration.py \
     --payload .heroku/runtime-secrets-payload.json \
     --approved-bindings .heroku/approved-staff-bindings.json
   ```

   To produce a new ignored reviewed bundle, add
   `--output .heroku/runtime-secrets-casa-reviewed.json`. The script refuses to
   overwrite an existing output and prints only redacted role counts. It does not
   validate real accounts by itself, write the cloud, rotate keys or change publication.
4. Before cutover, approve Student denial, scoped Manager workflows and private
   replacement/MFA recovery delivery. Review role/project differences manually;
   the dry-run count is not an approval record.
5. App MFA is the default; Google-claim acceptance stays off until the feasibility
   gate is approved (`GOOGLE_MFA_CLAIMS_ENABLED=false`). The first approved Admin requires an operator-issued
   one-use bootstrap approval. `scripts/casa_bootstrap_mfa.py --help` describes
   the dry-run inputs. Its `--apply` writes a cloud security record and must not be
   run against production without approval. A trusted offline operator verifies
   identity/change reference first; subsequent enrollment/recovery approvals come
   from an authenticated Admin in Research Operations.
6. The bound staff account enters its private 15-minute approval, saves the
   authenticator key, proves a code, and downloads the one-use recovery codes.
   Keep them offline; never include them in evidence/video. Enrollment/recovery
   invalidates old sessions/recovery values. Lost MFA and codes require independent
   Admin identity verification; there is no email-prefix or cookie-only bypass.
7. Expired sessions require explicit verification again. Logout invalidates all
   application tabs through the authority epoch. If authority storage is unavailable,
   access fails closed and logout must not claim revocation succeeded.

## Time-boxed Google MFA feasibility — maximum 60 minutes

Use the actual staff client and dedicated institutional and personal **test**
accounts, without any research access. The app's publication/advanced settings
may be prerequisites for additional claims. Google says those claims are not
universally returned and does not support forcing Google-account reauthentication.
[Google security-bundle documentation](https://developers.google.com/identity/siwg/security-bundle).

Allocate 15 minutes to client/settings, 30 minutes to both account types and 15
minutes to the decision/evidence. Record only booleans/method names/claim age and
the test-account pseudonym, never raw JWTs, subjects or secrets in public evidence.

| Scenario | Required result |
| --- | --- |
| Validated `amr` includes `mfa`, `auth_time` fresh and finite | May establish an MFA-backed session within the chosen age window |
| Missing `amr` or `auth_time`; password-only claim | Must require app MFA; enabling Google 2-step alone is not proof |
| Stale or future/nonfinite authentication time | Must require app MFA |
| Old Streamlit cookie after application logout/revocation | Must not silently recreate application access |
| Disabled binding/changed role/project/Student | Must deny production access despite a Google login |

If claims are absent, inconsistent or cannot prove the required freshness at the
time-box limit, select the already implemented app-MFA path. Do not disable MFA or
wait indefinitely on Google. Native browser enrollment/recovery still needs testing.
Do not enable `GOOGLE_MFA_CLAIMS_ENABLED=true` until reliable validated claims and
the native cookie-signing-key integrity have been verified and operator-approved.
App TOTP uses the RFC algorithm, with replay tracking and throttling; this is not
an independent cryptographic/security certification. [RFC 6238](https://www.rfc-editor.org/rfc/rfc6238).

## Enrollment and participant replacement access

The watch name is a reusable physical-device label, **not** participant identity.
Review and initialize an enrollment using verified original activation, actual
provider/project, approved consent reference and policy coverage. Original dates
must not be refreshed by a reconnect or by migration. Stop/review the previous
enrollment before reassignment; unresolved copies and cleanup tasks retain their
old enrollment identity.

Staff privately identify a participant through the research team's approved
channel, select the assigned project/watch/enrollment in Research Operations, and
issue a replacement access code. Deliver it privately. It is valid for 24 hours,
one use; issuing a replacement invalidates previous codes and management sessions.
Do not send a year-long URL bearer token, participant password, or OAuth token.

The participant enters the code at `/Manage_Connection`, chooses language, and
gets a browser-bound session (10-minute idle/30-minute absolute). Reassignment
revokes that session. A stopped enrollment's current session may still request
deletion: stopping collection is not a denial of withdrawal access. If the session
expires, reload and enter a privately delivered replacement code. The PI contact
remains available if self-service is inaccessible.

## Disconnect, revocation and deletion are distinct

```text
Request to stop/delete
    -> commit canonical collection block + generation
    -> attempt provider revocation       [confirmed / pending_manual]
    -> attempt secret cleanup + readback [confirmed / pending]
    -> open manual data-deletion task    [requested, NOT deleted]
```

The block precedes credential-store/provider access. Metadata outages can leave
registry mirrors outdated but must not permit a collector to use them as an
authority. A provider call already in flight may finish remotely: discard its
returned data, reject stale writes and flag any already-in-flight archive upload
for enrollment-specific removal/review. There is no atomic transaction spanning
Firestore, a provider and Drive.

If revocation fails but local secret cleanup succeeds, the application may no
longer have a credential with which to retry remote revocation. Keep that status
unconfirmed; the Manager helps the participant remove access in the provider's
account settings and records evidence. Do not mark provider revocation complete
merely because collection or local token use stopped.

OAuth exchange errors/crashes leave the state consumed. Never retry the same code
exchange or reset `used`. Reconcile `claimed`/uncertain records and any new secret
versions, then issue a fresh disclosed link. Review uncertain publication before
destroying a pinned version: the database write may have committed before an SDK
transport failure. Unknown orphan versions require an Admin's ownership review.

## Deadlines and accountability

The approved Google Health schedule uses one **calendar month**, month-end clamped,
with Asia/Jerusalem display, followed by at most one further calendar month for
processing after measurement ends. Preserve original activation and collection-end;
record actual measurement-end and deletion-due. The PI must confirm applicability
to any legacy Fitbit data before using this schedule for it.

| Task | Owner | Verification/escalation |
| --- | --- | --- |
| Review original dates, consent and assignment | Project Manager | Admin verifies migration; PI resolves policy coverage |
| Manually stop collection by consent deadline or immediately on withdrawal | Project Manager | Admin is backup and receives overdue escalation |
| Retry provider revocation/credential cleanup | Project Manager with Admin for secret/IAM tasks | Track outcomes separately; provider-console evidence when needed |
| Perform enrollment-specific all-copy deletion checklist | Project Manager | Admin verifies each disposition and evidence reference |
| Exceptions or withdrawal retention permission | PI | Separate explicit consent/approval; never implied from original consent |

The local page/function computes 72h/24h/deadline/overdue warnings. Scheduled
delivery, retry/escalation and a durable completion checklist remain to be built.
Until that gate is closed, use an assigned team task register and independently
verify deadlines; reminders alone do not prove commitments were met. There is
**no automatic routine collection-end stopping or automatic research-data deletion**.

## All-copy deletion checklist

Scope by enrollment, never delete all data under a reused watch name. Identify
unknown historical ownership before deleting. Manager records a per-store
disposition and an evidence/task reference without measurements; Admin verifies.

| Copy/store | What to review |
| --- | --- |
| Firestore | Status measurements, logs/derived values, deletion/enrollment state; retain only allowed nonmeasurement evidence |
| Shared Drive | Enrollment manifest files, revisions, trash, duplicate/pending/in-flight uploads and inherited access |
| Sheets | Legacy/recovery/shadow copies, historical rows and identity links |
| Secret Manager | Applicable enabled AND disabled versions; read back destruction; orphan versions/pending cleanup |
| App/runtime | Sensitive session caches, temporary archives/downloads, local tracking artifacts |
| Exports/analysis | Researcher downloads, derived datasets and separate identity-link records |
| Email | Historic heart-rate alert copies in sender/recipient mailboxes; operational messages under approved policy |
| Backups | Restore points, copied archives and retention/access constraints; no silent recreation of deleted data |
| Lab archive | No blanket transfer/delete; read-only historical ownership/policy review |

Data-deletion completion is not implemented as an automated action on this branch.
Do not set a completed status without the checklist and Admin evidence. Signed
consents/allowed administrative evidence follow institution requirements; published
results and any retention exception follow the exact approved wording, not an
invented application policy.
