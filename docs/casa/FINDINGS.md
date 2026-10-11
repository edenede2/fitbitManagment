# Redacted security findings

## Confirmed defects and local corrections

| ID | Priority | Evidence | Local correction | Remaining gate |
| --- | --- | --- | --- | --- |
| C-001 | P0 | Synthetic same-prefix accounts previously received the same role | Exact approved Google issuer/subject binding; prefix fallback removed | Verified roster, Admin approval, MFA bootstrap, browser testing |
| C-002 | P0 | Production numeric enum strings are `1`, `2`, `3`, not enum names | Compare values; destroy enabled/disabled versions; read back state | Synthetic cloud version tests; destruction approvals |
| C-003 | P0 | A credential-store failure previously occurred before a collection block | Authoritative block commits before lookup/revocation; independent outcomes | Legacy gate backport and every collector verified against canonical authority |
| C-004 | P0-related | Refresh or provider response may arrive after withdrawal/reassignment | Captured generation guards and transactional metadata publication | SDK/emulator races and browser/archive in-flight tests |
| C-005 | P1 | A just-written version at `latest` could be used before publication; broad cleanup could destroy a committed credential after an ambiguous error | New enrollment refs are pinned; failures queue Admin reconciliation; no automatic broad cleanup | Crash reconciliation for unknown/orphan versions; existing parent/latest ref migration |
| C-006 | P1 | Manager's filtered registry save replaced the whole table | Per-authorized-existing-row updates; no new/reassigned row via edited table | Full interactive service/export/write authorization inventory and regression tests |
| C-007 | P1 | Unrestricted OAuth token URI can send credentials to an unintended endpoint | Allowlisted Google endpoints; credential POSTs do not follow redirects | Review existing client endpoint values before rollout |
| C-008 | P1 | Long-lived bearer management links and nontransactional replay bookkeeping | One-use hashed enrollment codes; transactional canonical state claims; old bearer links rejected | Cutover communications, ingress rate limits, provider crash recovery evidence |
| C-009 | P1 | Measurements previously appeared in alert messages | Heart-rate values removed from future email snapshots | Historic mailbox copies and all derived/export copies in deletion checklist |

## New scanner findings — do not overstate scope

### C-010: credential-like material in Git history / legacy line

Read-only narrow pattern scan: **573 local commits**, **779 commit/file occurrences**,
with hits in `.env`, `.streamlit/secrets.toml`, `generateKey.ipynb`, `secrets.json`,
and `firbitfilesOrgenizer.py`. Occurrences across snapshots are **not** counts of
distinct credentials. The legacy `main` tip is among the flagged snapshots.
No matched values were output. Current nonignored worktree had no matches under
these patterns; that is **not** proof that all secrets are absent.

Potentially active exposed credentials are urgent. Privately map key IDs/token
ownership and active status, document impact and obtain explicit rotation/revocation
approval. Do not test unknown tokens by collecting real health data. Do not rewrite
history or delete files without approval. Previous approval to keep an old client
does not establish that all historical credentials are safe; assess each separately.
Include the Streamlit cookie-signing secret in that private review. A compromised
signer invalidates trust in native identity/MFA claims. Google-claim MFA is disabled
by default until feasibility/key-integrity approval; this does not replace key review.

### C-011: Authlib dependency advisory

`pip-audit 2.10.1`/OSV flagged Authlib 1.6.11 with `PYSEC-2026-188`,
`CVE-2026-44681`, `GHSA-r95x-qfjj-fjj2`. The output repeats the same advisory twice;
this is **one unique advisory**, not two distinct vulnerabilities. The affected
feature is an Authlib OIDC provider's implicit/hybrid grant endpoint. Repository
inspection found no registration of those grants: AdmonTracker uses Google as its
identity provider. **Inference:** no direct exploit path for this particular
advisory was identified in the app. The targeted 1.6.12 pin still removes the
reported affected component version; verify native staff login on the release
runtime before deployment. [Maintainer advisory](https://github.com/authlib/authlib/security/advisories/GHSA-r95x-qfjj-fjj2),
[patch release](https://github.com/authlib/authlib/releases/tag/v1.6.12).

The scan covered only five observed v76 components, not all transitive/OS packages.
The exact deployed graph and a complete advisory scan remain required.

### C-012: Bandit findings

Initial local Bandit 1.9.4 screen covered controllers, entities, services, utilities,
views, pages, scripts and collection/clock entrypoints: no parse errors, no medium/
high findings, 40 low findings. This is not a clean security certification.
Credential-name heuristics (B105/B106/B107) include empty values, field names,
endpoint constants and synthetic fixtures; no hardcoded live credential was
confirmed by this source scan. Bare exception-pass findings (B110) require
path-specific disposition: OAuth cleanup/audit failures remain manual-review
states, while older optional UI/background paths still require review. Later code
changes must be rescanned; see the final counts in [validation](VALIDATION.md).

## Open engineering findings / release blockers

| ID | Gap | Required next evidence/action |
| --- | --- | --- |
| C-013 | Legacy `main` lacks the Firestore adapter/schema; simply copying one gate helper is insufficient | Reviewed standalone canonical gate/SDK/IAM backport; demonstrate legacy reads, refreshes and cron honor withdrawal |
| C-014 | Enrollment migration is not performed; existing tokens/status/history still use reusable watch names | Reviewed original dates, explicit credential bindings, old-subject ownership map, no historical overwrites |
| C-015 | Deadline function/UI warnings exist, but scheduled email reminders and completion journal are not implemented | Reliable 72h/24h/deadline reminders, retry/failure escalation, Manager completion/Admin verification evidence |
| C-016 | Some interactive code still relies on broad adapters and navigation checks | Enumerate service/export/config boundaries; negative tests before every cached read/external call/write |
| C-017 | Unknown version on a crash during Secret Manager add cannot be reconciled solely from a returned ref | Durable credential-write intent and idempotent reconciliation; never retry an ambiguous external token exchange |
| C-018 | Per-browser participant limiter is not an ingress-wide anti-abuse control | Test distributed attempts/new WebSockets; approved edge/shared global throttling without storing participant IPs |
| C-019 | MFA claim feasibility has not been exercised on actual approved accounts | Time-box live check; missing/stale claims fail; app MFA path and recovery authenticated-browser tests |
| C-020 | Effective IAM, Shared Drive inheritance, access membership and production transport/cookie/origin controls not inspected in this implementation batch | Read-only evidence tied to exact release, approved remediation if necessary |
| C-021 | Enrollment history, old archive ownership and cleanup copies need complete lifecycle indexing | Reassignment-safe history/task sync, separate old-enrollment deletion tasks, all-store checklist |
| C-023 | Reauthorization must not silently associate a different provider account with an existing enrollment | Verify stable provider identity/schema and enforce identity continuity or an explicit reviewed new enrollment; include negative browser/provider tests |

TAC checklist version, testing approach, assessment scheduling, signed validation
and Google final publication remain externally unverified. No finding is marked
resolved in production on the basis of local tests alone.
