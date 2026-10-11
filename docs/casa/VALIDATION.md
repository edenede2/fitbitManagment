# Local CASA validation report — 8 October 2026

## Purpose and scope

Validate the three confirmed P0 corrections and supporting local controls with
synthetic data. This report does not establish assessor readiness, production
correctness, a CASA Letter of Validation or Google publication approval.

Branch `casa-readiness`, based on `heroku`
`f96a9836320bc120ba4ba49887a6da33d050c3f0`. No deployment/new release was created.
Source/dependency-declaration inventory: **116 nonignored source/declaration
files (Python files and `requirements.txt`)**, aggregate SHA-256
`10465e7714f6eb1068d21604fe5899c99e1682f4b643863f2461d6053b509074`.
The inventory excludes private ignored files, PDFs, non-Python assets and docs;
it is not a complete signed build artifact or dependency SBOM.

## Methods

Local interpreter: Python 3.10.12, local Streamlit 1.44.0. This is not the reported
production Python 3.11.17/Streamlit 1.56.0 environment. Test data is synthetic;
security storage is injected, lock-backed memory, **never a production fallback**.
Socket connections and inherited production credential/backend variables are
blocked/removed by the unit-test harness. No real provider/Firestore/Drive call or
credential destruction was used in these tests.

Disable ambient pytest plugin auto-loading: the host's xonsh plugin attempts to
write outside the workspace. That host-plugin failure is not an app failure.

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest -q test_casa_security.py \
  -k 'IdentityTests or CleanupTests or DisconnectTests'
git diff --check
python3 scripts/casa_source_inventory.py
python3 scripts/casa_secret_scan.py --history
```

Do not run the five old standalone lab smoke scripts on real files as part of a
unit test. They are skipped explicitly by `conftest.py`; ten test cases are excluded,
not counted as passing. Files: `test_fitbit_organizer.py`,
`test_complete_tracking.py`, `test_datetime_tracking.py`, `test_temp_fix.py`,
`test_status_fix.py`. Live isolated testing requires separate preparation/approval.

## Results

| Check | Result | Interpretation |
| --- | --- | --- |
| Safe local suite | **175 passed, 10 skipped**, 1.53s | Functional/mocked security regression evidence only |
| Focused three-P0 classes | **7 passed**, 26 deselected, 0.59s | Same-prefix denial, numeric cleanup verification, stop before outages |
| Expanded CASA test module | 33 cases included in final safe suite | Additional races, scope, MFA, session/enrollment and archive guards |
| Whitespace/diff check | Passed | No formatting errors detected |
| Python compilation of app/security/collector/test modules | Passed | Syntax validation in local Python 3.10, not production-runtime acceptance |
| Targeted Authlib 1.6.12 install/JOSE import | Passed in isolated temporary Python 3.10 environment | Patch import works; not native authenticated staff-login acceptance |
| Bandit 1.9.4, final source scope | **49 low**, **0 medium/high**, **0 parse errors** | Findings still require disposition; not a certification |
| Narrow credential screen | No current nonignored worktree matches; 573 Git commits, 779 historical commit/file occurrences | Does not prove absence or credential validity |
| pip-audit 2.10.1/OSV | One unique Authlib advisory reported twice among five observed v76 components | Partial baseline graph only; narrowly patched local declaration |
| Read-only legacy staff inventory | 3 Admin, 21 Manager, 1 Student scalar entries | Counts only; no identity bindings inferred or uploaded |

The prior planning result of 64 targeted passes and original project-memory
152-pass baseline are historical observations, not interchangeable with this
isolated suite. Counts changed because new regressions were added and unsafe lab
smoke cases were explicitly excluded.

### Coverage added

- Same-prefix identities, required/malformed/expired/nonfinite claims, duplicate
  bindings, unapproved subjects, Student denial and stale passed-in capabilities.
- Enabled and disabled numeric secret versions, readback verification, delayed
  destruction/failure without leaking provider-body text, concrete-version refs.
- Durable stop despite Secret Manager/metadata outages; repeated disconnect;
  stale token publication and prior-enrollment credential rejection.
- Eight concurrent callback claims and eight code-consumption attempts with one
  winner; consumed states stay used after simulated crash. These are **fake-store**
  races, not Firestore-emulator evidence.
- Management code expiry, per-browser rate limit, replacement/session revocation,
  browser binding, reassignment and session idle expiration.
- MFA missing/stale/future/nonfinite Google claims, public RFC TOTP vector, replay,
  one-use recovery, rate limiting, approved enrollment, secret/hash separation,
  idle/absolute session expiration and polling that does not extend activity.
- Month-end/year clamping and warning/escalation computation without stop/delete
  side effects. No scheduled notification delivery is claimed.
- Missing/partial/unapproved Google Health grants, partial Fitbit grants,
  incomplete tokens before secret write, and unapproved OAuth token endpoints.
- SDK adapter dependency reads before writes (fake SDK decorator/transaction).
- Fitbit archive gate before each request; withdrawal during an in-flight response
  prevents JSON consumption and the next request. Actual network/Drive behavior
  remains an integration gate.

### Scanner procedure and disposition

Bandit command (pinned tool, private temporary output):

```bash
bandit -r controllers entity services utils view pages scripts \
  run_data_collection.py run_drive_archive_collection.py clock.py firbitfilesOrgenizer.py \
  -f json -o .casa-private/bandit.json
```

Create a private output directory before rerunning. Scanner tools are not installed
as app dependencies. Final IDs: B110=18, B105=22, B106=2, B107=1,
B404=2, B607=2, B603=2. The initial narrower screen had 40 lows; including the
legacy collector found seven missing HTTP timeouts, which were locally corrected
with bounded timeouts/no credential redirects. Its unused pickle import was removed.
The final 49 count includes read-only Git tooling heuristics. No blanket suppression
or assertion that all low findings are harmless was added.

The Git scanners use argument arrays, no shell, no user-supplied subcommands and
read-only repository commands. PATH/trusted operator execution is part of the
tooling boundary. Credential-name heuristics are largely empty values/metadata
names/constants/fixtures; manual review did not confirm a live worktree secret.
Exception-pass locations retain required follow-up: uncertain OAuth revocation/
audit/publication and optional old UI paths are not certified by this scan.

Partial dependency command:

```bash
pip-audit --no-deps --disable-pip --vulnerability-service osv \
  -r docs/casa/known-production-components.txt -f json \
  -o .casa-private/partial-dependency-audit.json
```

That input intentionally preserves the observed **v76 baseline** Authlib 1.6.11;
the new source declaration is 1.6.12. The advisory's provider-grant preconditions
were investigated rather than interpreting an installed version as a demonstrated
exploit. See [findings](FINDINGS.md). A full exact-runtime/transitive audit is pending.

## Limitations and next validation

No actual Google MFA claims, native authenticated browser flows, Firestore emulator/
cloud SDK retries, effective IAM, Shared Drive access/deletion, provider refresh/
PKCE compatibility, legacy canonical-gate deployment or manual task-delivery/
completion evidence has been established. Unknown orphan secret writes after
crash and full service/export authorization remain engineering gates.

Before assessment, collect the exact deployed package graph using
`scripts/casa_dependency_inventory.py` in an approved test/runtime context,
repeat complete scans, exercise authenticated negative controls, complete the
legacy/lifecycle work, and tie results to a deployed release and TAC checklist.
Do not upload raw scanner output or private rosters/credentials as public evidence.

## Conclusion

Three confirmed P0 defects are corrected locally and synthetic regression checks
pass. Supporting controls are ready for further internal integration testing, not
for production cutover or an external validation claim. See
[deployment gates and rollback](DEPLOYMENT.md) before any external-state change.
