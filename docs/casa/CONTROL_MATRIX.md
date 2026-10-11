# Control/evidence matrix

Provisional CASA 2.1.1 topics, not invented control IDs. Reconcile exact IDs and
required test methods against TAC's assigned specification before assessment.

| Control/topic | Local evidence | What is not established |
| --- | --- | --- |
| Strong staff identity binding | `IdentityTests`; exact issuer/sub/email/expiry/audience checks | Approved real subjects and roster migration |
| Student denial and project isolation | Access tests; stale capability checks; scoped registry saves | Approved staff workflow impact; comprehensive service/export boundaries |
| MFA/session lifecycle | `SessionAndEnrollmentTests`, enrollment/recovery/idle/absolute tests | Actual Google claims, browser recovery/logout across tabs, cloud authority outages |
| Provider OAuth disclosure/replay | Firestore state transition adapter; 8 concurrent callback claims; disclosure tests | Actual Firestore transaction retries; provider PKCE feasibility and browser flow |
| Granted permissions | Missing/partial/extra Google scopes and partial Fitbit scope tests | Actual provider grants/refresh behavior and cancellation evidence |
| Participant management | Concurrent code consumption, expiry, replacement, browser binding and reassignment tests | Edge anti-abuse; real replacement delivery workflow; browser sessions |
| Withdrawal stops collection first | Outage tests; versioned stop fence; provider pre/post guards | All legacy paths; network-in-flight result disposal with real SDKs |
| Credential protection/destruction | Numeric version state/readback tests; pinned refs; publication-review task | Real synthetic secret cleanup; old ref ownership; uncertain writes reconciliation |
| Enrollment lifecycle | Calendar month clamp and pure reminder tests; explicit enrollment initialization | Historical migration, scheduler delivery, completion checklist/Admin verification |
| Archive/deletion scope | Enrollment suffix/manifest key; in-flight upload review; no watch-wide deletion | Shared Drive revisions/trash/derived/mailbox/backup completion evidence |
| Safe diagnostics | Redacted OAuth/provider errors; alert HR removal; redacted scan scripts | All legacy log/error paths; effective production retention/log access |
| Dependencies/source/secrets | Local Bandit; five-component OSV scan; narrow 573-commit secret screen | Full exact slug dependency/OS graph, complete scanner coverage, active key review |
| Platform permissions/transport | Existing architecture/config tests | Effective IAM, cookies, CSRF, CORS, WebSocket origin and membership evidence |
| Release correspondence | Base branch/commit recorded; local test report | Deployed release/hash matching these changes; signed external validation |

Unit concurrency tests use a lock-backed injected store. They do not certify
Firestore isolation, production IAM, network behavior or authenticated browser
security. The SDK adapter's read-before-write test is a fake adapter test, not an
emulator test.
