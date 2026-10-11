# Retention, withdrawal, and deletion procedure — Study 385/23

This procedure implements the approved Google Health addendum, final version 1.0.

## Collection and routine deletion

1. Record the date the participant's Google Health connection is activated.
2. Stop collection one month after activation, on disconnection, or on withdrawal,
   whichever occurs first, and cancel the application's authorization in the
   assigned demo account.
3. Retain measurement data only during the one-month collection period and one
   additional month for processing and quality checks.
4. No later than one month after measurement ends, permanently delete the Google
   Health measurement data and the link between the study code and identity.
5. Record completion of revocation and deletion in the study audit record without
   retaining the deleted measurements.
6. Retain signed consent and administrative records that do not contain measurements
   according to University and research requirements.

## Withdrawal

1. Accept requests at `radmon@psy.haifa.ac.il` or `+972-4-824-0964`; identify the
   participant by study or watch code and never request a personal password.
2. Stop collection immediately and cancel the application authorization in the
   assigned demo account.
3. Permanently delete all collected participant data unless the participant gives
   separate and explicit consent at that time to retain it.
4. Document any separate retention consent independently from the original consent.
5. Explain that summary results already published cannot be removed from an existing
   publication.

These are research-team operational steps. AdmonTracker's connection-management page
supports revocation and recording a deletion request; the research team remains
responsible for verifying deletion across every approved storage location.

## CASA operations ownership — pending implementation/cutover

The Manager owns manual stopping and per-enrollment deletion; Admin verifies
completion, is backup, and handles overdue escalation. The PI approves policy
exceptions and confirms any applicability to legacy Fitbit consent. No routine
automatic collection-end stopping or automatic research-data deletion is added.

Use original activation, a calendar-month collection end (month-end clamped,
Asia/Jerusalem display), actual measurement end and the approved deletion due
date. Reauthorization must not extend them. Deadline warning computation exists
locally; scheduled 72h/24h/deadline delivery, failure handling and a durable
completion checklist are unfinished release gates.

Report collection blocked, provider revocation confirmed/pending, secret cleanup
confirmed/pending and research-data deletion requested/verified independently.
Do not report deletion merely because a token was revoked. Cover Firestore, Drive
files/revisions/trash, Sheet mirrors, tokens, caches, temporary files, exports,
derived data/identity links, mailboxes and backups. Never delete by reusable watch
name alone. Details: [internal operations runbook](../casa/RUNBOOK.md).
