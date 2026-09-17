# Google Health reviewer video and submission runbook

Use this only after every release gate in `google_publication_checklist.md` is
complete. Record with a dedicated test participant and synthetic/non-research data.
Do not show credentials, authorization codes, tokens, participant names, Sheet
contents, Secret Manager payloads, Heroku config values, or real health data.

## Before recording

- Confirm the production app name is **AdmonTracker** everywhere.
- Confirm the External audience, support email, developer contacts, authorized
  domain, homepage, Privacy Policy, and Terms URLs in Google Auth Platform.
- Confirm both callbacks are registered on their respective web clients:
  - Staff: `https://app.admontracker.online/oauth2callback`
  - Participant: `https://app.admontracker.online/?google_health_callback=1`
- Confirm the approved addendum is downloadable, the disclosure gate is enforced,
  plaintext fallback is off, and the three requested scopes exactly match the app.
- Set the Google consent screen language to English for the recording.
- Open an incognito/private browser window and use an account created only for review.

## Recording sequence

1. Start signed out at `https://admontracker.online/`. Show the app identity,
   University of Haifa, research purpose, and public legal links.
2. Open Privacy Policy, Terms of Use, and Research Ethics. Show the Limited Use
   statement and preview/download controls for the approval, accreditation evidence,
   approved consent, and approved Google Health addendum.
3. Sign in as authorized test staff. Create a test participant link while showing
   the adult-eligibility and approved-consent attestations.
4. Open the private participant link in the test-participant browser. Show language
   selection, the in-context disclosure, document downloads, retention/deletion
   language, and every affirmative consent/signature control.
5. Continue to Google. Show the entire English Google consent screen and visibly
   show exactly these scopes:
   - activity and fitness, read-only;
   - health metrics and measurements, read-only;
   - sleep, read-only.
6. Complete authorization. Show the successful connection and the private
   connection-management link without exposing its token in the video.
7. Show the application features that use each scope: activity/calories, physiology,
   and sleep. Explain that data is used for the approved research and completeness
   monitoring, not diagnosis or emergency monitoring.
8. Open connection management. Demonstrate disconnect/revocation and record a
   deletion request. Explain how already collected data is handled using the exact
   ethics-approved wording.
9. End on the public contact information. State that data is not sold, used for
   advertising/credit decisions, or used for unrelated model training.

## Submission

1. Upload the English video as an unlisted link accessible without requesting access.
2. In Google Auth Platform, use **Publish App**, then **Prepare for Verification**.
3. Supply a scope-by-scope justification that matches the video and
   `google_scope_justification.md` exactly.
4. Submit the Google Health research intake with the approval/waiver letter,
   accreditation evidence and registry identifier, exact requested data/rationale,
   approved consent/addendum, retention/deletion procedure, and security/data-flow
   description.
5. Monitor the project owner/editor email addresses and answer reviewer questions
   without changing the production UI, branding, callbacks, or scopes shown in the
   video. Complete a CASA assessment if Google requests one.

Current official references:

- https://developers.google.com/health/policies/health-api-user-data-and-research-policy
- https://developers.google.com/health/policies/health-api-developer-user-data-policy
- https://support.google.com/cloud/answer/13464321?hl=en
- https://support.google.com/cloud/answer/13461325?hl=en
