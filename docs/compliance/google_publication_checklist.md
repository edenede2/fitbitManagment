# Google production publication checklist

## Release gate

Do not submit until every checkbox in this section is complete.

- [ ] PI and ethics committee approved the bilingual Google Health addendum and
      its method of consent.
- [ ] Exact approved retention and deletion wording is deployed in both languages.
- [ ] The final addendum PDF is deployed and its version/hash appears in a test
      `health_oauth_consents` record.
- [ ] `PARTICIPANT_DISCLOSURE_ENFORCED=true` and direct provider-link bypass tests fail.
- [ ] Plaintext OAuth clients/tokens were migrated, verified, blanked in Sheets,
      and `ALLOW_PLAINTEXT_SECRET_FALLBACK=false`.
- [ ] Drive permissions were manually matched to the authorized study-team list.
- [ ] Homepage, Privacy, Terms, and Research Ethics pages work while signed out.

## Google Auth Platform

- [ ] Use the canonical production project and an External audience.
- [ ] Verify the `admontracker.online` Search Console domain with a project owner/editor.
- [ ] App name: `Wearable Research Manager`.
- [ ] Support/developer contact: `radmon@psy.haifa.ac.il`; keep technical contacts
      in project notification settings.
- [ ] Register homepage, Privacy Policy, and Terms of Use URLs on the production domain.
- [ ] Use distinct web clients for staff OIDC and participant health authorization.
- [ ] Register the exact staff and health callback URLs from `HEROKU_DEPLOYMENT.md`.
- [ ] Request only:
  - `googlehealth.activity_and_fitness.readonly`
  - `googlehealth.health_metrics_and_measurements.readonly`
  - `googlehealth.sleep.readonly`
- [ ] Complete the Google Health research intake/attestation.

## Evidence package

- [ ] Study approval 385/23 in English.
- [ ] University accreditation/governance evidence, relevant Hebrew page, and an
      official English confirmation containing the actual registry identifier.
- [ ] Approved Hebrew consent and approved bilingual Google Health addendum.
- [ ] `data_flow_and_security.md` reviewed against actual production configuration.
- [ ] `google_scope_justification.md` was checked against participant-visible
      features, exact data types, final consent, and the reviewer video.
- [ ] `retention_deletion_procedure_draft.md` was completed with verbatim approved
      policy text and reviewed by the responsible institutional roles.
- [ ] Retention, withdrawal, disconnect, deletion-request, incident-response, and
      service-account/key-rotation procedures.
- [ ] If Google classifies the storage pattern as requiring a security assessment,
      complete the requested CASA process.

## Reviewer video (English, unlisted)

- [ ] Use a dedicated test participant with no real research data.
- [ ] Start signed out on the public homepage and open Privacy, Terms, and Ethics documents.
- [ ] Show staff creation of a link with adult and consent attestations.
- [ ] Show the participant disclosure, language option, document download, and all confirmations.
- [ ] Show the Google consent screen and explain every requested scope.
- [ ] Show the resulting participant-visible/staff-visible features for activity,
      physiology, and sleep.
- [ ] Show the private management link, provider revocation, and deletion-request workflow.
- [ ] Show that no token or client secret appears in Sheets, UI errors, or logs.

## Post-submission and operations

- [ ] Keep the OAuth app in testing until Google accepts the production submission.
- [ ] Preserve the exact production UI and scope list shown in the video during review.
- [ ] Monitor Google verification email, token refresh, `job_runs`, `clock_status`,
      `archive_manifest`, and provider errors.
- [ ] Re-review privacy/consent and repeat required verification after material scope,
      data-use, processor, domain, or retention changes.
