# DRAFT — retention, withdrawal, and deletion procedure

This procedure cannot be finalized until the PI and ethics committee supply the
exact approved English and Hebrew wording. It must not be used as a substitute for
the approved consent/addendum.

## Required policy fields

- Collection period: `[COPY APPROVED WORDING VERBATIM]`
- Retention duration or endpoint: `[COPY APPROVED WORDING VERBATIM]`
- Treatment of identifiable/pseudonymous data already collected after withdrawal:
  `[COPY APPROVED WORDING VERBATIM]`
- Treatment of de-identified, analyzed, or published data: `[COPY APPROVED WORDING VERBATIM]`
- Request-response deadline and responsible role: `[PI/ETHICS DECISION]`

Deploy the final bilingual text through `RESEARCH_RETENTION_TEXT_EN`,
`RESEARCH_RETENTION_TEXT_HE`, `RESEARCH_DELETION_TEXT_EN`, and
`RESEARCH_DELETION_TEXT_HE`. The authorization gate refuses an allegedly approved
release if any field is empty.

## Operational workflow

1. Authenticate a request using the private connection-management link or confirm
   the pseudonymous study code through the study's approved contact procedure. Do
   not request account passwords, OAuth tokens, or health measurements by email.
2. Record the request in the append-only `health_deletion_requests` table with the
   policy version and timestamp; do not add a participant name or IP address.
3. For withdrawal/disconnection, stop future collection immediately, mark the
   provider connection inactive, remove plaintext token cells, destroy active
   Secret Manager token versions, and request provider-side revocation.
4. Apply the ethics-approved rule above to existing Sheet records, Shared Drive
   archives, backups, analysis datasets, and derived/de-identified material. Record
   the action, responsible operator, completion time, exceptions, and approval basis
   without copying health data into the audit record.
5. Confirm completion to the participant through the approved communication channel.
   Escalate ambiguity, legal holds, published-result questions, or identity disputes
   to the PI/University privacy and ethics contacts before changing research data.

## Verification

- Confirm the token reference cannot be accessed and collection returns no active
  credential for that pseudonymous ID.
- Confirm no future `job_runs` or `archive_manifest` entry contains new data for the
  disconnected provider.
- Review all storage locations named in the final approved policy and document the
  outcome in a minimal audit record.
