import streamlit as st


st.set_page_config(
    page_title="Privacy Policy - Wearable Research Manager",
    page_icon="🔒",
    layout="wide",
)

st.title("Privacy Policy")
st.caption("Wearable Research Manager • Effective August 16, 2026")

st.info(
    "This policy supplements, and does not replace, the informed-consent documents "
    "and privacy notices for each research study."
)

st.markdown(
    """
## 1. Who operates the service

Wearable Research Manager (the **Service**) is operated by **Prof. Roee Admon's
Stress & Psychopathology Lab at the University of Haifa** (the **Lab**, **we**,
**us**, or **our**). The Service supports approved research operations involving
wearable devices, study questionnaires, data-quality monitoring, and participant
account connections.

For privacy questions or requests concerning access, correction, withdrawal, or
deletion, contact **edenede2@gmail.com**.

## 2. Who this policy covers

This policy covers invited research staff who use the authenticated Service,
research participants whose information is processed through it, and visitors who
use the public guest demonstration. A study-specific consent form or notice may
provide additional or more specific information. If there is a conflict concerning
research participation, the applicable approved study documents and law control.

## 3. The guest demonstration

The guest demonstration uses bundled, fictional examples only. Names, participant
identifiers, watch identifiers, health metrics, survey responses, alerts, and
configuration values shown there are synthetic. Guest sessions do not load a
production spreadsheet, access real watches, call Fitbit or Google Health APIs,
create authorization links, send messages, or save changes.

The hosting platform may still process technical information needed to deliver and
secure the website, such as IP address, browser information, timestamps, session
cookies, and diagnostic logs. Visitors must not enter personal, confidential, or
real participant information into the guest demonstration.

## 4. Information we process

Depending on the study and a person's role, the Service may process:

- Google-authenticated account information, such as name, email address, role, project assignment, and login/session information;
- pseudonymous participant identifiers, study assignments, device or watch identifiers, connection status, and authorization credentials or tokens;
- health and activity information authorized for the study, including heart rate, steps, activity, sleep, and related timestamps;
- device-operational information, including battery level, synchronization time, data-availability status, and connection errors;
- ecological momentary assessment, Qualtrics, AppSheet, or other approved study responses, which may include information about pain, fatigue, mood, sleep quality, or mental well-being;
- alert thresholds, recipient details, review decisions, research-team communications, and support requests; and
- audit, security, API, and operational logs used to run, troubleshoot, and protect the Service.

We seek to use pseudonymous identifiers rather than direct identifiers where the
research design permits. The exact information collected for a participant is
described in the applicable study materials and authorization screen.

## 5. Sources of information

Information may come from the individual, authorized research staff, Fitbit or the
Google Health API after authorization, Google Sign-In, approved study survey or
data-collection tools, research spreadsheets, and the Service's operational logs.
We do not use the guest demonstration as a source of research data.

## 6. Why we use information

We use information to administer approved studies; authenticate and authorize
staff; connect pseudonymous participant accounts and devices; collect and review
authorized research measurements; monitor data completeness and device status;
configure and deliver operational alerts; support participants and researchers;
protect the Service; comply with ethics approvals, consent documents, platform
rules, and law; and produce research outputs as permitted by the applicable study.

We do **not** sell personal or participant data. We do not use it for advertising,
data brokerage, lending or credit decisions, unrelated marketing, or training
general-purpose artificial-intelligence or machine-learning models.

## 7. Google and Google Health Limited Use

Wearable Research Manager's use and transfer to any other app of information
received from Google APIs will adhere to the
[Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
including the Limited Use requirements.

The use of information received from Google Health API will adhere to the
[Google Health API User Data and Health Research Policy](https://developers.google.com/health/policies/health-api-user-data-and-research-policy),
including the Limited Use requirements.

Access is requested only for data needed for the approved, user-facing research
function. Google user data is not transferred or made available for prohibited
purposes. Human access is limited to authorized people with a genuine study,
security, support, or legal need, consistent with the applicable consent and policy.
OAuth clients for development, staging, and production are kept separate where
required; the Service requests only necessary scopes, uses authorized secure
redirects, protects client credentials and user tokens, and revokes or deletes
tokens when access is no longer required, in accordance with the
[Google OAuth 2.0 Policies](https://developers.google.com/identity/protocols/oauth2/policies).

## 8. When information is shared

Information may be available to authorized members of the relevant research team
and University personnel with a legitimate need to know. It may also be processed
by service providers and integrations needed for the study or Service, such as
Google Sign-In, Google Cloud and Google Workspace, Google Health, Fitbit,
Streamlit hosting, approved survey or AppSheet tools, and email infrastructure.

We may disclose information when required by law; to investigate or prevent abuse
or a security incident; or to approved research collaborators when the study
consent, ethics approval, contract, and applicable platform policies permit it.
Providers may process information outside Israel. When cross-border processing
occurs, we use contractual, organizational, and technical safeguards required by
the applicable study and law.

## 9. Retention, withdrawal, and deletion

Retention periods follow the applicable study protocol, ethics approval, informed
consent form, University requirements, platform requirements, and law. We retain
information only as long as needed for those purposes. Authorization tokens are
revoked or deleted when access is no longer needed, subject to technical and legal
requirements.

To ask to access or correct information, withdraw from a study, revoke a device
connection, or request deletion, email **edenede2@gmail.com** and identify the
relevant study using the instructions in the consent form. We may need to verify
the request without unnecessarily collecting additional identifying information.

Withdrawal may stop future collection but may not require deletion of information
that has already been irreversibly de-identified, included in an aggregate analysis
or publication, or retained under a legal, ethics, scientific-integrity, or safety
obligation. Study-specific documents explain any additional limitations. A person
may also revoke third-party access through their Google or Fitbit account settings.

## 10. Security

We use role- and project-based access controls, authentication, least-privilege
design, read-only guest isolation, session-data clearing, protected credentials,
and appropriate transport and storage safeguards. Access is limited to authorized
personnel, and operational events may be logged for security and reliability.
No system is completely secure, and we cannot guarantee that a security incident
will never occur.

## 11. Research involving minors

Where an approved study includes minors, participation and processing must follow
the applicable ethics approval and the consent or assent of a parent, guardian, or
participant as required. The public guest demonstration is not directed to children
and must not be used to submit personal information.

## 12. Changes to this policy

We may update this policy when the Service, research practices, platform rules, or
law change. Material changes affecting existing research data will be communicated
and consent will be obtained when required. The effective date at the top identifies
the current version.

## 13. Contact

Questions, concerns, and privacy requests may be sent to
**edenede2@gmail.com**. Please do not include health information or account tokens
in an unencrypted email.
"""
)

st.divider()
col1, col2 = st.columns(2)
with col1:
    st.page_link("app.py", label="Return to Wearable Research Manager", icon="🏠")
with col2:
    st.page_link("pages/09_Terms_of_Service.py", label="Terms of Service", icon="📄")
