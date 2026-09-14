import streamlit as st


st.set_page_config(
    page_title="Terms of Service - Wearable Research Manager",
    page_icon="📄",
    layout="wide",
)

st.title("Terms of Service")
st.caption("Wearable Research Manager • Effective August 16, 2026")

st.markdown(
    """
## 1. About these Terms

These Terms of Service (**Terms**) govern access to and use of Wearable Research
Manager (the **Service**), operated by **Prof. Roee Admon's Stress &
Psychopathology Lab at the University of Haifa** (the **Lab**, **we**, **us**, or
**our**). By accessing or using the authenticated Service or public guest
demonstration, you agree to these Terms. If you do not agree, do not use the Service.

The Service is a private research-operations tool for invited personnel. Research
participants may interact with a separate account-authorization flow; their
participation is governed primarily by the applicable informed-consent documents,
ethics approval, and privacy notices.

## 2. Eligibility and authorized access

Authenticated access is limited to invited research staff and other people approved
by the Lab. You must use your own authorized account, provide accurate account
information, protect your credentials, and promptly report suspected unauthorized
access. You may access only the projects, participants, devices, and functions
assigned to your role.

You must not share credentials, impersonate another person, reuse a participant's
authorization link, or attempt to gain broader permissions. The Lab may suspend or
terminate access to protect participants, research integrity, the Service, or third
parties.

## 3. Guest demonstration

The public guest demonstration contains fictional, synthetic examples and is
read-only. It does not provide access to real participants, watches, health data,
OAuth clients, or production systems. Guest visitors must not submit personal,
confidential, health, study, or real device information; connect a real account or
device; attempt to activate disabled functions; or rely on sample values as factual
research or medical information.

## 4. Permitted research use

Authorized users may use the Service only for legitimate activities within an
approved research project and within their assigned role. Users must comply with
the applicable study protocol, ethics or review-board approval, informed-consent
documents, University policies, privacy and security requirements, and all relevant
laws and platform terms.

Users are responsible for confirming that a participant is eligible and has
completed any required informed consent before connecting an account or processing
research data. Access to participant data must be limited to people with a genuine
need to know.

## 5. Prohibited conduct

You must not:

- access, use, disclose, or change data without authorization;
- bypass role, project, guest, OAuth-state, or other security controls;
- scrape, bulk-export, probe, reverse engineer, disrupt, or overload the Service or a connected platform;
- re-identify or attempt to re-identify a participant or combine pseudonymous data with identifying sources without explicit approval;
- sell data or use it for advertising, data brokerage, credit, employment, insurance, unrelated marketing, or unrelated model training;
- upload malware, falsify research or device data, interfere with audit records, or use the Service unlawfully; or
- disclose participant information to an unauthorized person or through an unapproved channel.

## 6. Third-party platforms

The Service may interoperate with Google Sign-In, Google Cloud and Workspace,
Google Health, Fitbit, Streamlit, approved survey and AppSheet tools, and email
services. Your use of those platforms may be subject to their own terms and privacy
policies. Availability, authorization, data formats, and functionality may change
when a provider changes or discontinues a service.

You must not use the Service in a way that violates the
[Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
[Google OAuth policies](https://developers.google.com/identity/protocols/oauth2/policies),
[Google Health API policies](https://developers.google.com/health/policies/health-api-user-data-and-research-policy),
or [Fitbit Platform Terms](https://dev.fitbit.com/legal/platform-terms-of-service/).

## 7. Not medical care or emergency monitoring

The Service is a research and operational-management tool. It is **not** medical
advice, a medical device, a diagnostic service, an emergency-monitoring system, or
a substitute for professional clinical judgment. Device readings, synchronization
times, alerts, and missing-data indicators may be delayed, incomplete, inaccurate,
or unavailable. They must not be used to make urgent treatment or safety decisions.
In an emergency, contact the appropriate emergency service or qualified healthcare
professional.

## 8. Confidentiality and security responsibilities

Authorized users must treat participant and research information as confidential,
use approved devices and networks, follow access and data-handling procedures, and
report suspected loss, disclosure, or compromise promptly. Do not place account
tokens, credentials, or unnecessary health information in email, chat, support
requests, or the guest demonstration.

## 9. Intellectual property

The Service's software, interface, documentation, and original content are owned by
or licensed to the Lab or their respective owners. Subject to these Terms, invited
users receive a limited, revocable, non-transferable right to use the Service for
approved research operations. No other rights are granted.

Fitbit is a registered trademark or trademark of Fitbit, LLC in the United States
and certain other countries. A list of Fitbit logos can be found at the
[Fitbit trademark list](https://www.fitbit.com/legal/trademark-list).

The Wearable Research Manager application is designed for use with the Fitbit
platform. This application is not authored by Fitbit, and Fitbit does not service
or warrant the functionality of this application. Other names and marks belong to
their respective owners.

## 10. Availability and changes

We may maintain, modify, suspend, restrict, or discontinue any part of the Service,
including to respond to security, legal, ethics, research, or third-party-platform
requirements. We do not promise uninterrupted operation or permanent retention of
guest-demo settings. Guest state may reset at any time.

## 11. Disclaimers and limitation of liability

To the maximum extent permitted by applicable law, the Service and guest examples
are provided on an “as is” and “as available” basis without warranties of
merchantability, fitness for a particular purpose, non-infringement, uninterrupted
availability, or error-free data. Nothing in these Terms excludes a right or
liability that cannot lawfully be excluded.

To the maximum extent permitted by law, the Lab and its personnel will not be liable
for indirect, incidental, special, consequential, or punitive loss arising from use
of or inability to use the Service, reliance on device or guest data, or third-party
platform failure. Users remain responsible for complying with their professional,
research, security, and legal duties.

## 12. Suspension and termination

We may immediately restrict or terminate access for suspected unauthorized use,
security risk, breach of these Terms, end of a project or role, withdrawal of a
required platform authorization, or legal or ethics requirements. On termination,
you must stop using the Service and continue to protect confidential information.

## 13. Privacy

Our [Privacy Policy](./Privacy_Policy) explains how the Service processes
information and is incorporated into these Terms. Study-specific informed-consent
documents and privacy notices continue to apply.

## 14. Governing law

These Terms are governed by the laws of the State of Israel, without selecting an
exclusive court or limiting any mandatory right or forum available under applicable
law.

## 15. Changes to these Terms

We may update these Terms to reflect changes in the Service, research requirements,
platform rules, or law. The effective date at the top identifies the current
version. Where required, material changes will be communicated before they apply.

## 16. Contact

Questions about these Terms or the Service may be sent to
**edenede2@gmail.com**.
"""
)

st.divider()
col1, col2 = st.columns(2)
with col1:
    st.page_link("app.py", label="Return to Wearable Research Manager", icon="🏠")
with col2:
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy Policy", icon="🔒")
