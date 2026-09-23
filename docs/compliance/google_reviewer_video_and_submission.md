# Google OAuth reviewer video: recording script and submission runbook

Record one continuous English video of approximately 12 minutes. Use the exact
production app, production OAuth client, and branding submitted for verification.
Use a dedicated study test account and a test watch containing only non-research
data. The recording must show the complete authorization flow, the complete Google
consent screen in English, and working functionality for every requested scope.

For this controlled demonstration, the researcher may operate both sides of the
flow. State clearly that the Google account is a dedicated demo account, the six
watches are test devices personally charged, worn, and synchronized by the
researcher, and no real participant or participant data appears in the recording.
Use one of those watches as the assigned watch in the recorded flow. This role
switching is only for the verification demonstration; in normal study operation,
authorized staff and participants complete their respective steps separately.

## Rehearsal setup — complete before recording

1. In **Google Auth Platform → Audience**, confirm the app is External. If the app
   is still in Testing, add the dedicated reviewer/demo Google account under **Test
   users**. A non-test account will be blocked before verification.
2. In **Google Auth Platform → Clients**, open the participant web client used by
   AdmonTracker. Confirm its authorized redirect URI is exactly
   `https://app.admontracker.online/?google_health_callback=1`. Do not use the staff
   login client for participant health access.
3. In **Data Access**, confirm the client requests the four scopes listed below and
   no write or mindfulness scope. Keep the submitted list identical to the public
   site, approved addendum, and recorded consent screen.
4. On the assigned test watch, create recent test-only steps, heart-rate, sleep, and
   paired-device data. Synchronize the watch immediately before recording and verify
   that Google returns model, battery, and last-sync values.
   If you maintain six test watches for development and verification, confirm that
   every watch contains only your own test data and select one as the assigned watch
   for the recorded authorization flow.
5. Rehearse once with a disposable pseudonymous code. For the final take, use a new
   code such as `REVIEW_DEMO_02`; a consumed OAuth state cannot be reused.
6. Run the focused release checks from the repository root:

   ```bash
   python3 -m pytest -q test_public_verification_site.py test_production_readiness.py
   ```

7. Open the public URLs in a signed-out private window and check every PDF download.
   Open the production app in a separate staff browser profile so the staff and
   participant accounts cannot be confused.

## Recording gates

Do not record the final video until all of these checks pass:

- `https://admontracker.online/` is public while signed out and identifies
  **AdmonTracker**, University of Haifa, Study 385/23, and the app's purpose.
- Privacy, Terms, and Research Ethics are public and the approved English and Hebrew
  addenda download successfully.
- Google Auth Platform contains the same app name, logo, production domain, public
  URLs, and participant OAuth client used in the video.
- The participant OAuth client requests exactly these four scopes:
  - `googlehealth.activity_and_fitness.readonly`
  - `googlehealth.health_metrics_and_measurements.readonly`
  - `googlehealth.sleep.readonly`
  - `googlehealth.settings.readonly`
- `PARTICIPANT_DISCLOSURE_ENFORCED=true` and plaintext secret fallback is disabled.
- Firestore is the primary operational store, the protected Sheets copy is only the
  configured recovery mirror, OAuth tokens are in Secret Manager, and wearable JSON
  archives are in the restricted Shared Drive.
- The test watch has recent steps, heart-rate, sleep, and paired-device data. Confirm
  that **Device Details** displays model, battery, and last synchronization time.
- Have a second private participant link ready in case the first OAuth state expires
  or is consumed during rehearsal.

## Privacy-safe recording setup

- Record only the browser content area. Hide the address bar whenever a private
  authorization or management URL is open because its query string contains an
  access token or one-time state.
- Turn off browser notifications, password-manager overlays, autofill suggestions,
  bookmarks, and email/chat pop-ups.
- Never show passwords, one-time codes, OAuth client secrets, access/refresh tokens,
  Heroku configuration, Secret Manager values, Google Sheets contents, or real
  participant identifiers.
- Use a pseudonymous watch name such as `REVIEW_DEMO_01`. Do not use YN4 or another
  live research participant in the recording unless it is formally designated as
  the non-research reviewer account.
- Show only the dedicated demo Google account and your own test-watch data. Do not
  show the identifiers, dashboards, or measurements of any enrolled participant or
  another member of the research team.
- Use a staff test user that can see only the test project if possible. Otherwise,
  crop or blur any selector that could reveal a real project or watch code before
  uploading the video.
- Set the Google consent screen language to **English** using its language control.
- Zoom to a readable level. Pause after each page and scroll slowly enough for a
  reviewer to read the relevant text.

## Timed, word-for-word script

The text in quotation marks is the narration to say. The actions in the **Show and
do** sections are not narration.

### 0:00–0:45 — Introduce the app and demonstration setup

**Show and do:** Start at `https://admontracker.online/` in a signed-out private
browser window. Keep the AdmonTracker name, logo, University of Haifa identity, and
page URL visible.

**Say:**

> This is AdmonTracker, the production research application operated by the Stress
> and Psychopathology Lab at the University of Haifa for ethics-approved Study
> 385/23. The public site is available without login. The application supports
> participant authorization, wearable-data collection, data-completeness checks,
> and authorized study-team device management. For this verification recording, I
> am using a dedicated demo Google account and one of six test watches that I
> personally charge, wear, and synchronize. All measurements shown are my own test
> data; no real participant or participant data appears in this video. To show the
> complete end-to-end journey, I will first act as the researcher and then as the
> demo participant. In normal study operation, staff and participants complete
> their respective steps separately.

### 0:45–1:35 — Show the public purpose and four scopes

**Show and do:** Scroll through **Purpose and functionality** and **Why AdmonTracker
requests Google access**. Pause on the table that maps each exact read-only scope to
the data used and the feature you will demonstrate later.

**Say:**

> Participants use a Google account designated for the study and decide whether to
> grant read-only Google Health access. AdmonTracker requests four read-only scopes:
> activity and fitness; health metrics and measurements; sleep; and settings. The
> settings scope is used only for the assigned watch model, battery level and status,
> and last synchronization time, so staff can identify charging or synchronization
> problems. The app does not request Google Health write access or mindfulness data.

### 1:35–3:00 — Show public policy and ethics evidence

**Show and do:** Open **Privacy Policy** in a new tab. Show the sections describing
the Google data categories, purpose, storage and protection, sharing, retention,
withdrawal/deletion, and Google API Services Limited Use. Open **Terms of Use** and
briefly show voluntary research participation and research-only use. Open **Research
Ethics** and show the study approval and both approved Google Health addenda; click
one PDF preview or download control so the reviewer can see that it works.

**Say:**

> The Privacy Policy identifies every Google data category and explains its research
> purpose, storage, access, sharing, retention, withdrawal, deletion, and security.
> It affirms compliance with the Google API Services User Data Policy, including the
> Limited Use requirements. Google user data is not sold, used for advertising or
> credit decisions, or used to train unrelated general-purpose models. The Terms of
> Use explain that participation is voluntary and research-only. The Research Ethics
> page provides the Study 385/23 approval and the approved participant documents in
> English and Hebrew. Operational metadata is stored in Google Cloud Firestore,
> OAuth tokens are stored in Google Secret Manager, and research archives are stored
> in a restricted University Shared Drive.

### 3:00–3:40 — Sign in as authorized staff

**Show and do:** Open `https://app.admontracker.online/`. Before signing in, briefly
show the public purpose text, bilingual tabs, exact four-scope mapping, and public
legal links. Then sign in using the authorized test staff account. Pause recording
while typing credentials or completing a one-time code if necessary, then resume on
the authenticated welcome page. Show the curated staff navigation.

**Say:**

> I am now acting in the researcher role and entering the production application as
> an authorized test staff member.
> Its signed-out page explains the research purpose and Google Health permissions
> without requiring a login.
> Staff authentication uses a separate OAuth client with only OpenID, profile, and
> email. Participant health permissions are not requested by the staff-login client.
> The staff navigation exposes only the operational research pages.

### 3:40–4:40 — Generate a participant authorization link

**Show and do:** Open **Connect wearable**. In **Add a new watch & generate
authorization link**, enter `REVIEW_DEMO_01`, choose the test project, select
**Google Health**, select the active production Google Cloud/OAuth client, select
purpose **test**, and leave **Active** selected. Pause on the four-permission mapping
shown below the provider selection. Check both staff attestations:

1. the participant completed the current ethics-approved study consent and Google
   Health addendum; and
2. the participant is at least 18 years old.

Click **Add watch & generate link**. Keep **Copy private authorization link**
collapsed so the one-time state is never shown, and click **Open authorization**.

**Say:**

> An authorized staff member assigns a pseudonymous watch code and selects the
> production Google Health client. Before a participant link can be generated, staff
> must attest that the participant is an adult and completed the current
> ethics-approved consent and Google Health addendum. For this controlled test, I am
> the adult demo-account holder and have completed the same displayed consent steps;
> this does not enroll me as a research participant. This private, single-use link
> contains no participant name and expires if it is not used.

### 4:40–6:05 — Demonstrate participant disclosure and acknowledgement

**Show and do:** On **Participant Authorization**, select **English**. Keep the
address bar hidden. Slowly show the Google Health disclosure, including the five
health/research data types and the watch model/battery/synchronization fields. Click
**Save this disclosure**. Expand **Study documents** and show the downloadable
approved English addendum. Check all six acknowledgement boxes and click
**Acknowledge and continue**.

**Say:**

> I am now switching to the demo-participant role using the dedicated demo account.
> Before Google authorization, the participant receives an in-context disclosure
> in English or Hebrew and can save a copy. It lists heart rate, respiratory rate,
> steps and physical activity, sleep, plus the assigned watch model, battery status,
> and synchronization time. It explains the study purpose, read-only access,
> storage, authorized access, retention, withdrawal, and deletion. The participant
> can download the approved study documents and must affirmatively acknowledge every
> item before the provider authorization button is created.

**Show and do:** After the acknowledgement is recorded, click **Continue to Google
Health**.

**Say:**

> The acknowledgement is recorded with the disclosure version and document hash.
> The provider authorization URL is generated only after the required participant
> acknowledgement.

### 6:05–7:30 — Show the complete Google consent screen

**Show and do:** On Google's page, confirm the heading names **AdmonTracker**. If
needed, switch the consent-screen language to **English** using the control at the
bottom of the page. Show the complete consent screen from top to bottom. Expand any
permission details and visibly show all four permissions before approving:

1. activity and fitness, read-only;
2. health metrics and measurements, read-only;
3. sleep, read-only; and
4. settings, read-only.

Then approve access using the Google button presented on screen.

**Say:**

> This is the complete Google consent screen in English, and the app name matches the
> submitted production app. The activity-and-fitness permission supplies steps and
> physical activity. Health metrics and measurements supplies heart rate and
> respiratory rate. Sleep supplies sleep sessions, timing, duration, and stages when
> available. Settings supplies only the assigned watch model, battery level and
> status, and last synchronization time. All four permissions are read-only and are
> the narrowest Google Health permissions that cover the data in the approved
> participant addendum.

### 7:30–7:55 — Show successful connection without exposing a token

**Show and do:** Wait for the AdmonTracker success page. Show the success message and
the **Manage this connection** button. Do not expose a raw management URL. Do not
click the management button yet; leave this tab open for the final segment.

**Say:**

> Google returned to AdmonTracker and the test watch is connected successfully. The
> page provides a private management button for disconnect and deletion requests.
> The underlying token-bearing URL is not printed on the page or spoken in this
> video.

### 7:55–10:15 — Demonstrate each requested scope in the dashboard

**Show and do:** Return to the authenticated staff tab. Open **Dashboard**, select
`REVIEW_DEMO_01`, and point out **Provider: Google Health**.

**Say before loading the first signal:**

> I am switching back to the researcher role. The dashboard is available only to
> authorized study staff. The selected pseudonymous record is the test watch and
> demo account I disclosed at the beginning of this video.

1. In **Signal Data**, choose a short date range containing test data, select
   **Steps**, and click **Load Data**. Pause on the steps result.
2. Select **Heart Rate** and click **Load Data**. Pause on the heart-rate result.
3. Select **Sleep** and click **Load Data**. Pause on sleep timing/duration.
4. Open **Device Details** and click **Refresh Device Data**. Pause on device model,
   device type if returned, battery level/status, and **Last Synced**.

If Google returns no value for one category, create fresh test-account data and
re-record. Do not submit a final video that shows errors or unavailable data for a
scope being justified.

**Say while showing Steps:**

> This steps view demonstrates the activity-and-fitness read-only scope. Authorized
> staff use it for the approved physical-activity measure and data-completeness
> checks.

**Say while showing Heart Rate:**

> This heart-rate view demonstrates the health-metrics-and-measurements read-only
> scope. The same scope supplies the approved respiratory-rate measure for bounded
> research collection when the assigned device makes it available.

**Say while showing Sleep:**

> This sleep view demonstrates the sleep read-only scope. Sleep patterns are an
> explicit variable in Study 385/23.

**Say while showing Device Details:**

> This Device Details view demonstrates the settings read-only scope. AdmonTracker
> retrieves the assigned watch model, battery level and status, and last
> synchronization time to identify charging or synchronization failures and assess
> data completeness. The application does not change any Google Health setting.

### 10:15–11:35 — Demonstrate participant controls

**Show and do:** Return to the successful-connection tab and click **Manage this
connection** while keeping the browser address bar outside the recording. Select
**English**. Show **Provider: Google Health**, the disconnect explanation, the
confirmation checkbox, and the **Deletion request** section.

For the final submitted take, demonstrate the deletion-request action first if the
approved text and button are present. Then select **I want to disconnect and stop
future collection** and click **Disconnect connection**. Show the confirmation that
future collection stopped and locally stored tokens were removed. Use a disposable
review account because disconnect revokes the connection.

**Say:**

> I am switching back to the demo-participant role. The participant can use the
> private connection-management page to stop future collection and revoke the active
> connection. The same page presents the approved
> deletion wording and records a deletion request for study-team handling. The
> treatment of already collected research data follows the exact ethics-approved
> consent. Disconnect removes the stored provider tokens and prevents future
> collection.

### 11:35–12:05 — Close the demonstration

**Show and do:** Return to the public homepage or Privacy Policy and keep the PI
contact information visible.

**Say:**

> This concludes the end-to-end AdmonTracker authorization and data-use flow for all
> four requested Google Health scopes. The account and wearable data shown were used
> only for this controlled test demonstration and did not belong to an enrolled
> participant. Questions about participation, privacy,
> withdrawal, or deletion can be sent to Principal Investigator Professor Roee
> Admon at radmon@psy.haifa.ac.il. Thank you for reviewing AdmonTracker.

## Final video quality check

Before submitting the link, watch the entire uploaded video while signed out of the
video host and confirm:

- the link opens without requesting access;
- the app name is AdmonTracker everywhere;
- the production domain and public legal pages are visible;
- the Google consent screen is fully visible in English;
- all four requested permissions are readable;
- a working feature is shown for activity/fitness, health metrics, sleep, and
  settings/device details;
- no secret, password, real participant data, raw authorization URL, or raw
  management URL appears in any frame; and
- the opening narration clearly identifies the dedicated demo account, your six
  personally operated test watches, and your two demonstration roles; and
- narration matches the submitted scope justification and the deployed Privacy
  Policy.

Add timestamps for the four scope demonstrations to the verification submission or
video description: steps, heart rate, sleep, and device details.

## Submission sequence

1. Upload the final English video as an unlisted link accessible without login or an
   access request.
2. Use the text in `google_data_access_justification.txt` for the Google Auth
   Platform scope explanation, provided it still matches the recorded build.
3. In Google Auth Platform, confirm the app is **In production**, then select
   **Prepare for verification** and supply the public URLs, four scopes,
   justification, and video link.
4. Submit the Google Health research intake/evidence requested by Google, including
   the study approval, approved addenda, data-flow/security description, and
   retention/deletion procedure.
5. Keep the recorded production UI, branding, callback URLs, and scope list stable
   while Google reviews the application. Monitor the project contact email and
   respond to requests for clarification or a security assessment.

## Official references

- Google OAuth demo-video requirements:
  https://support.google.com/cloud/answer/13804565?hl=en
- Google Auth Platform submission process:
  https://support.google.com/cloud/answer/13461325?hl=en-GB
- OAuth verification requirements:
  https://support.google.com/cloud/answer/13464321?hl=en
- Sensitive-scope verification:
  https://developers.google.com/identity/protocols/oauth2/production-readiness/sensitive-scope-verification
- Google Health research policy:
  https://developers.google.com/health/policies/health-api-user-data-and-research-policy
- Google Health paired-device endpoint and settings scope:
  https://developers.google.com/health/reference/rest/v4/users.pairedDevices/list
