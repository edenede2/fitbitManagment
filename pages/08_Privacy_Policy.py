import streamlit as st

from utils.compliance import PI_EMAIL, PI_NAME
from utils.branding import render_app_logo


st.set_page_config(
    page_title="Privacy Policy - AdmonTracker",
    page_icon="🔒",
    layout="wide",
)
render_app_logo()

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)

if language == "English":
    st.title("Privacy Policy")
    st.caption("AdmonTracker • Last updated 17 September 2026")
    st.markdown(
        f"""
## 1. Operator and contact

AdmonTracker is operated for research by **{PI_NAME}'s Stress &
Psychopathology Lab, School of Psychological Sciences, University of Haifa**.
The public contact for privacy, participation, withdrawal, and deletion requests is
**{PI_EMAIL}**. Please identify the study and your pseudonymous study code, but do
not email account tokens or health measurements.

## 2. People covered

This policy covers invited adult research participants, authorized University
research personnel, and visitors to the synthetic guest demonstration. Participation
is voluntary. Study-specific ethics approval and informed consent control where they
provide more specific protections.

## 3. Information and sources

Depending on the approved protocol, we process Google-authenticated staff name,
email, role, project and session information; pseudonymous study and watch IDs;
OAuth connection status and protected tokens; device status and logs; approved
questionnaire responses; and authorized wearable measurements.

Wearable measurements can include **steps and activity, calories, heart rate,
heart-rate variability, skin temperature, respiratory rate, sleep, and associated
timestamps**. They come from Fitbit or Google Health only after authorization.
Google Health access is limited to read-only activity/fitness, health
measurements, and sleep scopes. The authorization screen identifies the actual
scopes requested.

The guest demonstration contains fictional examples only. It does not access
production Sheets, participant records, watches, Google Health, Fitbit, email, or
other external services. Hosting infrastructure can still process ordinary security
and delivery data such as IP address, browser details, session cookies, timestamps,
and diagnostic logs.

## 4. Purposes and prohibited uses

We use information only to operate ethics-approved research, connect accounts,
collect approved measurements, monitor completeness and device status, notify the
study team of operational problems, support participants, secure the service, and
produce research outputs permitted by the approved protocol.

We do **not** sell participant or Google user data, use it for advertising or data
brokerage, make credit, insurance, employment, or lending decisions from it, use it
for unrelated marketing, or use it to train general-purpose AI/ML models.

## 5. Google API Limited Use

AdmonTracker's use and transfer to any other app of information
received from Google APIs adheres to the
[Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
including its Limited Use requirements. Use of information received from the
Google Health API adheres to the
[Google Health API Developer and User Data Policy](https://developers.google.com/health/policies/health-api-developer-user-data-policy),
including its Limited Use requirements, and also follows the
[Google Health API User Data and Health Research Policy](https://developers.google.com/health/policies/health-api-user-data-and-research-policy).
Human access is limited to authorized people with a genuine research, security,
support, or legal need consistent with the approved consent.

## 6. Storage, processors, and sharing

The service runs at **https://app.admontracker.online** on Heroku. Operational and
research metadata is stored in access-controlled Google Sheets. Raw wearable
archives are stored in a restricted Google Workspace Shared Drive. OAuth client
secrets and participant access/refresh tokens are stored in Google Secret Manager.
Temporary processing files on Heroku are deleted after upload. Approved AppSheet,
Qualtrics, email, Fitbit, Google Sign-In, Google Cloud, Google Workspace, and Google
Health services may process data when needed for the study.

Data is available only to the relevant study team, authorized University personnel,
approved processors, and collaborators whose access is permitted by consent,
ethics approval, contract, platform policy, and law. Providers may process data
outside Israel under applicable safeguards. We may disclose information when law
requires it or to investigate a security incident.

## 7. Security

We use HTTPS, access controls by role and project, least-privilege service accounts,
restricted Shared Drive membership, protected secrets, encryption offered by the
hosting and cloud providers, OAuth state expiry and replay controls, audit records,
and isolated synthetic guest data. No system can be guaranteed completely secure.

## 8. Retention, withdrawal, and deletion

The retention period and treatment of previously collected or de-identified
research data are stated in the current ethics-approved participant documents
presented during authorization.

Participants may withdraw and stop future collection at any time without penalty by
contacting **{PI_EMAIL}** or using the connection-management instructions supplied
after authorization. Disconnecting revokes access and removes active tokens.
Requests concerning already collected data are handled under the approved consent,
ethics requirements, scientific-integrity duties, and applicable law. Google or
Fitbit access can also be revoked in the participant's provider account settings.

## 9. Changes

We update this notice when the service, approved protocol, processors, or applicable
requirements change. Material changes affecting participation are referred for
ethics review and renewed disclosure/consent when required.
"""
    )
else:
    st.title("מדיניות פרטיות")
    st.caption("AdmonTracker • עודכן לאחרונה: 17 בספטמבר 2026")
    st.markdown(
        f"""
## 1. מפעיל השירות ויצירת קשר

השירות מופעל לצורכי מחקר על-ידי המעבדה ללחץ ופסיכופתולוגיה של **פרופ׳ רועי אדמון,
בית הספר למדעי הפסיכולוגיה, אוניברסיטת חיפה**. לפניות בנושאי פרטיות, השתתפות,
פרישה או מחיקה: **{PI_EMAIL}**. יש לציין את מספר המחקר ואת קוד המחקר הבדוי,
אך אין לשלוח בדוא״ל אסימוני גישה או מדדי בריאות.

## 2. על מי חלה המדיניות

המדיניות חלה על משתתפים בגירים שהוזמנו למחקר, אנשי צוות מורשים ומבקרי סביבת
ההדגמה הסינתטית. ההשתתפות התנדבותית. במקרה שמסמכי המחקר המאושרים מעניקים
הגנה מפורטת יותר, הם הקובעים.

## 3. מידע ומקורות

בהתאם לפרוטוקול המאושר אנו עשויים לעבד פרטי חשבון של אנשי צוות, תפקיד ופרויקט;
מזהי מחקר ושעון בדויים; מצב חיבור ואסימוני OAuth מוגנים; נתוני תפעול ולוגים;
תשובות לשאלונים מאושרים; ומדדי לביש שאושרו.

המדדים עשויים לכלול **צעדים ופעילות, קלוריות, דופק, שונות קצב לב, טמפרטורת עור,
קצב נשימה, שינה וחותמות זמן**. מקורם ב-Fitbit או Google Health לאחר הרשאה בלבד.
הגישה ל-Google Health מוגבלת להרשאות קריאה בלבד של פעילות וכושר, מדדי בריאות
ושינה. מסך ההרשאה מציג את ההרשאות המבוקשות בפועל.

סביבת ההדגמה כוללת נתונים בדויים בלבד ואינה ניגשת למערכות הייצור. תשתית האירוח
עשויה לעבד נתונים טכניים רגילים הדרושים לאבטחה ולאספקת האתר.

## 4. מטרות ושימושים אסורים

המידע משמש אך ורק להפעלת מחקר שאושר אתית, חיבור חשבונות, איסוף המדדים שאושרו,
בקרת שלמות ותקינות, תמיכה ואבטחה, והפקת תוצרי מחקר שהפרוטוקול מתיר.

איננו מוכרים מידע; איננו משתמשים בו לפרסום, תיווך נתונים, החלטות אשראי, ביטוח
או תעסוקה, שיווק שאינו קשור למחקר, או אימון מודלי AI/ML כלליים.

## 5. כללי השימוש המוגבל של Google

השימוש והעברת המידע שהתקבל מ-Google APIs עומדים
[במדיניות נתוני המשתמש של Google API Services](https://developers.google.com/terms/api-services-user-data-policy),
לרבות דרישות Limited Use. השימוש במידע שהתקבל מ-Google Health API עומד גם
[במדיניות המפתחים ונתוני המשתמש של Google Health API](https://developers.google.com/health/policies/health-api-developer-user-data-policy),
לרבות דרישות Limited Use, וכן
[במדיניות Google Health למחקר ולנתוני משתמש](https://developers.google.com/health/policies/health-api-user-data-and-research-policy).
גישה אנושית מוגבלת לבעלי צורך מחקרי, אבטחתי, תמיכתי או משפטי אמיתי ובהתאם להסכמה.

## 6. אחסון, ספקים ושיתוף

השירות פועל ב-**https://app.admontracker.online** על Heroku. מטא-נתונים נשמרים
ב-Google Sheets מוגנים; ארכיוני מדדים גולמיים נשמרים ב-Shared Drive מוגבל;
וסודות OAuth ואסימוני גישה נשמרים ב-Google Secret Manager. קבצים זמניים ב-Heroku
נמחקים לאחר העלאה. שירותים מאושרים של AppSheet, Qualtrics, דוא״ל, Fitbit ו-Google
עשויים לעבד מידע ככל שנדרש למחקר.

הגישה ניתנת רק לצוות המחקר הרלוונטי, גורמי אוניברסיטה מורשים, מעבדים מאושרים
ושותפים שהגישה אליהם מותרת לפי ההסכמה, אישור האתיקה, ההסכם והדין.

## 7. אבטחה

אנו משתמשים ב-HTTPS, בקרת גישה לפי תפקיד ופרויקט, חשבונות שירות בהרשאה מזערית,
חברות מוגבלת ב-Shared Drive, ניהול סודות, הצפנה המסופקת על-ידי שירותי הענן,
תפוגה ומניעת שימוש חוזר ב-OAuth state, רישומי ביקורת והפרדת נתוני הדגמה. אין
מערכת שניתן להבטיח שהיא מאובטחת לחלוטין.

## 8. שמירה, פרישה ומחיקה

משך השמירה והטיפול במידע שכבר נאסף או עבר ביטול זיהוי מפורטים במסמכי המשתתף
שאושרו אתית ומוצגים במהלך ההרשאה.

ניתן לפרוש ולהפסיק איסוף עתידי בכל עת וללא קנס באמצעות פנייה ל-**{PI_EMAIL}**
או הוראות ניהול החיבור הנמסרות לאחר ההרשאה. ניתוק מבטל גישה ומסיר אסימונים פעילים.
בקשות לגבי מידע שכבר נאסף מטופלות לפי ההסכמה המאושרת, דרישות האתיקה, שלמות
מדעית והדין. ניתן גם לבטל גישה בהגדרות חשבון Google או Fitbit.

## 9. שינויים

נעדכן הודעה זו עם שינוי בשירות, בפרוטוקול, בספקים או בדרישות. שינוי מהותי
להשתתפות יועבר לבחינת ועדת האתיקה ולהסכמה מחודשת כאשר הדבר נדרש.
"""
    )

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link("app.py", label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link("pages/09_Terms_of_Service.py", label="Terms / תנאי שימוש", icon="📄")
with cols[2]:
    st.page_link("pages/10_Research_Ethics.py", label="Ethics / אתיקה", icon="🏛️")
