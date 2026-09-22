import streamlit as st

from utils.branding import render_app_logo
from utils.compliance import (
    PI_EMAIL,
    PI_NAME,
    PI_PHONE,
    PUBLIC_ETHICS_URL,
    PUBLIC_HOME_URL,
    PUBLIC_TERMS_URL,
)


st.set_page_config(page_title="Privacy Policy - AdmonTracker", page_icon="🔒", layout="wide")
render_app_logo(show_navigation=False)

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)

if language == "English":
    st.title("Privacy Policy")
    st.caption("AdmonTracker • Last updated 22 September 2026")
    st.markdown(
        f"""
## 1. Operator, study, and contact

AdmonTracker is operated for University of Haifa Study 385/23 by **{PI_NAME}'s
Stress & Psychopathology Lab, School of Psychological Sciences, University of
Haifa**. The study examines the effect of sleep patterns on the association between
childhood experiences and emotional responses to different situations. Questions
about privacy, participation, withdrawal, or deletion may be sent to **{PI_EMAIL}**
or **{PI_PHONE}**. Include only the study or watch code; do not send passwords,
access tokens, or health measurements by email.

## 2. Who and what this policy covers

This policy covers invited adult research participants, authorized University
research personnel, and visitors to the synthetic guest demonstration. Participation
is voluntary. The ethics-approved consent and Google Health addendum apply together
with this policy. The guest demonstration contains fictional examples only and does
not access production participant data or external research services.

## 3. Information accessed and collected

For staff authentication, AdmonTracker processes the Google OpenID account
identifier, name, email address, authentication status, assigned role, project, and
session information.

For Google Health participants, the connection uses only the **Google account
designated for the study (the demo account)**, not a personal Google account. After
affirmative authorization, AdmonTracker requests read-only access to **heart rate,
steps, sleep data, physical activity, and respiratory rate**, depending on device
availability and the permissions approved by the participant. It cannot change or
delete information in the demo account. Fitbit data is collected only where the
applicable approved participant documents authorize it.

For operations and quality control, the study also collects the assigned watch's
model, battery level and status, and last synchronization time to identify charging
or synchronization problems and check data completeness.

AdmonTracker also processes pseudonymous study and watch codes, provider and scope
selection, authorization status, token expiry, protected OAuth tokens, collection
and audit logs, device status, support records, and ordinary website security data
such as timestamps, browser information, session cookies, IP address, and diagnostic
logs.

## 4. Purpose and prohibited uses

Google Health information is used only for Study 385/23 and the quality checks
needed to conduct it. It is not used for diagnosis, treatment, medical decisions,
or emergency monitoring, and will not be used for a new study or another purpose
without separate consent unless the Ethics Committee approves an exception.

AdmonTracker does not sell the information or use it for advertising, marketing,
credit, employment, insurance, training general artificial-intelligence models, or
any purpose unrelated to this study. Published research results are summaries that
do not contain names or information that identifies an individual participant.

## 5. Google API Limited Use

AdmonTracker's use and transfer of information received from Google APIs adheres to
the [Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
including the Limited Use requirements. Use of Google Health information adheres to
the [Google Health API Developer and User Data Policy](https://developers.google.com/health/policies/health-api-developer-user-data-policy)
and the [Google Health API User Data and Health Research Policy](https://developers.google.com/health/policies/health-api-user-data-and-research-policy),
including the Limited Use requirements.

## 6. Storage, security, access, and sharing

Google Health data is stored under a study code without a name or direct identifier.
The link between the code and identity is stored separately in a protected University
of Haifa system. While that link exists, the data is coded and not fully anonymous.
Only authorized University of Haifa research-team members may access research data.
Access to the identity link is limited to the Principal Investigator, laboratory
head, and authorized programmer.

Secure University, Google, and Heroku services provide the technical infrastructure.
Operational metadata is held in Google Cloud Firestore with an access-controlled
Google Sheets recovery copy. Wearable archives are held in a restricted University
Google Workspace Shared Drive, and
OAuth secrets and tokens are held in Google Secret Manager. Data is transferred over
encrypted connections and protected by role-based access controls, restricted
service accounts, secret management, OAuth expiry and replay controls, and audit
records.

Information about an individual participant is not shared outside the research team
without new explicit consent and any required approvals. Technical providers process
data only to operate the approved service. Information may be disclosed where the
law requires it or to address a security incident.

## 7. Collection period and retention

Google Health data is collected for **one month after the connection is activated**.
Collection stops when that month ends, on disconnection, or on withdrawal, whichever
occurs first. Measurement data is retained during that collection month and for one
additional month for processing and quality checks. No later than one month after
measurement ends, Google Health data and the identity link are permanently deleted.
The application authorization is canceled at collection end, disconnection, or
withdrawal, whichever occurs first. Signed consent and administrative records that
do not contain measurements are retained according to University and research
requirements.

## 8. Withdrawal, disconnection, and deletion

Participants may refuse, disconnect, or withdraw at any time without penalty or loss
of benefits. Contact **{PI_EMAIL}** or **{PI_PHONE}** and provide the study or watch
code. On withdrawal, collection stops immediately and the application authorization
in the assigned demo account is canceled. All collected data is permanently deleted
unless the participant gives separate, explicit consent at that time to keep it.
Summary results already published cannot be removed from an existing publication.

## 9. Changes

This policy is updated when the service, approved protocol, providers, or applicable
requirements change. Material changes affecting participation follow the applicable
ethics-review and participant-consent process.
"""
    )
else:
    st.title("מדיניות פרטיות")
    st.caption("AdmonTracker • עודכן לאחרונה: 22 בספטמבר 2026")
    st.markdown(
        f"""
## 1. מפעיל השירות, המחקר ויצירת קשר

AdmonTracker מופעלת עבור מחקר 385/23 של אוניברסיטת חיפה על-ידי המעבדה ללחץ
ופסיכופתולוגיה של **פרופ׳ רועי אדמון, בית הספר למדעי הפסיכולוגיה, אוניברסיטת
חיפה**. המחקר בוחן את השפעת דפוסי השינה על הקשר בין חוויות ילדות לבין תגובות
רגשיות למצבים שונים. לפניות בנושא פרטיות, השתתפות, פרישה או מחיקה: **{PI_EMAIL}**
או **{PI_PHONE}**. יש לציין רק את קוד המחקר או השעון ואין לשלוח סיסמה, אסימון גישה
או נתוני בריאות בדוא״ל.

## 2. על מי ועל מה חלה המדיניות

המדיניות חלה על משתתפים בגירים שהוזמנו למחקר, אנשי אוניברסיטה מורשים ומבקרי
סביבת ההדגמה הסינתטית. ההשתתפות התנדבותית. טופס ההסכמה והנספח ל-Google Health
שאושרו על-ידי ועדת האתיקה חלים יחד עם מדיניות זו.

## 3. מידע שנקרא ונאסף

לצורך כניסת אנשי צוות, המערכת מעבדת מזהה OpenID של Google, שם, כתובת דוא״ל,
מצב אימות, תפקיד, פרויקט ופרטי הפעלה.

עבור משתתפי Google Health החיבור נעשה רק באמצעות **חשבון Google ייעודי למחקר
(חשבון הדמו)** ולא באמצעות חשבון אישי. לאחר הרשאה מפורשת AdmonTracker מבקשת
גישת קריאה בלבד ל-**דופק, צעדים, נתוני שינה, פעילות גופנית וקצב נשימה**, בהתאם
לזמינות במכשיר ולהרשאות שאושרו. המערכת אינה יכולה לשנות או למחוק מידע בחשבון
הדמו. נתוני Fitbit נאספים רק כאשר מסמכי המשתתף המאושרים החלים מתירים זאת.

לצורכי תפעול ובקרת איכות נאספים גם דגם השעון שהוקצה למחקר, מצב ורמת הסוללה ומועד
הסנכרון האחרון, לצורך איתור בעיות טעינה או סנכרון ובדיקת שלמות איסוף הנתונים.

המערכת מעבדת גם קוד מחקר ושעון, ספק והרשאות שנבחרו, מצב החיבור, תפוגת אסימונים,
אסימוני OAuth מוגנים, לוגים של איסוף וביקורת, מצב מכשיר, רישומי תמיכה ונתוני אבטחת
אתר רגילים כגון חותמות זמן, פרטי דפדפן, עוגיות הפעלה, כתובת IP ולוגים טכניים.

## 4. מטרות ושימושים אסורים

מידע מ-Google Health משמש רק למחקר 385/23 ולבדיקות האיכות הנחוצות לביצועו.
הוא אינו משמש לאבחון, טיפול, החלטות רפואיות או מעקב חירום. הוא לא ישמש למחקר
חדש או למטרה אחרת ללא הסכמה נפרדת, אלא אם ועדת האתיקה תאשר חריג.

המידע לא יימכר ולא ישמש לפרסום, שיווק, החלטות אשראי, תעסוקה או ביטוח, לאימון
מודלים כלליים של בינה מלאכותית או לכל מטרה שאינה קשורה למחקר. תוצאות יפורסמו
בצורה מסכמת בלבד, ללא שם או מידע שמזהה משתתף מסוים.

## 5. כללי השימוש המוגבל של Google

השימוש והעברת המידע שהתקבל מ-Google APIs עומדים
[במדיניות נתוני המשתמש של Google API Services](https://developers.google.com/terms/api-services-user-data-policy),
לרבות דרישות Limited Use. השימוש בנתוני Google Health עומד גם
[במדיניות המפתחים ונתוני המשתמש של Google Health API](https://developers.google.com/health/policies/health-api-developer-user-data-policy)
וב-[מדיניות Google Health לנתוני משתמש ולמחקר בריאות](https://developers.google.com/health/policies/health-api-user-data-and-research-policy),
לרבות דרישות Limited Use.

## 6. אחסון, אבטחה, גישה ושיתוף

נתוני Google Health נשמרים תחת קוד מחקר, ללא שם או מזהה ישיר. הקישור בין הקוד
לזהות נשמר בנפרד במערכת מוגנת של אוניברסיטת חיפה. כל עוד הקישור קיים, המידע
מקודד ואינו אנונימי לחלוטין. רק חברי צוות מורשים באוניברסיטת חיפה רשאים לגשת
לנתוני המחקר. הגישה לקישור לזהות מוגבלת לחוקר הראשי, לראש המעבדה ולמתכנת המורשה.

שירותים מאובטחים של האוניברסיטה, Google ו-Heroku מספקים את התשתית הטכנית.
מטא-נתונים תפעוליים נשמרים ב-Google Cloud Firestore עם עותק התאוששות מוגן
ב-Google Sheets. ארכיוני מדדים נשמרים ב-Shared Drive אוניברסיטאי מוגבל,
וסודות ואסימוני OAuth ב-Google Secret Manager.
המידע מועבר בחיבור מוצפן ומוגן בבקרת גישה לפי תפקיד, חשבונות שירות מוגבלים,
ניהול סודות, תפוגה ומניעת שימוש חוזר ב-OAuth ורישומי ביקורת.

מידע על משתתף מסוים לא יועבר מחוץ לצוות המחקר ללא הסכמה מפורשת חדשה והאישורים
הנדרשים. ספקי התשתית מעבדים מידע רק לצורך הפעלת השירות המאושר. מידע עשוי להימסר
כאשר הדין מחייב או לצורך טיפול באירוע אבטחה.

## 7. משך האיסוף והשמירה

נתוני Google Health נאספים במשך **חודש אחד ממועד הפעלת החיבור**. האיסוף נפסק
בתום החודש, בעת ניתוק או בעת פרישה, לפי המועד המוקדם. נתוני המדידה נשמרים במהלך
חודש האיסוף ולמשך חודש נוסף לצורך עיבוד ובדיקת איכות. לא יאוחר מחודש לאחר סיום
המדידה, נתוני Google Health והקישור לזהות נמחקים לצמיתות. הרשאת האפליקציה מבוטלת
בתום האיסוף, בעת ניתוק או בעת פרישה, לפי המועד המוקדם. טופס ההסכמה ורישומים
מנהליים שאינם כוללים את המדידות נשמרים לפי כללי האוניברסיטה והמחקר.

## 8. פרישה, ניתוק ומחיקה

ניתן לסרב, לנתק או לפרוש בכל עת ללא קנס או אובדן זכויות. יש לפנות ל-**{PI_EMAIL}**
או **{PI_PHONE}** ולציין את קוד המחקר או השעון. בעת פרישה האיסוף נפסק מיד והרשאת
האפליקציה בחשבון הדמו מבוטלת. כל הנתונים שנאספו נמחקים לצמיתות, אלא אם המשתתף/ת
נותן/ת באותו מועד הסכמה נפרדת ומפורשת לשמור אותם. לא ניתן להסיר מפרסום קיים
תוצאות מסכמות שכבר פורסמו.

## 9. שינויים

המדיניות תעודכן עם שינוי בשירות, בפרוטוקול המאושר, בספקים או בדרישות. שינויים
מהותיים להשתתפות יטופלו בהליך האתיקה והסכמת המשתתפים המתאים.
"""
    )

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link(PUBLIC_HOME_URL, label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link(PUBLIC_TERMS_URL, label="Terms / תנאי שימוש", icon="📄")
with cols[2]:
    st.page_link(PUBLIC_ETHICS_URL, label="Ethics / אתיקה", icon="🏛️")
