import streamlit as st

from utils.compliance import PI_EMAIL
from utils.branding import render_app_logo


st.set_page_config(
    page_title="Terms of Use - Wearable Research Manager",
    page_icon="📄",
    layout="wide",
)
render_app_logo()

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)

if language == "English":
    st.title("Terms of Use")
    st.caption("Wearable Research Manager • Last updated 14 September 2026")
    st.markdown(
        f"""
## Research service and eligibility

Wearable Research Manager is operated by the Stress & Psychopathology Lab,
School of Psychological Sciences, University of Haifa. Authenticated functions are
for invited research personnel. Participant authorization is for **adults aged 18
or older** who were invited to an approved study and completed the required consent.
Participation is voluntary and may be stopped without penalty as described in the
approved consent.

## Authorized use

Staff may use the service only within their assigned role and approved project.
Users must protect credentials, access only assigned records, follow the protocol,
ethics approval, University rules and law, and promptly report suspected disclosure
or compromise. Participant links and credentials must not be shared or reused.

You must not bypass access controls; re-identify participants without approval;
scrape, overload, reverse engineer or disrupt the service; falsify data or audit
records; introduce malware; disclose data through an unapproved channel; sell data;
or use it for advertising, credit, employment, insurance, unrelated marketing, or
unrelated model training.

## Guest demonstration

The guest demonstration is read-only and contains fictional examples. Do not enter
real personal, study, device, or health information or attempt to connect an account.

## Connected services

The service can use Google Sign-In, Google Cloud and Workspace, Google Health,
Fitbit, Heroku, AppSheet, Qualtrics, and approved email infrastructure. Their
availability and separate terms may apply. Use must comply with the
[Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy),
[Google OAuth policies](https://developers.google.com/identity/protocols/oauth2/policies),
[Google Health policies](https://developers.google.com/health/policies/health-api-user-data-and-research-policy),
and [Fitbit Platform Terms](https://dev.fitbit.com/legal/platform-terms-of-service/).

## Not medical care or emergency monitoring

This is a research and operations service. It is **not medical advice, not a medical device,
diagnosis, treatment, or emergency monitoring**. Measurements, sync status,
and alerts can be delayed, incomplete, inaccurate, or unavailable. In an emergency,
contact the appropriate emergency service or healthcare professional.

## Availability, intellectual property, and termination

The service may be changed, restricted, suspended, or discontinued for research,
security, ethics, legal, or provider reasons. Software and original content belong
to or are licensed to their respective owners. Authorized access is limited,
revocable, and non-transferable. Access may be terminated after a role or project
ends, consent or provider access is withdrawn, or a security or policy issue occurs.

To the maximum extent permitted by law, the service is provided “as is” and “as
available,” without a promise of uninterrupted availability or error-free device
data. Nothing excludes rights or liabilities that cannot legally be excluded.

## Privacy, governing law, and contact

The public Privacy Policy and the study-specific approved consent explain data use
and form part of these terms. These terms are governed by the laws of the State of Israel
without limiting mandatory participant rights or available forums.

Questions may be sent to **{PI_EMAIL}**. Do not send health data or access tokens
in unencrypted email.
"""
    )
else:
    st.title("תנאי שימוש")
    st.caption("Wearable Research Manager • עודכן לאחרונה: 14 בספטמבר 2026")
    st.markdown(
        f"""
## שירות מחקר וזכאות

השירות מופעל על-ידי המעבדה ללחץ ופסיכופתולוגיה, בית הספר למדעי הפסיכולוגיה,
אוניברסיטת חיפה. תפקודי הצוות מיועדים למשתמשים שהוזמנו ואושרו. הרשאת משתתף
מיועדת **לבגירים בני 18 ומעלה** שהוזמנו למחקר מאושר והשלימו את ההסכמה הנדרשת.
ההשתתפות התנדבותית וניתן להפסיקה ללא קנס בהתאם להסכמה המאושרת.

## שימוש מורשה

אנשי צוות רשאים להשתמש בשירות רק במסגרת תפקידם והפרויקט שהוקצה להם. יש להגן
על פרטי הגישה, לגשת רק לרשומות מורשות, לפעול לפי הפרוטוקול, אישור האתיקה,
נהלי האוניברסיטה והדין, ולדווח מיד על חשד לחשיפה. אין לשתף או להשתמש מחדש
בקישורי משתתפים או בפרטי גישה.

אין לעקוף בקרות גישה; לזהות מחדש משתתפים ללא אישור; לגרד, להעמיס או לשבש את
השירות; לזייף נתונים או רישומי ביקורת; לחשוף מידע בערוץ לא מאושר; למכור מידע;
או להשתמש בו לפרסום, אשראי, תעסוקה, ביטוח, שיווק לא קשור או אימון מודלים לא קשור.

## סביבת הדגמה

ההדגמה לקריאה בלבד וכוללת דוגמאות בדויות. אין להזין בה מידע אישי, מחקרי,
בריאותי או מידע על מכשיר אמיתי, ואין לנסות לחבר אליה חשבון.

## שירותים מחוברים

השירות עשוי להשתמש ב-Google Sign-In, Google Cloud ו-Workspace, Google Health,
Fitbit, Heroku, AppSheet, Qualtrics ותשתית דוא״ל מאושרת. חלים גם התנאים הנפרדים
של ספקים אלה וכללי Google API, OAuth, Google Health ו-Fitbit.

## לא טיפול רפואי או ניטור חירום

זהו כלי מחקר ותפעול. הוא **אינו ייעוץ רפואי, מכשיר רפואי, אבחון, טיפול או מערכת
ניטור חירום**. מדדים, מצב סנכרון והתראות עלולים להיות מאוחרים, חלקיים או שגויים.
במקרה חירום יש לפנות לשירותי החירום או לאיש מקצוע רפואי.

## זמינות, זכויות וסיום גישה

ניתן לשנות, להגביל, להשעות או להפסיק את השירות מטעמי מחקר, אבטחה, אתיקה,
דין או ספק. הגישה מוגבלת, ניתנת לביטול ואינה ניתנת להעברה. ניתן לסיימה עם
סיום תפקיד או פרויקט, משיכת הסכמה או הרשאת ספק, או אירוע אבטחה או מדיניות.
השירות ניתן, במידה שהדין מתיר, כפי שהוא וכפי שהוא זמין, ללא הבטחת זמינות רצופה.

## פרטיות, דין ויצירת קשר

מדיניות הפרטיות ומסמכי ההסכמה המאושרים מסבירים את השימוש במידע ומהווים חלק
מתנאים אלה. הדין החל הוא דין מדינת ישראל, בלי לגרוע מזכויות חובה או פורומים
העומדים למשתתפים. לשאלות: **{PI_EMAIL}**. אין לשלוח מידע רפואי או אסימוני גישה
בדוא״ל שאינו מוצפן.
"""
    )

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link("app.py", label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy / פרטיות", icon="🔒")
with cols[2]:
    st.page_link("pages/10_Research_Ethics.py", label="Ethics / אתיקה", icon="🏛️")
