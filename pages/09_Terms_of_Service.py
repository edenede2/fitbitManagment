import streamlit as st

from utils.branding import render_app_logo
from utils.compliance import (
    PI_EMAIL,
    PUBLIC_ETHICS_URL,
    PUBLIC_HOME_URL,
    PUBLIC_PRIVACY_URL,
)


st.set_page_config(page_title="Terms of Use - AdmonTracker", page_icon="📄", layout="wide")
render_app_logo()

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)

if language == "English":
    st.title("Terms of Use")
    st.caption("AdmonTracker • Last updated 21 September 2026")
    st.markdown(
        f"""
## Research service and eligibility

AdmonTracker is operated by the Stress & Psychopathology Lab, School of
Psychological Sciences, University of Haifa. Staff functions are for invited,
authorized research personnel. Participant authorization is for adults aged 18 or
older who are eligible for Study 385/23 and completed the approved consent process.
Participation and provider authorization are voluntary and may be stopped without
penalty as described in the approved documents.

## Authorized use

Staff may use the service only within their assigned role and approved project.
Users must protect credentials, access only assigned records, follow the protocol,
ethics approval, University rules, and law, and report suspected disclosure or
compromise promptly. Participant links and credentials must not be shared or reused.

Users must not bypass access controls, re-identify participants without approval,
disrupt the service, falsify data or audit records, disclose information through an
unapproved channel, sell data, or use data for advertising, credit, employment,
insurance, unrelated marketing, or unrelated model training.

## Participant accounts and authorization

Google Health participants must use only the Google account designated for the study
(the demo account), not a personal Google account. No user should give the research
team a personal Google password or sign-in details. Authorization is read-only and
limited to the data and period described in the approved participant documents.

## Connected services

AdmonTracker uses Google Sign-In, Google Cloud and Workspace, Google Health, Fitbit,
Heroku, and approved University infrastructure. Provider availability and separate
terms may apply. Use must comply with the Google API Services User Data Policy,
Google OAuth policies, Google Health policies, and Fitbit Platform Terms.

## Not medical care or emergency monitoring

AdmonTracker is a research and operations service. It is not medical advice, a
medical device, diagnosis, treatment, or emergency monitoring. Measurements and
sync status may be delayed, incomplete, inaccurate, or unavailable. In an emergency,
contact the appropriate emergency service or healthcare professional.

## Availability and termination

The service may be changed, restricted, suspended, or discontinued for research,
security, ethics, legal, or provider reasons. Authorized access is limited,
revocable, and non-transferable. Access may end when a role or project ends, when
consent or provider access is withdrawn, or after a security or policy issue.

## Privacy, governing law, and contact

The public Privacy Policy and approved participant documents explain data use and
form part of these terms. These terms are governed by the laws of the State of Israel
without limiting mandatory participant rights or available forums. Questions may be
sent to **{PI_EMAIL}**. Do not send health data, passwords, or access tokens by email.
"""
    )
else:
    st.title("תנאי שימוש")
    st.caption("AdmonTracker • עודכן לאחרונה: 21 בספטמבר 2026")
    st.markdown(
        f"""
## שירות מחקר וזכאות

AdmonTracker מופעלת על-ידי המעבדה ללחץ ופסיכופתולוגיה, בית הספר למדעי
הפסיכולוגיה, אוניברסיטת חיפה. תפקודי הצוות מיועדים לאנשי מחקר שהוזמנו ואושרו.
הרשאת משתתף מיועדת לבגירים בני 18 ומעלה שנמצאו מתאימים למחקר 385/23 והשלימו
את הליך ההסכמה המאושר. ההשתתפות והרשאת הספק הן מרצון וניתן להפסיקן ללא קנס
בהתאם למסמכים המאושרים.

## שימוש מורשה

אנשי צוות רשאים להשתמש בשירות רק במסגרת התפקיד והפרויקט שהוקצו להם. יש להגן
על פרטי הגישה, לגשת רק לרשומות מורשות, לפעול לפי הפרוטוקול, אישור האתיקה,
נהלי האוניברסיטה והדין, ולדווח מיד על חשד לחשיפה. אין לשתף או לעשות שימוש חוזר
בקישורי משתתפים או בפרטי גישה.

אין לעקוף בקרות גישה, לזהות מחדש משתתפים ללא אישור, לשבש את השירות, לזייף מידע
או רישומי ביקורת, לחשוף מידע בערוץ לא מאושר, למכור מידע או להשתמש בו לפרסום,
אשראי, תעסוקה, ביטוח, שיווק לא קשור או אימון מודלים לא קשור.

## חשבונות משתתפים והרשאה

משתתפי Google Health חייבים להשתמש רק בחשבון Google הייעודי למחקר (חשבון הדמו)
ולא בחשבון אישי. אין למסור לצוות המחקר סיסמה או פרטי כניסה לחשבון Google אישי.
ההרשאה היא לקריאה בלבד ומוגבלת למידע ולתקופה שבמסמכי המשתתף המאושרים.

## שירותים מחוברים

AdmonTracker משתמשת ב-Google Sign-In, Google Cloud ו-Workspace, Google Health,
Fitbit, Heroku ותשתיות אוניברסיטאיות מאושרות. זמינות הספקים ותנאיהם הנפרדים עשויים
לחול. השימוש כפוף למדיניות Google API Services, OAuth, Google Health ו-Fitbit.

## לא טיפול רפואי או ניטור חירום

זהו שירות מחקר ותפעול. הוא אינו ייעוץ רפואי, מכשיר רפואי, אבחון, טיפול או מערכת
ניטור חירום. מדדים ומצב סנכרון עלולים להיות מאוחרים, חלקיים, שגויים או בלתי זמינים.
במקרה חירום יש לפנות לשירותי החירום או לאיש מקצוע רפואי.

## זמינות וסיום גישה

ניתן לשנות, להגביל, להשעות או להפסיק את השירות מטעמי מחקר, אבטחה, אתיקה, דין
או ספק. הגישה מוגבלת, ניתנת לביטול ואינה ניתנת להעברה. היא עשויה להסתיים עם
סיום תפקיד או פרויקט, משיכת הסכמה או הרשאת ספק, או בעקבות אירוע אבטחה או מדיניות.

## פרטיות, דין ויצירת קשר

מדיניות הפרטיות ומסמכי המשתתף המאושרים מסבירים את השימוש במידע ומהווים חלק
מתנאים אלה. הדין החל הוא דין מדינת ישראל, בלי לגרוע מזכויות חובה או פורומים
העומדים למשתתפים. לשאלות: **{PI_EMAIL}**. אין לשלוח בדוא״ל נתוני בריאות,
סיסמאות או אסימוני גישה.
"""
    )

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link(PUBLIC_HOME_URL, label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link(PUBLIC_PRIVACY_URL, label="Privacy / פרטיות", icon="🔒")
with cols[2]:
    st.page_link(PUBLIC_ETHICS_URL, label="Ethics / אתיקה", icon="🏛️")
