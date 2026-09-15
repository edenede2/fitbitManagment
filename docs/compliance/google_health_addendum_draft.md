# DRAFT — NOT ETHICS APPROVED / טיוטה — טרם אושרה על־ידי ועדת האתיקה

## Google Health participant addendum — study 385/23

Principal investigator: Prof. Roee Admon, University of Haifa  
Contact: radmon@psy.haifa.ac.il | +972-4-824-0964

This addendum supplements the study's existing informed-consent form. It must not
be used to enroll or authorize a participant until its wording and method of
consent have been approved by the principal investigator and ethics committee.

### Nature and purpose

The study requests read-only access to wearable information needed to examine the
relationship between childhood experiences, sleep patterns, and emotional,
cognitive, and behavioral responses in adulthood. Depending on device availability,
the data can include steps/activity, calories, heart rate, heart-rate variability,
skin temperature, respiratory rate, sleep, and timestamps.

### Duration — REQUIRED ETHICS DECISION

`[INSERT THE EXACT PI/ETHICS-APPROVED COLLECTION AND RETENTION PERIOD.]`

### Data handling and security

The app uses a pseudonymous study/watch code. Research metadata is kept in
access-controlled Google Sheets, raw wearable archives in a restricted University
Google Shared Drive, and OAuth client secrets and participant access/refresh tokens
in Google Secret Manager. The application is hosted on Heroku. Access is limited
to authorized study personnel and approved processors. Data is not sold or used
for advertising, credit, employment, insurance, unrelated marketing, or training
general-purpose AI/ML models.

Google issues short-lived access tokens and, when available, a refresh token after
authorization. The service uses them only to read the measurements described here,
refreshes access when needed, and keeps token values in Secret Manager rather than
the research spreadsheet. Disconnecting removes the app's token access and requests
provider revocation. Participants must never send tokens to the research team.

### Risks and benefits

Possible risks include privacy or confidentiality loss, account-connection errors,
and discomfort associated with awareness of personal sleep or physiology data.
No direct medical benefit is promised. The app is not medical or emergency
monitoring, and wearable measurements may be incomplete or inaccurate.

### Voluntary authorization, withdrawal, and deletion — REQUIRED ETHICS DECISION

Participation and Google Health authorization are voluntary. Refusal or withdrawal
does not cause a penalty. Disconnecting stops future collection and revokes active
tokens. Contact radmon@psy.haifa.ac.il with the study code to withdraw or request
deletion.

`[INSERT THE EXACT PI/ETHICS-APPROVED RULE FOR DATA ALREADY COLLECTED,
DE-IDENTIFIED, ANALYZED, OR PUBLISHED.]`

### Proposed participant confirmation — requires approval

- I confirm that I am at least 18 years old.
- I received and completed the ethics-approved study consent.
- I reviewed this addendum, could save a copy, and voluntarily authorize the
  listed read-only data access.
- I understand how to withdraw, disconnect, and request deletion.

---

## נספח למשתתף עבור Google Health — מחקר 385/23

חוקר ראשי: פרופ׳ רועי אדמון, אוניברסיטת חיפה  
יצירת קשר: radmon@psy.haifa.ac.il | 04-824-0964

נספח זה משלים את טופס ההסכמה הקיים. אסור להשתמש בו לצירוף משתתף או למתן
הרשאה לפני שנוסחו ואופן קבלת ההסכמה יאושרו על־ידי החוקר הראשי וועדת האתיקה.

### מהות ומטרה

המחקר מבקש גישת קריאה בלבד למידע לביש הדרוש לבחינת הקשר בין חוויות ילדות,
דפוסי שינה ותגובות רגשיות, קוגניטיביות והתנהגותיות בבגרות. בהתאם לזמינות
המכשיר, המידע עשוי לכלול צעדים ופעילות, קלוריות, דופק, שונות קצב לב,
טמפרטורת עור, קצב נשימה, שינה וחותמות זמן.

### משך — נדרשת החלטת ועדת האתיקה

`[יש להכניס את תקופת האיסוף והשמירה המדויקת שאושרה.]`

### טיפול במידע ואבטחה

האפליקציה משתמשת בקוד מחקר/שעון בדוי. מטא-נתונים נשמרים ב-Google Sheets
מוגנים, ארכיוני מדדים ב-Shared Drive אוניברסיטאי מוגבל, וסודות OAuth ואסימוני
גישה ב-Google Secret Manager. האפליקציה מתארחת ב-Heroku. הגישה מוגבלת לצוות
המחקר ולספקים שאושרו. המידע אינו נמכר ואינו משמש לפרסום, אשראי, תעסוקה,
ביטוח, שיווק שאינו קשור או אימון מודלי AI/ML כלליים.

לאחר ההרשאה Google מנפיקה אסימון גישה קצר-חיים, וכאשר זמין גם אסימון רענון.
השירות משתמש בהם רק לקריאת המדדים המתוארים, מרענן גישה לפי הצורך ושומר את ערכי
האסימונים ב-Secret Manager ולא בגיליון המחקר. ניתוק מסיר את גישת האפליקציה
ומבקש ביטול אצל הספק. אין לשלוח אסימונים לצוות המחקר.

### סיכונים ותועלת

הסיכונים האפשריים כוללים אובדן פרטיות או סודיות, שגיאות בחיבור החשבון ואי-נוחות
הקשורה למודעות לנתוני שינה או פיזיולוגיה. לא מובטחת תועלת רפואית ישירה. זו
אינה מערכת רפואית או מערכת חירום, ומדדי המכשיר עלולים להיות חלקיים או שגויים.

### הרשאה מרצון, פרישה ומחיקה — נדרשת החלטת ועדת האתיקה

ההשתתפות וההרשאה ל-Google Health הן מרצון. סירוב או פרישה אינם גוררים קנס.
ניתוק מפסיק איסוף עתידי ומבטל אסימונים פעילים. לפרישה או בקשת מחיקה יש לפנות
ל-radmon@psy.haifa.ac.il ולציין את קוד המחקר.

`[יש להכניס את הכלל המדויק שאושר לגבי מידע שכבר נאסף, בוטל זיהויו, נותח או פורסם.]`

### אישור משתתף מוצע — טעון אישור

- אני מאשר/ת שמלאו לי 18 שנים.
- קיבלתי והשלמתי את ההסכמה למחקר שאושרה על־ידי ועדת האתיקה.
- עיינתי בנספח, יכולתי לשמור עותק, ואני מאשר/ת מרצון את גישת הקריאה המפורטת.
- ברור לי כיצד לפרוש, לנתק את החיבור ולבקש מחיקה.
