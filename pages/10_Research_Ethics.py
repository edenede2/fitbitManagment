import streamlit as st

from utils.compliance import PI_EMAIL, PI_NAME, STUDY_NUMBER, research_documents


st.set_page_config(
    page_title="Research Ethics - Wearable Research Manager",
    page_icon="🏛️",
    layout="wide",
)

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)
is_hebrew = language == "עברית"

st.title("אתיקה ומסמכי מחקר" if is_hebrew else "Research Ethics and Participant Documents")
st.caption(
    f"מחקר {STUDY_NUMBER} • החוקר הראשי: פרופ׳ רועי אדמון"
    if is_hebrew
    else f"Study {STUDY_NUMBER} • Principal investigator: {PI_NAME}"
)
st.info(
    "המסמכים מפורסמים לשקיפות. פרסומם אינו מהווה אישור של Google, Fitbit או NIH למחקר."
    if is_hebrew
    else (
        "These documents are provided for transparency. Publication does not imply that "
        "Google, Fitbit, or NIH endorses or has approved the study."
    )
)

for document in research_documents():
    title = document.title_he if is_hebrew else document.title_en
    status = document.status_he if is_hebrew else document.status_en
    st.subheader(title)
    st.write(status)
    if not document.exists:
        st.warning("המסמך אינו זמין בפריסה זו." if is_hebrew else "Document unavailable in this deployment.")
        continue

    data = document.path.read_bytes()
    if document.mime_type == "application/pdf" and hasattr(st, "pdf"):
        with st.expander("תצוגה מקדימה" if is_hebrew else "Preview"):
            st.pdf(data, height=720)
    elif document.mime_type != "application/pdf":
        st.caption(
            "הקובץ המקורי הוא DOCX וזמין להורדה."
            if is_hebrew
            else "The original is a DOCX file and is available for download."
        )

    st.download_button(
        "הורדת המסמך" if is_hebrew else "Download document",
        data=data,
        file_name=document.download_name,
        mime=document.mime_type,
        key=f"download_{document.key}_{language}",
    )
    st.caption(f"SHA-256: `{document.sha256()}`")

st.warning(
    "הטופס הקיים אינו כולל הרשאת Google Health. נספח דו-לשוני נמצא בהכנה ויפורסם "
    "כמסמך מאושר רק לאחר אישור החוקר הראשי וועדת האתיקה."
    if is_hebrew
    else (
        "The existing pilot consent does not cover Google Health authorization. A bilingual "
        "addendum is being prepared and will be presented as approved only after PI and ethics review."
    )
)
st.write(("יצירת קשר: " if is_hebrew else "Contact: ") + PI_EMAIL)

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link("app.py", label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy / פרטיות", icon="🔒")
with cols[2]:
    st.page_link("pages/09_Terms_of_Service.py", label="Terms / תנאי שימוש", icon="📄")
