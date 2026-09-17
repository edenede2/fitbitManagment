import streamlit as st

from utils.compliance import PI_EMAIL, PI_NAME, STUDY_NUMBER, research_documents
from utils.branding import render_app_logo


st.set_page_config(
    page_title="Research Ethics - Wearable Research Manager",
    page_icon="🏛️",
    layout="wide",
)
render_app_logo()

language = st.radio("Language / שפה", ["English", "עברית"], horizontal=True)
is_hebrew = language == "עברית"

st.title("אתיקה ומסמכי מחקר" if is_hebrew else "Research Ethics and Participant Documents")
st.caption(
    f"מחקר {STUDY_NUMBER} • החוקר הראשי: פרופ׳ רועי אדמון"
    if is_hebrew
    else f"Study {STUDY_NUMBER} • Principal investigator: {PI_NAME}"
)
st.write(
    "בעמוד זה ניתן לעיין במסמכי המחקר הזמינים ולהוריד עותק."
    if is_hebrew
    else "Review the available study documents below or download a copy."
)

for document in research_documents():
    # Only published documents belong on the participant-facing page. Release
    # readiness and missing-asset diagnostics remain internal to staff tooling.
    if not document.exists:
        continue
    title = document.title_he if is_hebrew else document.title_en
    st.subheader(title)

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

st.write(("יצירת קשר: " if is_hebrew else "Contact: ") + PI_EMAIL)

st.divider()
cols = st.columns(3)
with cols[0]:
    st.page_link("app.py", label="Home / דף הבית", icon="🏠")
with cols[1]:
    st.page_link("pages/08_Privacy_Policy.py", label="Privacy / פרטיות", icon="🔒")
with cols[2]:
    st.page_link("pages/09_Terms_of_Service.py", label="Terms / תנאי שימוש", icon="📄")
