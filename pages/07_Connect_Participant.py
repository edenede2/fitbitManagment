import streamlit as st
from utils.fitbit_oauth import new_state, build_authorize_url
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter

def get_spreadsheet():
    spreadsheet_key = st.secrets["spreadsheet_key"]
    ss = Spreadsheet(name="Fitbit Database", api_key=spreadsheet_key)
    GoogleSheetsAdapter.connect(ss)
    return ss

st.title("Connect Participant (Fitbit OAuth)")

participant = st.text_input("participant_anon_id")
if st.button("Generate connect link") and participant:
    state = new_state()
    auth_url = build_authorize_url(state)

    ss = get_spreadsheet()
    states_sheet = ss.get_sheet("oauth_states", sheet_type="oauth_states")  # תצטרכו sheet_type או legacy בהתאם למימוש שלכם
    # כתיבה - בהתאם ל-API של הישות אצלכם:
    ss.update_sheet("oauth_states", [{
        "state": state,
        "participant_anon_id": participant,
        "created_at": str(st.session_state.get("now", "")),
        "used": "FALSE",
        "used_at": "",
        "notes": ""
    }], strategy="append")

    st.success("Link generated")
    st.write("Open this link in an incognito window while logged into the participant's demo account:")
    st.code(auth_url)
    st.link_button("Open authorization", auth_url)