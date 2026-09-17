import streamlit as st

# Import controllers
from controllers.auth_controller import AuthenticationController

from utils.fitbit_callback import handle_fitbit_callback
from utils.google_health_callback import handle_google_health_callback
from utils.access_control import render_demo_banner, render_legal_links
from utils.branding import render_app_logo

# Set up app configuration
st.set_page_config(
    page_title="AdmonTracker",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded"
)
render_app_logo(show_in_page=True)

def main():
    """Main application function - handles authentication and session state"""
    # Initialize authentication controller
    auth_controller = AuthenticationController()
    
    # 1) handle callback FIRST (no login required)
    if handle_google_health_callback(auth_controller):
        st.stop()
    if handle_fitbit_callback(auth_controller):
        st.stop()
    # Handle authentication in sidebar
    auth_controller.render_auth_ui()
    context = auth_controller.get_access_context()

    if context.is_authenticated:
        st.sidebar.markdown("## App Pages")
        st.sidebar.markdown("""
        - **Homepage**: Overview of the active users and their projects
        - **Dashboard**: Overview of wearable activity and device stats
        - **Device Management**: Manage research wearable devices
        - **Alerts Configuration**: Configure alerts for devices and EMA
        - **NOVA Qualtrics Management**: Manage buldog and Qualtrics data
        - **APPSHEET Management**: Manage AppSheet data
        """)
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Need Help?")
        st.sidebar.markdown("Contact: radmon@psy.haifa.ac.il")

        st.title("Welcome to AdmonTracker")
        st.write(f"You are logged in as: **{context.email}**")
        st.write(f"Your role is: **{context.role}**")
        st.write(f"Your project is: **{context.project}**")
        st.write("You can now access the dashboard and features.")
        st.write("Use the sidebar to navigate through the app.")
    elif context.is_guest:
        render_demo_banner()
        st.title("Welcome to AdmonTracker")
        st.write("You are exploring the complete product interface with bundled synthetic examples.")
        st.write("Use the sidebar to open any feature page. Write and external-service actions are disabled.")
        st.subheader("Guest demo guarantees")
        st.markdown("""
        - No production spreadsheet, participant record, or real watch is loaded.
        - No Google Health, Fitbit, OAuth, messaging, or email request is made.
        - No changes can be saved locally or remotely.
        """)
    else:
        st.title("Welcome to AdmonTracker")
        st.caption("Stress & Psychopathology Lab • University of Haifa")
        english, hebrew = st.tabs(["English", "עברית"])
        with english:
            st.write(
                "This service supports University of Haifa ethics-approved research by "
                "connecting invited adult participants' wearable accounts, collecting "
                "authorized read-only Fitbit or Google Health measurements, and helping "
                "authorized study staff monitor data completeness."
            )
            st.info(
                "Research staff may log in from the sidebar. Participants should use only "
                "the private authorization link supplied by the study team."
            )
            st.markdown("### Staff features")
            st.markdown("""
            - Wearable activity, sleep, and physiology dashboards
            - Device and data-completeness monitoring
            - Research alerts and approved study integrations
            """)
        with hebrew:
            st.write(
                "השירות תומך במחקר שאושר על-ידי ועדת האתיקה של אוניברסיטת חיפה. "
                "הוא מאפשר חיבור חשבונות לבישים של משתתפים בגירים שהוזמנו למחקר, "
                "איסוף מדדי Fitbit או Google Health בקריאה בלבד שאושרו, ובקרת שלמות הנתונים."
            )
            st.info(
                "אנשי צוות יכולים להתחבר מהסרגל. משתתפים צריכים להשתמש רק בקישור "
                "ההרשאה הפרטי שנמסר להם מצוות המחקר."
            )
        st.markdown("---")
        st.markdown("### Need Help?")
        st.markdown("Contact Prof. Roee Admon: radmon@psy.haifa.ac.il")
        st.markdown("### Legal")
        render_legal_links()

if __name__ == "__main__":
    main()
