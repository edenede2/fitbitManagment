import streamlit as st

# Import controllers
from controllers.auth_controller import AuthenticationController

from utils.fitbit_callback import handle_fitbit_callback
from utils.google_health_callback import handle_google_health_callback
from utils.access_control import render_demo_banner, render_legal_links

# Set up app configuration
st.set_page_config(
    page_title="Wearable Research Manager",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
        st.sidebar.markdown("Contact support: edenede2@gmail.com")

        st.title("Welcome to Wearable Research Manager")
        st.write(f"You are logged in as: **{context.email}**")
        st.write(f"Your role is: **{context.role}**")
        st.write(f"Your project is: **{context.project}**")
        st.write("You can now access the dashboard and features.")
        st.write("Use the sidebar to navigate through the app.")
    elif context.is_guest:
        render_demo_banner()
        st.title("Welcome to Wearable Research Manager")
        st.write("You are exploring the complete product interface with bundled synthetic examples.")
        st.write("Use the sidebar to open any feature page. Write and external-service actions are disabled.")
        st.subheader("Guest demo guarantees")
        st.markdown("""
        - No production spreadsheet, participant record, or real watch is loaded.
        - No Google Health, Fitbit, OAuth, messaging, or email request is made.
        - No changes can be saved locally or remotely.
        """)
    else:
        st.title("Welcome to Wearable Research Manager")
        st.write("Please log in to access the dashboard and features.")
        st.info("Use the sidebar to authenticate or open the read-only guest demonstration.")
        st.markdown("## Features Available After Login:")
        st.markdown("""
        - **Dashboard**: Overview of wearable activity and stats
        - **Device Tracking**: Monitor research devices and sync status
        - **Data Analysis**: Analyze collected health and activity data
        - **Research Operations**: Review alerts and connected study tools
        """)
        st.markdown("---")
        st.markdown("### Need Help?")
        st.markdown("Contact support: edenede2@gmail.com")
        st.markdown("### Legal")
        render_legal_links()

if __name__ == "__main__":
    main()
