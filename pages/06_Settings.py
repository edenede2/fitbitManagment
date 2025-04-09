import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Settings - Fitbit Management System",
    page_icon="⚙️",
    layout="wide"
)

# Check authentication
if 'user_email' not in st.session_state:
    st.warning("Please log in from the main page to access this feature.")
    st.stop()

# Check role permissions (only Manager and Admin have access)
user_role = st.session_state.get('user_role', 'Guest')
if user_role not in ['Admin', 'manager']:
    st.warning("You don't have permission to access this page.")
    st.stop()

# Display settings content
st.title("Settings")
st.info("Settings functionality will be implemented here")
