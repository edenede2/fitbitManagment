import streamlit as st

# Page configuration
st.set_page_config(
    page_title="About - Fitbit Management System",
    page_icon="ℹ️",
    layout="wide"
)

# Display about content (accessible to all users)
st.title("About")
st.write("""
## Fitbit Management System

This application allows monitoring and management of Fitbit devices across different projects.

### Features
- Real-time device monitoring
- Historical data analysis
- User assignment and management
- Battery and health tracking

### User Roles
- **Admin:** Full access to all projects and features
- **Manager:** Access to specific project data and settings
- **Student:** Access to assigned watches only
- **Guest:** Limited access to general information
""")
