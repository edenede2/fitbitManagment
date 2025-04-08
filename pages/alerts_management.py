import streamlit as st
import pandas as pd
import datetime
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, SheetsAPI
from model.config import get_secrets
import time

# Page configuration
# st.set_page_config(
#     page_title="Alert Management",
#     page_icon="🔔",
#     layout="wide"
# )

# Initialize session state for tracking changes
if "accepted_suspicious" not in st.session_state:
    st.session_state.accepted_suspicious = []
if "accepted_late" not in st.session_state:
    st.session_state.accepted_late = []

# ---- Functions for loading and updating sheet data ----

def load_spreadsheet():
    """Load and connect to the Google Spreadsheet"""
    # Get the spreadsheet key from secrets
    spreadsheet_key = get_secrets().get("spreadsheet_key", "")
    
    # Create and connect spreadsheet
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key=spreadsheet_key
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    
    return spreadsheet

def load_total_answers(spreadsheet:Spreadsheet):
    """Load total answers from spreadsheet"""
    total_answers_sheet = spreadsheet.get_sheet("qualtrics_nova", "qualtrics_nova")
    df = total_answers_sheet.to_dataframe(engine="polars")
    
        
    return df, total_answers_sheet

def load_suspicious_numbers(spreadsheet:Spreadsheet):
    """Load suspicious numbers from spreadsheet"""
    suspicious_sheet = spreadsheet.get_sheet("suspicious_nums", "suspicious_nums")
    df = suspicious_sheet.to_dataframe(engine="polars")
    
    # Add column for verification if not exists
    if 'accepted' not in df.columns:
        df['accepted'] = False
        
    return df, suspicious_sheet

def load_late_numbers(spreadsheet:Spreadsheet):
    """Load late numbers from spreadsheet"""
    late_sheet = spreadsheet.get_sheet("late_nums", "late_nums")
    df = pd.DataFrame(late_sheet.data)
    
    # Add column for verification if not exists
    if 'accepted' not in df.columns:
        df['accepted'] = False
        
    return df, late_sheet

def update_suspicious_sheet(spreadsheet, suspicious_sheet, row_index, accept=True):
    """Update the suspicious_nums sheet with acceptance status"""
    suspicious_sheet.data[row_index]['accepted'] = accept
    suspicious_sheet.data[row_index]['lastUpdated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save changes to Google Sheets
    GoogleSheetsAdapter.save(spreadsheet, "suspicious_nums")
    st.success(f"Updated suspicious number: {suspicious_sheet.data[row_index]['nums']}")
    
    # Record in session state to maintain UI state
    st.session_state.accepted_suspicious.append(suspicious_sheet.data[row_index]['nums'])
    
    # Return updated dataframe for display
    return pd.DataFrame(suspicious_sheet.data)

def update_late_sheet(spreadsheet, late_sheet, row_index, accept=True):
    """Update the late_nums sheet with acceptance status"""
    late_sheet.data[row_index]['accepted'] = accept
    late_sheet.data[row_index]['lastUpdated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save changes to Google Sheets
    GoogleSheetsAdapter.save(spreadsheet, "late_nums")
    st.success(f"Updated late number: {late_sheet.data[row_index]['nums']}")
    
    # Record in session state to maintain UI state
    st.session_state.accepted_late.append(late_sheet.data[row_index]['nums'])
    
    # Return updated dataframe for display
    return pd.DataFrame(late_sheet.data)

def format_time_ago(timestamp_str):
    """Format a timestamp to show how long ago it occurred"""
    try:
        if not timestamp_str:
            return "Unknown"
            
        # Try to parse the timestamp with different formats
        for fmt in ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
            try:
                timestamp = datetime.datetime.strptime(timestamp_str, fmt)
                break
            except ValueError:
                continue
        else:
            return timestamp_str  # Return original if no format works
            
        now = datetime.datetime.now()
        diff = now - timestamp
        
        if diff.days > 0:
            return f"{diff.days} days ago"
        elif diff.seconds >= 3600:
            hours = diff.seconds // 3600
            return f"{hours} hours ago"
        elif diff.seconds >= 60:
            minutes = diff.seconds // 60
            return f"{minutes} minutes ago"
        else:
            return f"{diff.seconds} seconds ago"
    except Exception as e:
        return f"Error: {str(e)}"

# Create a main function that can be called from app.py
def show_alerts_management(user_email, user_role, user_project):
    """Main function to display the alerts management page - can be called from app.py"""
        # Initialize session state
    if "accepted_suspicious" not in st.session_state:
        st.session_state.accepted_suspicious = []
    if "accepted_late" not in st.session_state:
        st.session_state.accepted_late = []
        
    # Page configuration
    st.title("📊 Alert Management")
    st.write("Review and manage patient questionnaire alerts.")

    # Load data
    with st.spinner("Loading data..."):
        spreadsheet = load_spreadsheet()
        total_answers_df , total_answers_sheet = load_total_answers(spreadsheet)
        suspicious_df, suspicious_sheet = load_suspicious_numbers(spreadsheet)
        late_df, late_sheet = load_late_numbers(spreadsheet)

    # Create tabs for different alert types
    tab1, tab2, tab3 = st.tabs(["Total Answers", "Suspicious Numbers", "Late Numbers"])

    # ----- Total Answers Tab -----
    with tab1:
        st.header("Total Answers")
        st.info("This is the total number of patients who answered the questionnaire.")
        
        # Check if there's data
        if total_answers_df.is_empty():
            st.warning("No total answers found.")
        else:
            if 'endDate' in total_answers_df.columns:
                suspicious_df['Time Ago'] = suspicious_df['endDate'].apply(format_time_ago)
            st.dataframe(total_answers_df)
            st.subheader("Summary Statistics")
            st.write(total_answers_df.describe())


            

    # ----- Suspicious Numbers Tab -----
    with tab2:
        st.header("Suspicious Numbers")
        st.info("These are patients who answered the questionnaire but their phone numbers weren't identified in the Bulldog system.")
        
        # Check if there's data
        if suspicious_df.empty:
            st.warning("No suspicious numbers found.")
        else:
            # Add human-readable time ago column for display
            if 'filledTime' in suspicious_df.columns:
                suspicious_df['Time Ago'] = suspicious_df['filledTime'].apply(format_time_ago)
                
            # Filter options
            st.subheader("Filter Options")
            show_accepted = st.checkbox("Show Accepted Numbers", value=False, key="show_accepted_suspicious")
            
            # Apply filters
            filtered_df = suspicious_df
            if not show_accepted:
                filtered_df = suspicious_df[~suspicious_df['accepted'].astype(str).str.lower().isin(['true', 'yes', '1', 't'])]
            
            # Display data table
            st.subheader(f"Suspicious Numbers ({len(filtered_df)} entries)")
            
            # Create a copy for display with better column names
            display_df = filtered_df.copy()
            if 'nums' in display_df.columns:
                display_df = display_df.rename(columns={
                    'nums': 'Phone Number',
                    'filledTime': 'Questionnaire Filled',
                    'lastUpdated': 'Last Reviewed',
                    'accepted': 'Accepted'
                })
            
                # Reorder columns for better display
                column_order = ['Phone Number', 'Questionnaire Filled', 'Time Ago', 'Last Reviewed', 'Accepted']
                display_df = display_df[[col for col in column_order if col in display_df.columns]]
            
            st.dataframe(display_df)
            
            # Process individual entries
            st.subheader("Review Suspicious Numbers")
            
            # Use columns for better layout
            cols = st.columns([1, 4])
            with cols[0]:
                selected_index = st.number_input("Select Row #", 
                                              min_value=0, 
                                              max_value=len(filtered_df)-1 if len(filtered_df) > 0 else 0,
                                              value=0)
            
            # Only show accept buttons if there are entries
            if not filtered_df.empty:
                with cols[1]:
                    # Get the actual index in the original dataframe
                    actual_index = filtered_df.index[selected_index] if selected_index < len(filtered_df) else 0
                    
                    # Display selected entry details
                    if 'nums' in filtered_df.columns and actual_index in filtered_df.index:
                        selected_number = filtered_df.loc[actual_index, 'nums']
                        st.write(f"Selected: **{selected_number}**")
                        
                        is_accepted = filtered_df.loc[actual_index, 'accepted']
                        if str(is_accepted).lower() in ['true', 'yes', '1', 't']:
                            st.success("This number has already been accepted")
                        else:
                            accept_button = st.button("Mark as Accepted", key=f"accept_suspicious_{actual_index}")
                            
                            if accept_button:
                                # Get the matching index in the original sheet data
                                sheet_index = suspicious_df.index.get_loc(actual_index)
                                
                                # Update the sheet
                                suspicious_df = update_suspicious_sheet(spreadsheet, suspicious_sheet, sheet_index)
                                
                                # Refresh the filtered df
                                filtered_df = suspicious_df
                                if not show_accepted:
                                    filtered_df = suspicious_df[~suspicious_df['accepted'].astype(str).str.lower().isin(['true', 'yes', '1', 't'])]
                                
                                st.rerun()

    # ----- Late Numbers Tab -----
    with tab3:
        st.header("Late Numbers")
        st.info("These are patients who were sent a WhatsApp questionnaire but did not answer within the time threshold.")
        
        # Check if there's data
        if late_df.empty:
            st.warning("No late numbers found.")
        else:
            # Add human-readable time ago column
            if 'sentTime' in late_df.columns:
                late_df['Time Ago'] = late_df['sentTime'].apply(format_time_ago)
                
            # Filter options
            st.subheader("Filter Options")
            show_accepted = st.checkbox("Show Accepted Numbers", value=False, key="show_accepted_late")
            
            # Apply filters
            filtered_df = late_df
            if not show_accepted:
                filtered_df = late_df[~late_df['accepted'].astype(str).str.lower().isin(['true', 'yes', '1', 't'])]
            
            # Display data table
            st.subheader(f"Late Numbers ({len(filtered_df)} entries)")
            
            # Create a copy for display with better column names
            display_df = filtered_df.copy()
            if 'nums' in display_df.columns:
                display_df = display_df.rename(columns={
                    'nums': 'Phone Number',
                    'sentTime': 'WhatsApp Sent',
                    'hoursLate': 'Hours Late',
                    'lastUpdated': 'Last Reviewed',
                    'accepted': 'Accepted'
                })
            
                # Reorder columns for better display
                column_order = ['Phone Number', 'WhatsApp Sent', 'Time Ago', 'Hours Late', 'Last Reviewed', 'Accepted']
                display_df = display_df[[col for col in column_order if col in display_df.columns]]
            
            st.dataframe(display_df)
            
            # Process individual entries
            st.subheader("Review Late Numbers")
            
            # Use columns for better layout
            cols = st.columns([1, 4])
            with cols[0]:
                selected_index = st.number_input("Select Row #", 
                                              min_value=0, 
                                              max_value=len(filtered_df)-1 if len(filtered_df) > 0 else 0,
                                              value=0,
                                              key="late_index")
            
            # Only show accept buttons if there are entries
            if not filtered_df.empty:
                with cols[1]:
                    # Get the actual index in the original dataframe
                    actual_index = filtered_df.index[selected_index] if selected_index < len(filtered_df) else 0
                    
                    # Display selected entry details
                    if 'nums' in filtered_df.columns and actual_index in filtered_df.index:
                        selected_number = filtered_df.loc[actual_index, 'nums']
                        st.write(f"Selected: **{selected_number}**")
                        
                        is_accepted = filtered_df.loc[actual_index, 'accepted']
                        if str(is_accepted).lower() in ['true', 'yes', '1', 't']:
                            st.success("This number has already been accepted")
                        else:
                            accept_button = st.button("Mark as Accepted", key=f"accept_late_{actual_index}")
                            
                            if accept_button:
                                # Get the matching index in the original sheet data
                                sheet_index = late_df.index.get_loc(actual_index)
                                
                                # Update the sheet
                                late_df = update_late_sheet(spreadsheet, late_sheet, sheet_index)
                                
                                # Refresh the filtered df
                                filtered_df = late_df
                                if not show_accepted:
                                    filtered_df = late_df[~late_df['accepted'].astype(str).str.lower().isin(['true', 'yes', '1', 't'])]
                                
                                st.rerun()

    # Add a footer with helpful information
    st.divider()
    st.write("### About Alert Management")
    st.markdown("""
    This page helps you manage questionnaire-related alerts:

    - **Suspicious Numbers**: Patients who completed questionnaires but weren't properly identified in our system
    - **Late Numbers**: Patients who received WhatsApp questionnaires but haven't completed them within the expected timeframe

    Mark items as "Accepted" once you've reviewed them to stop receiving email alerts about them.
    """)

# If this script is run directly, call the main function
def display_alerts_management(user_email, user_role, user_project):
    """Function to display the alerts management page"""
    

    # Show the page
    show_alerts_management(user_email, user_role, user_project)
