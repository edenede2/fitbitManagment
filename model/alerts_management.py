import streamlit as st
import pandas as pd
import polars as pl
import datetime
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, SheetsAPI
from model.config import get_secrets
import time

# Initialize session state for tracking changes and caching data
if "accepted_suspicious" not in st.session_state:
    st.session_state.accepted_suspicious = []
if "accepted_late" not in st.session_state:
    st.session_state.accepted_late = []

# Setup separate cache entries for dataframes to avoid unpacking issues
if "cached_total_answers_df" not in st.session_state:
    st.session_state.cached_total_answers_df = None
if "cached_suspicious_df" not in st.session_state:
    st.session_state.cached_suspicious_df = None
if "cached_late_df" not in st.session_state:
    st.session_state.cached_late_df = None
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = None
if "edited_data" not in st.session_state:
    st.session_state.edited_data = {
        "total_answers": None,
        "suspicious": None,
        "late": None
    }
# Add session state for filter preferences
if "show_accepted_suspicious" not in st.session_state:
    st.session_state.show_accepted_suspicious = False
if "show_accepted_late" not in st.session_state:
    st.session_state.show_accepted_late = False

# Add additional session state for tracking pending changes
if "pending_suspicious_changes" not in st.session_state:
    st.session_state.pending_suspicious_changes = {}
if "pending_late_changes" not in st.session_state:
    st.session_state.pending_late_changes = {}

# ---- Functions for loading and updating sheet data ----

# Create a simple connection to the spreadsheet - don't cache the object
def get_spreadsheet_connection():
    """Create and connect to Google Spreadsheet"""
    # Get the spreadsheet key from secrets
    spreadsheet_key = get_secrets().get("spreadsheet_key", "")
    
    # Create and connect spreadsheet
    spreadsheet = Spreadsheet(
        name="Fitbit Database",
        api_key=spreadsheet_key
    )
    GoogleSheetsAdapter.connect(spreadsheet)
    
    return spreadsheet

# Cache the raw data instead of the spreadsheet object
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_sheet_data(sheet_name):
    """Get raw data from a specific sheet and cache it"""
    try:
        # Get fresh connection
        spreadsheet = get_spreadsheet_connection()
        
        # Get sheet data
        sheet = spreadsheet.get_sheet(sheet_name, sheet_name)
        
        # Get the raw data as a list of lists or dict
        raw_data = sheet.to_dataframe(engine="polars").to_dict(as_series=False)
        
        return raw_data
    except Exception as e:
        st.error(f"Error getting data from sheet {sheet_name}: {str(e)}")
        return {}

# Use the cached raw data to create dataframes
def load_total_answers():
    """Load total answers from cached sheet data"""
    # Get cached data
    raw_data = get_sheet_data("EMA")
    
    # Convert to polars dataframe
    if raw_data:
        df = pl.DataFrame(raw_data)
        
        # Convert accepted column to boolean if it exists
        if 'accepted' in df.columns:
            df = df.with_columns(
                pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
            )
            
        return df
    
    return pl.DataFrame()

def load_suspicious_numbers():
    """Load suspicious numbers from cached sheet data"""
    # Get cached data
    raw_data = get_sheet_data("suspicious_nums")
    
    # Convert to polars dataframe
    if raw_data:
        df = pl.DataFrame(raw_data)
        
        # Add column for verification if not exists
        if 'accepted' not in df.columns:
            df = df.with_columns(pl.lit(False).alias('accepted'))
        else:
            # Convert existing accepted values to boolean
            df = df.with_columns(
                pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
            )
            
        return df
    
    return pl.DataFrame()

def load_late_numbers():
    """Load late numbers from cached sheet data"""
    # Get cached data
    raw_data = get_sheet_data("late_nums")
    
    # Convert to polars dataframe
    if raw_data:
        df = pl.DataFrame(raw_data)
        
        # Add column for verification if not exists
        if 'accepted' not in df.columns:
            df = df.with_columns(pl.lit(False).alias('accepted'))
        else:
            # Convert existing accepted values to boolean
            df = df.with_columns(
                pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
            )
            
        return df
    
    return pl.DataFrame()

# Function to update a specific sheet - this will be done sparingly
def update_sheet_data(sheet_name, df):
    """Update a specific sheet with new data"""
    try:
        # Get fresh connection
        spreadsheet = get_spreadsheet_connection()
        
        # Update the sheet
        spreadsheet.update_sheet(sheet_name, df)
        GoogleSheetsAdapter.save(spreadsheet, sheet_name)
        
        # Clear the cache for this sheet to force a refresh next time
        get_sheet_data.clear()
        
        return True
    except Exception as e:
        st.error(f"Error updating sheet {sheet_name}: {str(e)}")
        return False

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

# Add callbacks for data editor changes
def on_total_answers_change(edited_df):
    st.session_state.edited_data["total_answers"] = edited_df

def on_suspicious_change(edited_df):
    st.session_state.edited_data["suspicious"] = edited_df

def on_late_change(edited_df):
    st.session_state.edited_data["late"] = edited_df

# Toggle handlers for show_accepted filters with safety checks
def toggle_show_accepted_suspicious():
    """Toggle the show_accepted_suspicious session state with safety check"""
    if "show_accepted_suspicious" in st.session_state:
        st.session_state.show_accepted_suspicious = not st.session_state.show_accepted_suspicious
    else:
        # Initialize it if it doesn't exist yet
        st.session_state.show_accepted_suspicious = True

def toggle_show_accepted_late():
    """Toggle the show_accepted_late session state with safety check"""
    if "show_accepted_late" in st.session_state:
        st.session_state.show_accepted_late = not st.session_state.show_accepted_late
    else:
        # Initialize it if it doesn't exist yet
        st.session_state.show_accepted_late = True

# New function to handle pending changes for suspicious numbers
def handle_suspicious_status_change(phone_number, new_status):
    """Store suspicious number status change in session state without saving to sheet yet"""
    st.session_state.pending_suspicious_changes[phone_number] = new_status

# New function to handle pending changes for late numbers
def handle_late_status_change(phone_number, new_status):
    """Store late number status change in session state without saving to sheet yet"""
    st.session_state.pending_late_changes[phone_number] = new_status

# Create a main function that can be called from app.py
def show_alerts_management(user_email, user_role, user_project, spreadsheet: Spreadsheet) -> None:
    """Main function to display the alerts management page - can be called from app.py"""
    # Ensure session state variables are initialized at the beginning of this function
    if "show_accepted_suspicious" not in st.session_state:
        st.session_state.show_accepted_suspicious = False
    if "show_accepted_late" not in st.session_state:
        st.session_state.show_accepted_late = False
    if "pending_suspicious_changes" not in st.session_state:
        st.session_state.pending_suspicious_changes = {}
    if "pending_late_changes" not in st.session_state:
        st.session_state.pending_late_changes = {}
    
    # Page configuration
    st.title("📊 Alert Management")
    st.write("Review and manage patient questionnaire alerts.")

    # Create a refresh button in the sidebar
    with st.sidebar:
        refresh = st.button("↻ Refresh Data")
        if st.session_state.last_refresh:
            st.caption(f"Last refreshed: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    
    # Load data only if needed (first load or explicit refresh)
    if (st.session_state.cached_total_answers_df is None or 
        st.session_state.cached_suspicious_df is None or
        st.session_state.cached_late_df is None or
        refresh):
        
        with st.spinner("Loading data..."):
            # Clear the cache to force a refresh
            if refresh:
                get_sheet_data.clear()
            
            # Load data from cached raw data
            total_answers_df = load_total_answers()
            suspicious_df = load_suspicious_numbers()
            late_df = load_late_numbers()
            
            # Update session state caches
            st.session_state.cached_total_answers_df = total_answers_df
            st.session_state.cached_suspicious_df = suspicious_df
            st.session_state.cached_late_df = late_df
            st.session_state.last_refresh = datetime.datetime.now()
            
            # Clear edited data on refresh
            st.session_state.edited_data["total_answers"] = None
            st.session_state.edited_data["suspicious"] = None
            st.session_state.edited_data["late"] = None
    else:
        # Use cached data 
        total_answers_df = st.session_state.cached_total_answers_df
        suspicious_df = st.session_state.cached_suspicious_df
        late_df = st.session_state.cached_late_df

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
            # Add time ago information if endDate column exists
            if 'endDate' in total_answers_df.columns:
                display_df = total_answers_df.with_columns(
                    pl.col('endDate').map_elements(format_time_ago, return_dtype=pl.Utf8).alias('Time Ago')
                )
            else:
                display_df = total_answers_df
            
            # Filter options
            st.subheader("Filter Options")
            date_filter = st.date_input("Filter by date (from)", 
                                     value=datetime.datetime.now() - datetime.timedelta(days=7),
                                     max_value=datetime.datetime.now())
            
            # Apply filters if date column exists - this is done in-memory, no API calls
            if 'endDate' in display_df.columns and date_filter:
                date_str = date_filter.strftime("%Y-%m-%d")
                display_df = display_df.filter(pl.col('endDate') >= date_str)
            
            # Display data with editor
            st.subheader(f"Total Answers ({display_df.height} entries)")
            
            # Configure the checkbox column for accepted
            column_config = {}
            if 'accepted' in display_df.columns:
                column_config["accepted"] = st.column_config.CheckboxColumn("Accepted", help="Mark as accepted")
            
            # Use session state to maintain editor state between rerenders
            if st.session_state.edited_data["total_answers"] is None:
                pandas_df = display_df.to_pandas()
            else:
                pandas_df = st.session_state.edited_data["total_answers"]
            
            edited_df = st.data_editor(
                pandas_df,
                key="total_answers_editor",
                column_config=column_config,
                on_change=lambda: on_total_answers_change(st.session_state.total_answers_editor)
            )
            
            # Save changes button
            if st.button("Save Changes to Total Answers", key="save_total_answers"):
                try:
                    # Convert back to polars
                    updated_df = pl.DataFrame(edited_df)
                    
                    # Convert boolean accepted column back to TRUE/FALSE strings for Google Sheets
                    if 'accepted' in updated_df.columns:
                        updated_df = updated_df.with_columns(
                            pl.when(pl.col('accepted')).then(pl.lit("TRUE")).otherwise(pl.lit("FALSE")).alias('accepted')
                        )
                    
                    # Update the sheet using our helper function
                    if update_sheet_data("EMA", updated_df):
                        st.success("Total answers data updated successfully!")
                        
                        # Clear cached data for this sheet
                        st.session_state.cached_total_answers_df = None
                        
                        # Clear edited data after successful save
                        st.session_state.edited_data["total_answers"] = None
                        
                        # Add slight delay to avoid immediate refresh
                        time.sleep(0.5)
                        st.rerun()
                except Exception as e:
                    st.error(f"Error saving changes: {str(e)}")
            
            # Summary statistics (only show if not too much data to avoid performance issues)
            if display_df.height < 1000:  # Only calculate stats for reasonably sized datasets
                st.subheader("Summary Statistics")
                
                # Count answers by date if date column exists
                if 'endDate' in display_df.columns:
                    date_counts = display_df.group_by('endDate').agg(pl.count().alias('Count'))
                    st.subheader("Answers by Date")
                    st.bar_chart(date_counts.to_pandas().set_index('endDate'))
                
                # Show numerical statistics for any numeric columns
                numeric_cols = display_df.select(pl.col(pl.NUMERIC_DTYPES)).columns
                if numeric_cols:
                    stats_df = display_df.select(numeric_cols).describe()
                    st.write(stats_df)

    # ----- Suspicious Numbers Tab -----
    with tab2:
        st.header("Suspicious Numbers")
        st.info("These are patients who answered the questionnaire but their phone numbers weren't identified in the Bulldog system.")
        
        # Check if there's data
        if suspicious_df.is_empty():
            st.warning("No suspicious numbers found.")
        else:
            # Add human-readable time ago column for display
            if 'filledTime' in suspicious_df.columns:
                suspicious_df = suspicious_df.with_columns(
                    pl.col('filledTime').map_elements(format_time_ago, return_dtype=pl.Utf8).alias('Time Ago')
                )
                
            # Filter options - use session state to persist filter choice
            st.subheader("Filter Options")
            # Make sure we're using the session state value directly, not through a variable
            show_accepted = st.checkbox("Show Accepted Numbers", 
                                      value=st.session_state.show_accepted_suspicious, 
                                      key="suspicious_filter",
                                      on_change=toggle_show_accepted_suspicious)
            
            # Apply filters - done in memory, no API calls
            filtered_df = suspicious_df
            if not show_accepted:
                filtered_df = suspicious_df.filter(
                    ~pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't'])
                )
            
            # Apply any pending changes to the display dataframe (doesn't change original data)
            if st.session_state.pending_suspicious_changes:
                # Create a copy of the dataframe to apply pending changes
                display_copy = filtered_df.clone()
                
                # Create a modified version with pending changes applied
                for phone, new_status in st.session_state.pending_suspicious_changes.items():
                    # Create a mask for the row to update
                    mask = pl.col("nums") == phone
                    # Update the status in the display copy
                    display_copy = display_copy.with_columns(
                        pl.when(mask)
                          .then(pl.lit(new_status))
                          .otherwise(pl.col("accepted"))
                          .alias("accepted")
                    )
                
                # Use the modified dataframe for display
                filtered_df = display_copy
            
            # Display data table with better column names
            st.subheader(f"Suspicious Numbers ({filtered_df.height} entries)")
            
            # Create a display version with better column names
            display_df = filtered_df.clone()
            if 'nums' in display_df.columns:
                display_df = display_df.rename({
                    'nums': 'Phone Number',
                    'filledTime': 'Questionnaire Filled',
                    'lastUpdated': 'Last Reviewed',
                    'accepted': 'Accepted'
                })
            
                # Reorder columns for better display
                column_order = ['Phone Number', 'Questionnaire Filled', 'Time Ago', 'Last Reviewed', 'Accepted']
                available_columns = [col for col in column_order if col in display_df.columns]
                display_df = display_df.select(available_columns)
            
            # Convert to pandas for display
            pandas_df = display_df.to_pandas()
            
            # Show dataframe
            st.dataframe(pandas_df, use_container_width=True)
            
            # Add a selection mechanism for editing
            st.subheader("Mark Numbers as Accepted")
            
            # Create two columns for layout
            col1, col2 = st.columns([1, 3])
            
            # Let user select which number to update
            with col1:
                if 'Phone Number' in pandas_df.columns:
                    phone_numbers = pandas_df['Phone Number'].tolist()
                    selected_phone = st.selectbox(
                        "Select Phone Number", 
                        options=phone_numbers,
                        key="suspicious_phone_select"
                    )
                    
                    # Get the row index from the display dataframe
                    selected_idx = pandas_df[pandas_df['Phone Number'] == selected_phone].index[0]
                    
                    # Get current acceptance status, checking pending changes first
                    if selected_phone in st.session_state.pending_suspicious_changes:
                        current_status = st.session_state.pending_suspicious_changes[selected_phone]
                    else:
                        current_status = pandas_df.loc[selected_idx, 'Accepted']
                    
                    # Create the checkbox for accepting/rejecting
                    new_status = st.checkbox("Mark as Accepted", 
                                           value=current_status,
                                           key=f"suspicious_accept_{selected_idx}")
                    
                    # Store the change in pending changes if it differs from current status
                    if new_status != current_status:
                        handle_suspicious_status_change(selected_phone, new_status)
                        st.rerun()  # Refresh to show the changes in the table
            
            # Show details about the selected number
            with col2:
                if 'Phone Number' in pandas_df.columns and selected_phone:
                    # Get details for the selected phone
                    row = pandas_df[pandas_df['Phone Number'] == selected_phone].iloc[0]
                    
                    st.markdown(f"### Details for {selected_phone}")
                    st.markdown(f"**Questionnaire Filled:** {row.get('Questionnaire Filled', 'N/A')}")
                    st.markdown(f"**Time Ago:** {row.get('Time Ago', 'N/A')}")
                    st.markdown(f"**Last Reviewed:** {row.get('Last Reviewed', 'N/A')}")
                    st.markdown(f"**Current Status:** {'Accepted' if row.get('Accepted', False) else 'Not Accepted'}")
            
            # Add a save button at the bottom to commit all pending changes
            if st.session_state.pending_suspicious_changes:
                st.divider()
                st.write(f"**{len(st.session_state.pending_suspicious_changes)}** phone numbers have pending status changes.")
                
                # Display the pending changes
                pending_changes_text = ""
                for phone, status in st.session_state.pending_suspicious_changes.items():
                    pending_changes_text += f"• {phone}: {'Accept' if status else 'Not Accept'}\n"
                
                with st.expander("Show pending changes"):
                    st.text(pending_changes_text)
                
                # Add the save button
                if st.button("Save All Changes", key="save_suspicious_changes"):
                    try:
                        # Update the status for all pending changes
                        updated_df = suspicious_df.clone()
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        for phone, status in st.session_state.pending_suspicious_changes.items():
                            # Create a mask for each row to update
                            mask = pl.col("nums") == phone
                            
                            # Update the status and timestamp for matching rows
                            updated_df = updated_df.with_columns([
                                pl.when(mask)
                                  .then(pl.lit("TRUE" if status else "FALSE"))
                                  .otherwise(pl.col("accepted"))
                                  .alias("accepted"),
                                pl.when(mask)
                                  .then(pl.lit(now))
                                  .otherwise(pl.col("lastUpdated"))
                                  .alias("lastUpdated")
                            ])
                        
                        # Update the sheet
                        if update_sheet_data("suspicious_nums", updated_df):
                            st.success(f"Updated status for {len(st.session_state.pending_suspicious_changes)} phone numbers")
                            
                            # Clear pending changes and cached data
                            st.session_state.pending_suspicious_changes = {}
                            st.session_state.cached_suspicious_df = None
                            
                            # Add slight delay to avoid immediate refresh
                            time.sleep(0.5)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error updating statuses: {str(e)}")

    # ----- Late Numbers Tab -----
    with tab3:
        st.header("Late Numbers")
        st.info("These are patients who were sent a WhatsApp questionnaire but did not answer within the time threshold.")
        
        # Check if there's data
        if late_df.is_empty():
            st.warning("No late numbers found.")
        else:
            # Add human-readable time ago column
            if 'sentTime' in late_df.columns:
                late_df = late_df.with_columns(
                    pl.col('sentTime').map_elements(format_time_ago, return_dtype=pl.Utf8).alias('Time Ago')
                )
                
            # Filter options - use session state to persist filter choice
            st.subheader("Filter Options")
            # Make sure we're using the session state value directly, not through a variable
            show_accepted = st.checkbox("Show Accepted Numbers", 
                                      value=st.session_state.show_accepted_late, 
                                      key="late_filter",
                                      on_change=toggle_show_accepted_late)
            
            # Apply filters - done in memory, no API calls
            filtered_df = late_df
            if not show_accepted:
                filtered_df = late_df.filter(
                    ~pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't'])
                )
            
            # Apply any pending changes to the display dataframe (doesn't change original data)
            if st.session_state.pending_late_changes:
                # Create a copy of the dataframe to apply pending changes
                display_copy = filtered_df.clone()
                
                # Create a modified version with pending changes applied
                for phone, new_status in st.session_state.pending_late_changes.items():
                    # Create a mask for the row to update
                    mask = pl.col("nums") == phone
                    # Update the status in the display copy
                    display_copy = display_copy.with_columns(
                        pl.when(mask)
                          .then(pl.lit(new_status))
                          .otherwise(pl.col("accepted"))
                          .alias("accepted")
                    )
                
                # Use the modified dataframe for display
                filtered_df = display_copy
            
            # Create a copy for display with better column names
            display_df = filtered_df.clone()
            if 'nums' in display_df.columns:
                display_df = display_df.rename({
                    'nums': 'Phone Number',
                    'sentTime': 'WhatsApp Sent',
                    'hoursLate': 'Hours Late',
                    'lastUpdated': 'Last Reviewed',
                    'accepted': 'Accepted'
                })
            
                # Reorder columns for better display
                column_order = ['Phone Number', 'WhatsApp Sent', 'Time Ago', 'Hours Late', 'Last Reviewed', 'Accepted']
                available_columns = [col for col in column_order if col in display_df.columns]
                display_df = display_df.select(available_columns)
                
            # Display data table with better column names
            st.subheader(f"Late Numbers ({filtered_df.height} entries)")
            
            # Convert to pandas for data editor
            pandas_df = display_df.to_pandas()
            
            # Show dataframe
            st.dataframe(pandas_df, use_container_width=True)
            
            # Add a selection mechanism for editing - similar to suspicious tab
            st.subheader("Mark Numbers as Accepted")
            
            # Create two columns for layout
            col1, col2 = st.columns([1, 3])
            
            # Let user select which number to update
            with col1:
                if 'Phone Number' in pandas_df.columns:
                    phone_numbers = pandas_df['Phone Number'].tolist()
                    selected_phone = st.selectbox(
                        "Select Phone Number", 
                        options=phone_numbers,
                        key="late_phone_select"
                    )
                    
                    # Get the row index from the display dataframe
                    selected_idx = pandas_df[pandas_df['Phone Number'] == selected_phone].index[0]
                    
                    # Get current acceptance status, checking pending changes first
                    if selected_phone in st.session_state.pending_late_changes:
                        current_status = st.session_state.pending_late_changes[selected_phone]
                    else:
                        current_status = pandas_df.loc[selected_idx, 'Accepted']
                    
                    # Create the checkbox for accepting/rejecting
                    new_status = st.checkbox("Mark as Accepted", 
                                           value=current_status,
                                           key=f"late_accept_{selected_idx}")
                    
                    # Store the change in pending changes if it differs from current status
                    if new_status != current_status:
                        handle_late_status_change(selected_phone, new_status)
                        st.rerun()  # Refresh to show the changes in the table
            
            # Show details about the selected number
            with col2:
                if 'Phone Number' in pandas_df.columns and selected_phone:
                    # Get details for the selected phone
                    row = pandas_df[pandas_df['Phone Number'] == selected_phone].iloc[0]
                    
                    st.markdown(f"### Details for {selected_phone}")
                    st.markdown(f"**WhatsApp Sent:** {row.get('WhatsApp Sent', 'N/A')}")
                    st.markdown(f"**Time Ago:** {row.get('Time Ago', 'N/A')}")
                    st.markdown(f"**Hours Late:** {row.get('Hours Late', 'N/A')}")
                    st.markdown(f"**Last Reviewed:** {row.get('Last Reviewed', 'N/A')}")
                    st.markdown(f"**Current Status:** {'Accepted' if row.get('Accepted', False) else 'Not Accepted'}")
            
            # Add a save button at the bottom to commit all pending changes
            if st.session_state.pending_late_changes:
                st.divider()
                st.write(f"**{len(st.session_state.pending_late_changes)}** phone numbers have pending status changes.")
                
                # Display the pending changes
                pending_changes_text = ""
                for phone, status in st.session_state.pending_late_changes.items():
                    pending_changes_text += f"• {phone}: {'Accept' if status else 'Not Accept'}\n"
                
                with st.expander("Show pending changes"):
                    st.text(pending_changes_text)
                
                # Add the save button
                if st.button("Save All Changes", key="save_late_changes"):
                    try:
                        # Update the status for all pending changes
                        updated_df = late_df.clone()
                        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        for phone, status in st.session_state.pending_late_changes.items():
                            # Create a mask for each row to update
                            mask = pl.col("nums") == phone
                            
                            # Update the status and timestamp for matching rows
                            updated_df = updated_df.with_columns([
                                pl.when(mask)
                                  .then(pl.lit("TRUE" if status else "FALSE"))
                                  .otherwise(pl.col("accepted"))
                                  .alias("accepted"),
                                pl.when(mask)
                                  .then(pl.lit(now))
                                  .otherwise(pl.col("lastUpdated"))
                                  .alias("lastUpdated")
                            ])
                        
                        # Update the sheet
                        if update_sheet_data("late_nums", updated_df):
                            st.success(f"Updated status for {len(st.session_state.pending_late_changes)} phone numbers")
                            
                            # Clear pending changes and cached data
                            st.session_state.pending_late_changes = {}
                            st.session_state.cached_late_df = None
                            
                            # Add slight delay to avoid immediate refresh
                            time.sleep(0.5)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error updating statuses: {str(e)}")

    # Add a footer with helpful information
    st.divider()
    st.write("### About Alert Management")
    st.markdown("""
    This page helps you manage questionnaire-related alerts:

    - **Suspicious Numbers**: Patients who completed questionnaires but weren't properly identified in our system
    - **Late Numbers**: Patients who received WhatsApp questionnaires but haven't completed them within the expected timeframe

    Mark items as "Accepted" once you've reviewed them to stop receiving email alerts about them.
    """)
