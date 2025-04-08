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
if "cached_data" not in st.session_state:
    st.session_state.cached_data = {
        "total_answers": None,
        "suspicious": None,
        "late": None,
        "last_refresh": None
    }
if "edited_data" not in st.session_state:
    st.session_state.edited_data = {
        "total_answers": None,
        "suspicious": None,
        "late": None
    }

# ---- Functions for loading and updating sheet data ----

@st.cache_data(ttl=300)  # Cache for 5 minutes
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

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_total_answers(spreadsheet:Spreadsheet):
    """Load total answers from spreadsheet"""
    total_answers_sheet = spreadsheet.get_sheet("EMA", "EMA")
    df = total_answers_sheet.to_dataframe(engine="polars")
    
    # Convert accepted column to boolean if it exists
    if 'accepted' in df.columns:
        df = df.with_columns(
            pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
        )
        
    return df, total_answers_sheet

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_suspicious_numbers(spreadsheet:Spreadsheet):
    """Load suspicious numbers from spreadsheet"""
    suspicious_sheet = spreadsheet.get_sheet("suspicious_nums", "suspicious_nums")
    df = suspicious_sheet.to_dataframe(engine="polars")
    
    # Add column for verification if not exists
    if 'accepted' not in df.columns:
        df = df.with_columns(pl.lit(False).alias('accepted'))
    else:
        # Convert existing accepted values to boolean
        df = df.with_columns(
            pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
        )
        
    return df, suspicious_sheet

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_late_numbers(spreadsheet:Spreadsheet):
    """Load late numbers from spreadsheet"""
    late_sheet = spreadsheet.get_sheet("late_nums", "late_nums")
    df = late_sheet.to_dataframe(engine="polars")
    
    # Add column for verification if not exists
    if 'accepted' not in df.columns:
        df = df.with_columns(pl.lit(False).alias('accepted'))
    else:
        # Convert existing accepted values to boolean
        df = df.with_columns(
            pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't']).alias('accepted')
        )
        
    return df, late_sheet

# Add callbacks for data editor changes
def on_total_answers_change(edited_df):
    st.session_state.edited_data["total_answers"] = edited_df

def on_suspicious_change(edited_df):
    st.session_state.edited_data["suspicious"] = edited_df

def on_late_change(edited_df):
    st.session_state.edited_data["late"] = edited_df

# Create a main function that can be called from app.py
def show_alerts_management(user_email, user_role, user_project):
    """Main function to display the alerts management page - can be called from app.py"""
    # Page configuration
    st.title("📊 Alert Management")
    st.write("Review and manage patient questionnaire alerts.")

    # Create a refresh button in the sidebar
    with st.sidebar:
        refresh = st.button("↻ Refresh Data")
        last_refresh = st.session_state.cached_data["last_refresh"]
        if last_refresh:
            st.caption(f"Last refreshed: {last_refresh.strftime('%H:%M:%S')}")
    
    # Load data only if needed (first load or explicit refresh)
    if (st.session_state.cached_data["total_answers"] is None or 
        st.session_state.cached_data["suspicious"] is None or
        st.session_state.cached_data["late"] is None or
        refresh):
        
        with st.spinner("Loading data..."):
            spreadsheet = load_spreadsheet()
            total_answers_df, total_answers_sheet = load_total_answers(spreadsheet)
            suspicious_df, suspicious_sheet = load_suspicious_numbers(spreadsheet)
            late_df, late_sheet = load_late_numbers(spreadsheet)
            
            # Update cache
            st.session_state.cached_data["total_answers"] = (total_answers_df, total_answers_sheet)
            st.session_state.cached_data["suspicious"] = (suspicious_df, suspicious_sheet)
            st.session_state.cached_data["late"] = (late_df, late_sheet)
            st.session_state.cached_data["last_refresh"] = datetime.datetime.now()
            
            # Clear edited data on refresh
            st.session_state.edited_data["total_answers"] = None
            st.session_state.edited_data["suspicious"] = None
            st.session_state.edited_data["late"] = None
    else:
        # Use cached data
        total_answers_df, total_answers_sheet = st.session_state.cached_data["total_answers"]
        suspicious_df, suspicious_sheet = st.session_state.cached_data["suspicious"]
        late_df, late_sheet = st.session_state.cached_data["late"]

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
                    pl.col('endDate').map_elements(format_time_ago).alias('Time Ago')
                )
            else:
                display_df = total_answers_df
            
            # Filter options
            st.subheader("Filter Options")
            date_filter = st.date_input("Filter by date (from)", 
                                     value=datetime.datetime.now() - datetime.timedelta(days=7),
                                     max_value=datetime.datetime.now())
            
            # Apply filters if date column exists
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
                    
                    # Update the sheet
                    spreadsheet = load_spreadsheet()  # Get fresh connection
                    spreadsheet.update_sheet("EMA", updated_df)
                    GoogleSheetsAdapter.save(spreadsheet, "EMA")
                    st.success("Total answers data updated successfully!")
                    
                    # Refresh cache for this sheet
                    st.session_state.cached_data["total_answers"] = None
                    
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
                    pl.col('filledTime').map_elements(format_time_ago).alias('Time Ago')
                )
                
            # Filter options
            st.subheader("Filter Options")
            show_accepted = st.checkbox("Show Accepted Numbers", value=False, key="show_accepted_suspicious")
            
            # Apply filters
            filtered_df = suspicious_df
            if not show_accepted:
                filtered_df = suspicious_df.filter(
                    ~pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't'])
                )
            
            # Display data table with editor
            st.subheader(f"Suspicious Numbers ({filtered_df.height} entries)")
            
            # Create a copy for display with better column names
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
            
            # Convert to pandas for data editor
            if st.session_state.edited_data["suspicious"] is None:
                pandas_df = display_df.to_pandas()
            else:
                pandas_df = st.session_state.edited_data["suspicious"]
            
            # Configure the Accepted column as a checkbox
            column_config = {
                "Accepted": st.column_config.CheckboxColumn(
                    "Accepted", 
                    help="Mark number as accepted"
                )
            }
            
            # Disable Time Ago column since it's calculated
            disabled_cols = ["Time Ago"]
            
            edited_suspicious_df = st.data_editor(
                pandas_df, 
                key="suspicious_editor",
                disabled=disabled_cols,
                column_config=column_config,
                on_change=lambda: on_suspicious_change(st.session_state.suspicious_editor)
            )
            
            # Save changes button
            if st.button("Save Changes to Suspicious Numbers", key="save_suspicious"):
                try:
                    # Need to map edited data back to original column names
                    reverse_mapping = {
                        'Phone Number': 'nums',
                        'Questionnaire Filled': 'filledTime',
                        'Last Reviewed': 'lastUpdated',
                        'Accepted': 'accepted'
                    }
                    
                    # Convert back to polars
                    updated_df = pl.DataFrame(edited_suspicious_df)
                    
                    # First drop any columns that will be renamed to avoid duplicates
                    for display_name, original_name in reverse_mapping.items():
                        # First check if the display name exists in the dataframe
                        if display_name in updated_df.columns:
                            # Then check if the original name also exists (which would cause a duplicate)
                            if original_name in updated_df.columns:
                                # Drop the original name column to avoid duplicates after rename
                                updated_df = updated_df.drop(original_name)
                    
                    # Now do the renaming
                    for display_name, original_name in reverse_mapping.items():
                        if display_name in updated_df.columns:
                            updated_df = updated_df.rename({display_name: original_name})
                    
                    # Remove any columns not in the original schema
                    original_columns = suspicious_df.columns
                    columns_to_keep = [col for col in updated_df.columns if col in original_columns]
                    updated_df = updated_df.select(columns_to_keep)
                    
                    # Convert boolean accepted column back to TRUE/FALSE strings for Google Sheets
                    if 'accepted' in updated_df.columns:
                        updated_df = updated_df.with_columns(
                            pl.when(pl.col('accepted')).then(pl.lit("TRUE")).otherwise(pl.lit("FALSE")).alias('accepted')
                        )
                    
                    # Update timestamp for modified rows
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    updated_df = updated_df.with_columns(pl.lit(now).alias('lastUpdated'))
                    
                    # Update the sheet
                    spreadsheet = load_spreadsheet()  # Get fresh connection
                    spreadsheet.update_sheet("suspicious_nums", updated_df)
                    GoogleSheetsAdapter.save(spreadsheet, "suspicious_nums")
                    st.success("Suspicious numbers updated successfully!")
                    
                    # Refresh cache for this sheet
                    st.session_state.cached_data["suspicious"] = None
                    
                    # Clear edited data after successful save
                    st.session_state.edited_data["suspicious"] = None
                    
                    # Add slight delay to avoid immediate refresh
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving changes: {str(e)}")

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
                    pl.col('sentTime').map_elements(format_time_ago).alias('Time Ago')
                )
                
            # Filter options
            st.subheader("Filter Options")
            show_accepted = st.checkbox("Show Accepted Numbers", value=False, key="show_accepted_late")
            
            # Apply filters
            filtered_df = late_df
            if not show_accepted:
                filtered_df = late_df.filter(
                    ~pl.col('accepted').cast(str).str.to_lowercase().is_in(['true', 'yes', '1', 't'])
                )
            
            # Display data table with editor
            st.subheader(f"Late Numbers ({filtered_df.height} entries)")
            
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
            
            # Convert to pandas for data editor
            if st.session_state.edited_data["late"] is None:
                pandas_df = display_df.to_pandas()
            else:
                pandas_df = st.session_state.edited_data["late"]
            
            # Configure the Accepted column as a checkbox
            column_config = {
                "Accepted": st.column_config.CheckboxColumn(
                    "Accepted", 
                    help="Mark number as accepted"
                )
            }
            
            # Disable Time Ago column since it's calculated
            disabled_cols = ["Time Ago"]
            
            edited_late_df = st.data_editor(
                pandas_df, 
                key="late_editor",
                disabled=disabled_cols,
                column_config=column_config,
                on_change=lambda: on_late_change(st.session_state.late_editor)
            )
            
            # Save changes button
            if st.button("Save Changes to Late Numbers", key="save_late"):
                try:
                    # Need to map edited data back to original column names
                    reverse_mapping = {
                        'Phone Number': 'nums',
                        'WhatsApp Sent': 'sentTime',
                        'Hours Late': 'Hours Late',
                        'Last Reviewed': 'lastUpdated',
                        'Accepted': 'accepted'
                    }
                    
                    # Convert back to polars
                    updated_df = pl.DataFrame(edited_late_df)
                    
                    # First drop any columns that will be renamed to avoid duplicates
                    for display_name, original_name in reverse_mapping.items():
                        # First check if the display name exists in the dataframe
                        if display_name in updated_df.columns:
                            # Then check if the original name also exists (which would cause a duplicate)
                            if original_name in updated_df.columns:
                                # Drop the original name column to avoid duplicates after rename
                                updated_df = updated_df.drop(original_name)
                    
                    # Now do the renaming
                    for display_name, original_name in reverse_mapping.items():
                        if display_name in updated_df.columns:
                            updated_df = updated_df.rename({display_name: original_name})
                    
                    # Remove any columns not in the original schema
                    original_columns = late_df.columns
                    columns_to_keep = [col for col in updated_df.columns if col in original_columns]
                    updated_df = updated_df.select(columns_to_keep)
                    
                    # Convert boolean accepted column back to TRUE/FALSE strings for Google Sheets
                    if 'accepted' in updated_df.columns:
                        updated_df = updated_df.with_columns(
                            pl.when(pl.col('accepted')).then(pl.lit("TRUE")).otherwise(pl.lit("FALSE")).alias('accepted')
                        )
                    
                    # Update timestamp for modified rows
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    updated_df = updated_df.with_columns(pl.lit(now).alias('lastUpdated'))
                    
                    # Update the sheet
                    spreadsheet = load_spreadsheet()  # Get fresh connection
                    spreadsheet.update_sheet("late_nums", updated_df)
                    GoogleSheetsAdapter.save(spreadsheet, "late_nums")
                    st.success("Late numbers updated successfully!")
                    
                    # Refresh cache for this sheet
                    st.session_state.cached_data["late"] = None
                    
                    # Clear edited data after successful save
                    st.session_state.edited_data["late"] = None
                    
                    # Add slight delay to avoid immediate refresh
                    time.sleep(0.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving changes: {str(e)}")

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
    show_alerts_management(user_email, user_role, user_project)
