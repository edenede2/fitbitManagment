import streamlit as st
import datetime
from entity.User import User, UserRepository
from entity.Project import Project, ProjectRepository
from entity.Watch import Watch, WatchFactory
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from Decorators.congrates import congrats, welcome_returning_user
from model.config import get_secrets
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime, timedelta
import time
from entity.Sheet import GoogleSheetsAdapter, SheetsAPI, Spreadsheet
import altair as alt
import uuid
import plotly.express as px
import plotly.graph_objects as go
import re
# from streamlit_elements import elements, dashboard, mui, html

def display_homepage(user_email, user_role, user_project, spreadsheet: Spreadsheet) -> None:
    """
    Display the homepage with personalized content based on user's role and project
    """
    st.title(congrats(user_name=user_email.split('@')[0], user_role=user_role))
    st.write("Welcome to the Fitbit Management System dashboard.")
    
    # Display role-specific information
    if user_role == "Admin":
        st.info(f"You are logged in as an Administrator with access to all projects.")
    elif user_role == "Manager":
        st.info(f"You are logged in as a Manager for project: {user_project}")
    elif user_role == "Student":
        st.info(f"You are logged in as a Student assigned to project: {user_project}")
    else:
        st.info(f"You are logged in as a Guest with limited access.")
    
    # Display project overview
    st.subheader("Project Overview")
    
    # Add the Fitbit Log table
    display_fitbit_log_table(user_email, user_role, user_project, spreadsheet)
    
    # Add any additional homepage content here
    st.subheader("Quick Links")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("**Dashboard**")
        st.write("View detailed analytics and statistics.")
    with col2:
        st.write("**Fitbit Management**")
        st.write("Manage Fitbit devices and assignments.")
    with col3:
        st.write("**Alerts Configuration**")
        st.write("Configure alert thresholds and notifications.")

def render_battery_gauge(battery_level):
    """Render a battery level as a colored progress bar"""
    try:
        if battery_level is None or battery_level == "" or pd.isna(battery_level):
            return "No data"
        
        # Convert to numeric value
        battery = float(battery_level)
        
        # Determine color based on level
        if battery >= 80:
            color = "green"
        elif battery >= 50:
            color = "orange"
        else:
            color = "red"
            
        # Create HTML for progress bar
        html = f"""
        <div style="width:100%; background-color:#f0f0f0; border-radius:5px; height:20px;">
            <div style="width:{battery}%; background-color:{color}; height:20px; border-radius:5px; text-align:center; color:white; line-height:20px; font-size:12px;">
                {battery}%
            </div>
        </div>
        """
        return html
    except Exception as e:
        return f"Error: {str(e)}"

def format_time_ago(timestamp):
    """Format a datetime as a human-readable 'time ago' string"""
    if timestamp is None or pd.isna(timestamp):
        return "Never"
    
    # If timestamp is a string, try to parse it
    if isinstance(timestamp, str):
        try:
            timestamp = pd.to_datetime(timestamp)
        except:
            return timestamp
    
    now = pd.Timestamp.now()
    delta = now - timestamp
    
    seconds = delta.total_seconds()
    
    # Handle future dates
    if seconds < 0:
        return f"Future: {timestamp.strftime('%Y-%m-%d %H:%M')}"
    
    if seconds < 60:
        return f"{int(seconds)}s ago"
    elif seconds < 3600:
        return f"{int(seconds/60)}m ago"
    elif seconds < 86400:
        return f"{int(seconds/3600)}h ago"
    elif seconds < 604800:
        return f"{int(seconds/86400)}d ago"
    else:
        return timestamp.strftime("%Y-%m-%d")

def format_time_ago_concise(timestamp):
    """Format a datetime as a concise 'time ago' string with only the most significant unit"""
    if timestamp is None or pd.isna(timestamp):
        return "Never"
    
    # If timestamp is a string, try to parse it
    if isinstance(timestamp, str):
        try:
            timestamp = pd.to_datetime(timestamp)
        except:
            return timestamp
    
    now = pd.Timestamp.now()
    delta = now - timestamp
    
    seconds = delta.total_seconds()
    
    # Handle future dates
    if seconds < 0:
        return f"Future"
    
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds/60)}m"
    elif seconds < 86400:
        return f"{int(seconds/3600)}h"
    elif seconds < 604800:
        return f"{int(seconds/86400)}d"
    else:
        return timestamp.strftime("%Y-%m-%d")

def time_status_indicator(timestamp):
    """Return a status indicator based on time elapsed"""
    if timestamp is None or pd.isna(timestamp):
        return "❓"
    
    # If timestamp is a string, try to parse it
    if isinstance(timestamp, str):
        try:
            timestamp = pd.to_datetime(timestamp)
        except:
            return "❓"
    
    now = pd.Timestamp.now()
    delta = now - timestamp
    hours = delta.total_seconds() / 3600
    
    # Handle future dates
    if hours < 0:
        return "⏳"  # Hourglass for future time
    
    if hours <= 3:
        return "✅"
    elif hours <= 12:
        return "🟡"
    elif hours <= 24:
        return "🟠"
    else:
        return "🔴"

def load_fitbit_sheet_data(spreadsheet):
    """Load data from the Fitbit sheet to identify watch assignments"""
    try:
        # Get the fitbit sheet
        fitbit_sheet = spreadsheet.get_sheet("fitbit", "fitbit")
        
        # Map watch names to their assigned students
        watch_mapping = {}
        for item in fitbit_sheet.data:
            watch_name = item.get("name", "")
            project_name = item.get("project", "")
            is_active = str(item.get("isActive", "")).lower() not in ["false", "0", "no", "n", ""]
            
            # Create key as project-watchName
            key = f"{project_name}-{watch_name}"
            
            watch_mapping[key] = {
                "student": item.get("currentStudent", ""),
                "active": is_active
            }
            
        return watch_mapping
    except Exception as e:
        st.error(f"Error loading Fitbit sheet data: {e}")
        return {}

def display_fitbit_log_table(user_email, user_role, user_project, spreadsheet):
    """Display the Fitbit Log table with data from the FitbitLog sheet"""
    st.subheader("Fitbit Watch Status")
    
    with st.spinner("Loading Fitbit data..."):
        try:
            # Load the FitbitLog sheet
            fitbit_log_sheet = spreadsheet.get_sheet("FitbitLog", "log")
            
            # Get watch assignment info
            watch_mapping = load_fitbit_sheet_data(spreadsheet)
            
            if not fitbit_log_sheet.data:
                st.warning("No Fitbit log data available.")
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(fitbit_log_sheet.data)
            
            # Store original raw data before processing for display in expander
            raw_df = df.copy()
            
            # Add debugging info for lastSynced column
            # st.write("Debug - Original date formats in lastSynced column:")
            if 'lastSynced' in df.columns:
                unique_date_formats = df['lastSynced'].dropna().unique()[:5]  # Show first 5 unique values
                # st.code(str(unique_date_formats))
            
            # Define common date formats to try
            date_formats = [
                '%Y-%m-%dT%H:%M:%S.%f',  # ISO format with microseconds
                '%Y-%m-%dT%H:%M:%S',     # ISO format without microseconds
                '%Y-%m-%d %H:%M:%S',     # Standard datetime format
                '%Y-%m-%d',              # Just date
                '%m/%d/%Y %H:%M:%S',     # US format
                '%d/%m/%Y %H:%M:%S'      # European format
            ]
            
            # Convert datetime columns with specified formats to avoid warnings
            datetime_cols = ['lastCheck', 'lastSynced', 'lastBattary', 'lastHR', 
                            'lastSleepStartDateTime', 'lastSleepEndDateTime', 'lastSteps']
            
            for col in datetime_cols:
                if col in df.columns:
                    # Try each format in order until one works
                    parsed = None
                    for fmt in date_formats:
                        try:
                            # Try to parse with the specific format
                            temp_series = pd.to_datetime(df[col], format=fmt, errors='coerce')
                            # Check if we got some non-null values
                            if not temp_series.isna().all():
                                parsed = temp_series
                                break
                        except:
                            continue
                    
                    # If none of the formats worked, fall back to the default parser
                    if parsed is None:
                        try:
                            # Fall back to dateutil parser but suppress warnings
                            import warnings
                            with warnings.catch_warnings():
                                warnings.simplefilter("ignore")
                                parsed = pd.to_datetime(df[col], errors='coerce')
                        except Exception as e:
                            st.warning(f"Error parsing {col}: {str(e)}")
                            continue
                    
                    # Update the column if we got some valid dates
                    if parsed is not None and not parsed.isna().all():
                        df[col] = parsed
                    else:
                        # If all became NaT, keep original values
                        st.warning(f"Failed to parse dates in {col} column")
            
            # After parsing, check if lastSynced column has valid dates
            if 'lastSynced' in df.columns:
                valid_dates = df['lastSynced'].notna().sum()
                total_rows = len(df)
                # st.write(f"Successfully parsed {valid_dates} out of {total_rows} dates in lastSynced column")
                
                # If we have very few valid dates, try the original string values for display
                if valid_dates < total_rows * 0.5:  # If less than 50% parsed successfully
                    st.warning("Poor date parsing rate. Using original string values for lastSynced.")
                    df['lastSynced'] = raw_df['lastSynced']  # Restore original values
            
            # Sort by lastCheck (most recent first)
            if 'lastCheck' in df.columns:
                df = df.sort_values('lastCheck', ascending=False)
            
            # Add student assignment and watch status information
            df['assigned_student'] = df.apply(
                lambda row: watch_mapping.get(f"{row.get('project')}-{row.get('watchName')}", {}).get('student', ''),
                axis=1
            )
            
            df['is_active'] = df.apply(
                lambda row: watch_mapping.get(f"{row.get('project')}-{row.get('watchName')}", {}).get('active', True),
                axis=1
            )
            
            # Filter based on user role and project
            if user_role.lower() == "admin":
                # Admin sees everything
                filtered_df = df
            elif user_role.lower() == "manager":
                # Manager sees watches from their project
                filtered_df = df[df['project'] == user_project]
            else:
                # Student sees watches from their project, highlighting their own
                filtered_df = df[df['project'] == user_project]
            
            # Allow filtering by project for Admin
            if user_role.lower() == "admin":
                projects = sorted(df['project'].unique())
                selected_projects = st.multiselect("Filter by Project:", projects, default=projects)
                if selected_projects:
                    filtered_df = filtered_df[filtered_df['project'].isin(selected_projects)]
            
            # Get the latest record for each watch
            latest_df = filtered_df.sort_values('lastCheck', ascending=False).drop_duplicates('watchName')
            
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Watches", len(latest_df))
            with col2:
                st.metric("Active Watches", len(latest_df[latest_df['is_active'] == True]))
            with col3:
                low_battery = len(latest_df[latest_df['lastBattaryVal'].astype(str).str.replace('%', '').astype(float, errors='ignore') < 20])
                st.metric("Low Battery", f"{low_battery}")
            with col4:
                # Count watches not synced in last 24 hours
                not_synced = latest_df[latest_df['lastSynced'] < (pd.Timestamp.now() - pd.Timedelta(hours=24))].shape[0]
                st.metric("Not Synced (24h)", f"{not_synced}")
            
            # For students, show their assigned watch first
            if user_role.lower() == "student":
                my_watches = latest_df[latest_df['assigned_student'] == user_email]
                if not my_watches.empty:
                    st.subheader("My Assigned Watch")
                    for _, row in my_watches.iterrows():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown(f"### {row['watchName']}")
                            st.markdown(f"**Project:** {row['project']}")
                        
                        with col2:
                            battery_val = row.get('lastBattaryVal', '')
                            last_sync = format_time_ago(row.get('lastSynced'))
                            sync_status = time_status_indicator(row.get('lastSynced'))
                            
                            st.markdown(f"**Battery:** {render_battery_gauge(battery_val)}", unsafe_allow_html=True)
                            st.markdown(f"**Last Synced:** {sync_status} {last_sync}")
                            
                            # Show heart rate and steps
                            hr_val = row.get('lastHRVal', 'N/A')
                            steps_val = row.get('lastStepsVal', 'N/A')
                            st.markdown(f"**Heart Rate:** {hr_val} bpm | **Steps:** {steps_val}")
                            
                            # Show sleep data if available
                            sleep_start = row.get('lastSleepStartDateTime')
                            sleep_end = row.get('lastSleepEndDateTime')
                            sleep_dur = row.get('lastSleepDur', 'N/A')
                            
                            if pd.notna(sleep_start) and pd.notna(sleep_end):
                                st.markdown(f"**Last Sleep:** {sleep_start.strftime('%m/%d %H:%M')} to {sleep_end.strftime('%m/%d %H:%M')} ({sleep_dur} min)")
            
            # Main watch table
            st.subheader("All Watches Overview")
            
            # Create a copy of the dataframe for display
            display_df = latest_df.copy()
            
            # Format columns for display with concise time
            if 'lastSynced' in display_df.columns:
                display_df['Last Sync'] = display_df.apply(
                    lambda row: f"{time_status_indicator(row['lastSynced'])} {format_time_ago_concise(row['lastSynced'])}", 
                    axis=1
                )
            
            if 'lastHR' in display_df.columns and 'lastHRVal' in display_df.columns:
                # Fix heart rate display by properly handling NaN values and ensuring string conversion
                display_df['Heart Rate'] = display_df.apply(
                    lambda row: f"{time_status_indicator(row['lastHR'])} {'' if pd.isna(row.get('lastHRVal')) else str(int(row.get('lastHRVal'))) + ' bpm'}", 
                    axis=1
                )
            
            # Format sleep duration to hours with 2 decimal places
            if 'lastSleepEndDateTime' in display_df.columns and 'lastSleepDur' in display_df.columns:
                display_df['Sleep'] = display_df.apply(
                    lambda row: f"{time_status_indicator(row['lastSleepEndDateTime'])} {convert_min_to_hours(row.get('lastSleepDur'))}", 
                    axis=1
                )
            
            if 'lastSteps' in display_df.columns and 'lastStepsVal' in display_df.columns:
                # Ensure steps are properly formatted as integers when available
                display_df['Steps'] = display_df.apply(
                    lambda row: f"{time_status_indicator(row.get('lastSteps'))} {'' if pd.isna(row.get('lastStepsVal')) else str(int(row.get('lastStepsVal')))}", 
                    axis=1
                )
            
            # Prepare battery column for ProgressColumn - ensure numeric values
            if 'lastBattaryVal' in display_df.columns:
                # Convert to numeric values and handle NaN
                display_df['Battery Level'] = pd.to_numeric(display_df['lastBattaryVal'], errors='coerce') / 100.0
                # Replace NaNs with 0 for the progress column
                display_df['Battery Level'] = display_df['Battery Level'].fillna(0)
            
            # Define columns for display
            display_columns = ['watchName', 'project', 'Battery Level', 'Last Sync', 'Heart Rate', 'Sleep', 'Steps']
            display_columns = [col for col in display_columns if col in display_df.columns]
            
            # Use column config to define column formats
            column_config = {
                "watchName": "Watch Name",
                "project": "Project",
                "Battery Level": st.column_config.ProgressColumn(
                    "Battery",
                    help="Battery level of the watch",
                    format="percent",
                    min_value=0,
                    max_value=1.0
                ),
                "Last Sync": "Last Sync",
                "Heart Rate": "Heart Rate",
                "Sleep": "Sleep Duration",
                "Steps": "Steps"
            }
            
            # Highlight rows where the watch is assigned to current user
            if user_role.lower() == "student":
                assigned_watches = display_df[display_df['assigned_student'] == user_email]['watchName'].tolist()
            else:
                assigned_watches = []
            
            # Display using st.dataframe with column config
            st.dataframe(
                display_df[display_columns],
                column_config=column_config,
                use_container_width=True,
                height=min(35 * len(display_df) + 38, 600),
                hide_index=True
            )
            
            # Add expandable section with detailed view
            with st.expander("View Detailed Data"):
                # First show the filtered view with key columns
                st.subheader("Filtered Data View")
                detail_cols = ['watchName', 'project', 'lastCheck', 'lastSynced', 
                              'lastBattaryVal', 'lastHRVal', 'lastStepsVal',
                              'CurrentFailedSync', 'TotalFailedSync',
                              'CurrentFailedHR', 'TotalFailedHR',
                              'CurrentFailedSleep', 'TotalFailedSleep',
                              'CurrentFailedSteps', 'TotalFailedSteps']
                
                # Select columns that actually exist in the dataframe
                available_cols = [col for col in detail_cols if col in latest_df.columns]
                detail_df = latest_df[available_cols].copy()
                
                # Format datetime columns for display
                for col in ['lastCheck', 'lastSynced']:
                    if col in detail_df.columns:
                        detail_df[col] = detail_df[col].dt.strftime('%Y-%m-%d %H:%M')
                
                # Display as dataframe with student assignment highlighting
                st.dataframe(detail_df, use_container_width=True)
                
                # Show complete raw data from the sheet
                st.subheader("Complete Raw Data")
                st.dataframe(raw_df, use_container_width=True)
                
                # Add download button for the raw data
                csv = raw_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Raw Data as CSV",
                    data=csv,
                    file_name=f"fitbit_log_data_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            # Add visualization section
            st.subheader("Visualizations")
            
            # Let user select a watch to view historical data
            watch_options = sorted(filtered_df['watchName'].unique())
            if watch_options:
                selected_watch = st.selectbox("Select Watch for History:", watch_options)
                
                # Get historical data for the selected watch
                watch_history = filtered_df[filtered_df['watchName'] == selected_watch].sort_values('lastCheck')
                
                if not watch_history.empty:
                    # Create tabs for different metrics
                    tab1, tab2, tab3, tab4 = st.tabs(["Battery", "Heart Rate", "Steps", "Sleep"])
                    
                    with tab1:
                        # Convert battery values to numeric
                        watch_history['battery_num'] = pd.to_numeric(watch_history['lastBattaryVal'], errors='coerce')
                        battery_df = watch_history[['lastCheck', 'battery_num']].dropna()
                        
                        if not battery_df.empty:
                            fig = px.line(battery_df, x='lastCheck', y='battery_num', 
                                         title=f"Battery History - {selected_watch}",
                                         labels={'lastCheck': 'Time', 'battery_num': 'Battery Level (%)'},
                                         range_y=[0, 100])
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No battery data available for this watch")
                    
                    with tab2:
                        # Convert HR values to numeric
                        watch_history['hr_num'] = pd.to_numeric(watch_history['lastHRVal'], errors='coerce')
                        hr_df = watch_history[['lastCheck', 'hr_num']].dropna()
                        
                        if not hr_df.empty:
                            fig = px.line(hr_df, x='lastCheck', y='hr_num', 
                                         title=f"Heart Rate History - {selected_watch}",
                                         labels={'lastCheck': 'Time', 'hr_num': 'Heart Rate (bpm)'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No heart rate data available for this watch")
                    
                    with tab3:
                        # Convert steps values to numeric
                        watch_history['steps_num'] = pd.to_numeric(watch_history['lastStepsVal'], errors='coerce')
                        steps_df = watch_history[['lastCheck', 'steps_num']].dropna()
                        
                        if not steps_df.empty:
                            fig = px.bar(steps_df, x='lastCheck', y='steps_num', 
                                        title=f"Steps History - {selected_watch}",
                                        labels={'lastCheck': 'Time', 'steps_num': 'Steps'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No steps data available for this watch")
                    
                    with tab4:
                        # Convert sleep duration to numeric
                        watch_history['sleep_min'] = pd.to_numeric(watch_history['lastSleepDur'], errors='coerce')
                        sleep_df = watch_history[['lastCheck', 'sleep_min']].dropna()
                        
                        if not sleep_df.empty:
                            fig = px.bar(sleep_df, x='lastCheck', y='sleep_min', 
                                        title=f"Sleep Duration History - {selected_watch}",
                                        labels={'lastCheck': 'Date', 'sleep_min': 'Sleep Duration (min)'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No sleep data available for this watch")
                else:
                    st.info(f"No historical data available for {selected_watch}")
            else:
                st.info("No watches available for visualization")
                
        except Exception as e:
            st.error(f"Error displaying Fitbit log data: {e}")
            # Add debugging info if needed
            st.exception(e)

def convert_min_to_hours(minutes_value):
    """Convert minutes to hours with 2 decimal places"""
    try:
        # Handle non-numeric values
        if minutes_value == 'N/A' or minutes_value is None or pd.isna(minutes_value):
            return 'N/A'
        
        # Convert to float and divide by 60
        minutes = float(minutes_value)
        hours = minutes / 60.0
        
        # Format with 2 decimal places
        return f"{hours:.2f} h"
    except (ValueError, TypeError):
        return f"{minutes_value}"
