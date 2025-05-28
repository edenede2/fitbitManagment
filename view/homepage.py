import streamlit as st
import datetime
from entity.User import User, UserRepository
from entity.Project import Project, ProjectRepository
from entity.Watch import Watch, WatchFactory
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from Decorators.congrates import congrats, welcome_returning_user
from model.config import get_secrets
from mitosheet.streamlit.v1 import spreadsheet as msp
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
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
import warnings
from typing import List, Dict, Any
from controllers.agGridHelper import aggrid_polars
# from streamlit_elements import elements, dashboard, mui, html
from controllers.agGridHelper import aggrid_polars

def display_homepage(user_email, user_role, user_project, spreadsheet: Spreadsheet) -> None:
    """
    Display the homepage with personalized content based on user's role and project
    """
    if user_email is None:
        st.warning("Please go to the app page and come back or refresh the page.")
    else:
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
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                timestamp = pd.to_datetime(timestamp)
        except:
            return timestamp
    
    # Check for future dates by year first
    now = pd.Timestamp.now()
    if timestamp.year > now.year:
        return f"Future({timestamp.strftime('%Y-%m-%d')})"
    
    delta = now - timestamp
    seconds = delta.total_seconds()
    
    # Handle near-future dates (same year but future time)
    if seconds < 0:
        return f"Soon({timestamp.strftime('%H:%M')})"
    
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
            # Suppress warnings about format inference
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                timestamp = pd.to_datetime(timestamp)
        except:
            return "❓"
    
    # Convert to datetime64 for consistent time delta calculation
    try:
        now = pd.Timestamp.now().to_datetime64()
        timestamp = pd.to_datetime(timestamp).to_datetime64()
        
        # Handle future dates properly: check year first
        timestamp_year = pd.to_datetime(timestamp).year
        current_year = pd.to_datetime(now).year
        
        if timestamp_year > current_year:
            return "🔵"  # Blue circle for future years
        
        delta = now - timestamp
        # Convert numpy.timedelta64 to hours by dividing by 1 hour
        hours = delta / np.timedelta64(1, 'h')
        
        # Handle future dates
        if hours < 0:
            return "⏳"  # Hourglass for future time
        
        # Handle past dates as before
        if hours <= 3:
            return "✅"
        elif hours <= 12:
            return "🟡"
        elif hours <= 24:
            return "🟠"
        else:
            return "🔴"
    except Exception:
        return "❓"  # Return question mark for any conversion failures

def load_fitbit_sheet_data(spreadsheet:Spreadsheet) -> Dict[str, Any]:
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

def preprocess_dataframe_for_display(df):
    """Clean dataframe to make it Arrow-compatible for display"""
    processed_df = df.copy()
    
    # Identify columns that should be numeric
    potential_numeric_cols = [
        'lastBattaryVal', 'lastHRVal', 'lastStepsVal', 'lastSleepDur',
        'CurrentFailedSync', 'TotalFailedSync', 'CurrentFailedHR', 'TotalFailedHR',
        'CurrentFailedSleep', 'TotalFailedSleep', 'CurrentFailedSteps', 'TotalFailedSteps'
    ]
    
    # Process each column that exists in the dataframe
    for col in [c for c in potential_numeric_cols if c in processed_df.columns]:
        # Convert empty strings to None first
        processed_df[col] = processed_df[col].replace('', None)
        # Then convert column to appropriate type
        try:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
        except:
            pass  # Keep as is if conversion fails
    
    # Ensure datetime columns are properly formatted for display
    datetime_cols = ['lastCheck', 'lastSynced', 'lastBattary', 'lastHR', 
                    'lastSleepStartDateTime', 'lastSleepEndDateTime', 'lastSteps']
    
    for col in [c for c in datetime_cols if c in processed_df.columns]:
        if pd.api.types.is_datetime64_any_dtype(processed_df[col]):
            # Already datetime, no action needed
            pass
        else:
            # Try to convert to datetime
            try:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
            except:
                # If conversion fails, keep as is
                pass
    
    return processed_df

def display_fitbit_log_table(user_email, user_role, user_project, spreadsheet: Spreadsheet) -> None:
    """Display the Fitbit Log table with data from the FitbitLog sheet"""
    st.subheader("Fitbit Watch Status")
    
    with st.spinner("Loading Fitbit data..."):
        try:
            # Load the FitbitLog sheet
            fitbit_log_sheet = spreadsheet.get_sheet("FitbitLog", "log")
            
            # Get watch assignment info
            watch_mapping = load_fitbit_sheet_data(spreadsheet)
            
            fitbit_log_df = fitbit_log_sheet.to_dataframe(engine="polars")
            if fitbit_log_df.is_empty():
                st.warning("No Fitbit log data available.")
                return
            
            # 1) Fill null and empty "lastSynced" with a placeholder date
            fitbit_log_df = fitbit_log_df.with_columns(
                pl.when(pl.col('lastSynced').is_null() | (pl.col('lastSynced') == ''))
                .then(pl.lit("2000-01-01 00:00:00"))
                .otherwise(pl.col('lastSynced'))
                .alias('lastSynced')
            )

            # Cast to datetime after inserting placeholder
            fitbit_log_df = fitbit_log_df.with_columns(
                pl.col('lastSynced').cast(pl.Datetime, strict=False)
            )
            
            # Sort by lastCheck (most recent first)
            if 'lastCheck' in fitbit_log_df.columns:
                fitbit_log_df = fitbit_log_df.sort('lastCheck', descending=True)
            
            # Add student assignment and watch status information
            # Create a mapping dictionary for assigned students
            student_mapping = {key: value.get('student', '') for key, value in watch_mapping.items()}

            # Add assigned_student column using polars expressions
            fitbit_log_df = fitbit_log_df.with_columns([
                (pl.col("project") + "-" + pl.col("watchName"))
                .map_elements(lambda key: student_mapping.get(key, ''), return_dtype=pl.Utf8)
                .alias("assigned_student")
            ])
            fitbit_log_df = fitbit_log_df.with_columns(
                pl.col('assigned_student').cast(pl.Utf8)
            )

            # Create a mapping dictionary for active status
            active_mapping = {key: value.get('active', True) for key, value in watch_mapping.items()}

            # Add is_active column using polars expressions
            fitbit_log_df = fitbit_log_df.with_columns([
                (pl.col("project") + "-" + pl.col("watchName"))
                .map_elements(lambda key: active_mapping.get(key, True), return_dtype=pl.Boolean)
                .alias("is_active")
            ])

            # Ensure the column is of boolean type
            fitbit_log_df = fitbit_log_df.with_columns(
                pl.col("is_active").cast(pl.Boolean)
            )

            fitbit_log_df = fitbit_log_df.filter(
                pl.col('is_active') == True
            )
            
            # Filter based on user role and project
            if user_role.lower() == "admin":
                # Admin sees everything
                filtered_df = fitbit_log_df
            elif user_role.lower() == "manager":
                # Manager sees watches from their project
                filtered_df = fitbit_log_df.filter(pl.col('project') == user_project)
            else:
                # Student sees watches from their project, highlighting their own
                filtered_df = fitbit_log_df.filter(pl.col('project') == user_project)
            
            # Allow filtering by project for Admin
            if user_role.lower() == "admin":
                projects = sorted(fitbit_log_df['project'].unique())
                selected_projects = st.multiselect("Filter by Project:", projects, default=projects)
                if selected_projects:
                    filtered_df = filtered_df.filter(pl.col('project').is_in(selected_projects))
            
            # Get the latest record for each watch
            latest_df = filtered_df.sort("lastCheck", descending=True).unique(subset=["watchName"], keep="first")
            
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Watches", len(latest_df))
            with col2:
                st.metric("Active Watches", len(latest_df.filter(pl.col('is_active') == True)))
            with col3:
                low_battery = len(latest_df.filter(
                    pl.col('lastBattaryVal').cast(pl.Utf8).str.replace('%', '').cast(pl.Float64, strict=False) < 20
                ))
                st.metric("Low Battery", f"{low_battery}")

            
            # For students, show their assigned watch first
            if user_role.lower() == "student":
                my_watches = latest_df.filter(pl.col('assigned_student') == user_email)
                if not my_watches.is_empty():
                    st.subheader("My Assigned Watch")
                    for _, row in my_watches.iter_rows():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown(f"### {row['watchName']}")
                            st.markdown(f"**Project:** {row['project']}")
                        
                        with col2:
                            battery_val = row.get('lastBattaryVal', '')
                            if row.get('lastSynced') is None or pd.isna(row.get('lastSynced')):
                                last_sync = "Never"
                                sync_status = "❓"
                            else:    
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
            display_df = latest_df.clone()
            
            # 2) In the display DataFrame, show "No data" if value is the placeholder date
            if 'lastSynced' in display_df.columns:
                display_df = display_df.with_columns([
                    pl.col('lastSynced')
                    .map_elements(lambda x: (
                        "No data"
                        if str(x).startswith("2000-01-01")
                        else f"{time_status_indicator(x)} {format_time_ago_concise(x)}"
                    ), return_dtype=pl.Utf8)
                    .alias('Last Sync')
                ])
            def safe_int_convert(val):
                """Safely convert a value to int with error handling"""
                try:
                    if pd.isna(val) or val == '' or not val:
                        return 'N/A'
                    return int(float(val))  # Convert to float first for strings like '72.0'
                except (ValueError, TypeError):
                    return val

            # Fix heart rate display by properly handling NaN values and empty strings
            if 'lastHR' in display_df.columns and 'lastHRVal' in display_df.columns:
                display_df = display_df.with_columns([
                    pl.struct(['lastHR', 'lastHRVal'])
                    .map_elements(lambda row: (
                        f"{time_status_indicator(row['lastHR'])} " + 
                        (f"{safe_int_convert(row['lastHRVal'])} bpm" 
                         if row['lastHRVal'] is not None and row['lastHRVal'] != '' 
                         else "N/A")
                    ), return_dtype=pl.Utf8)
                    .alias('Heart Rate')
                ])
            
            # Calculate sleep duration directly from the timestamps
            display_df = display_df.with_columns([
                pl.struct(['lastSleepStartDateTime', 'lastSleepEndDateTime'])
                .map_elements(lambda row: calculate_sleep_duration(row['lastSleepStartDateTime'], row['lastSleepEndDateTime']), 
                             return_dtype=pl.Float64)
                .alias('calculated_sleep_dur')
            ])
            
            # Use calculated duration when available, fall back to stored duration
            display_df = display_df.with_columns([
                pl.struct(['lastSleepEndDateTime', 'calculated_sleep_dur', 'lastSleepDur'])
                .map_elements(lambda row: 
                    f"{time_status_indicator(row['lastSleepEndDateTime'])} " + 
                    (convert_min_to_hours(row['calculated_sleep_dur']) 
                     if row['calculated_sleep_dur'] is not None 
                     else convert_min_to_hours(row['lastSleepDur']))
                , return_dtype=pl.Utf8)
                .alias('Sleep')
            ])
            
            # Ensure steps are properly formatted with safe integer conversion
            if 'lastSteps' in display_df.columns and 'lastStepsVal' in display_df.columns:
                display_df = display_df.with_columns([
                    pl.struct(['lastSteps', 'lastStepsVal'])
                    .map_elements(lambda row: 
                        f"{time_status_indicator(row['lastSteps'])} " + 
                        (f"{safe_int_convert(row['lastStepsVal'])}" 
                         if row['lastStepsVal'] is not None and row['lastStepsVal'] != '' 
                         else "N/A")
                    , return_dtype=pl.Utf8)
                    .alias('Steps')
                ])
            
            # Prepare battery column for ProgressColumn with better error handling
            if 'lastBattaryVal' in display_df.columns:
                # Convert to numeric values and handle NaN and empty strings
                display_df = display_df.with_columns([
                    pl.col('lastBattaryVal')
                    .map_elements(lambda x: 
                        0 if x is None or x == '' or not x 
                        else float(x) / 100.0
                    , return_dtype=pl.Float64)
                    .alias('Battery Level')
                ])
            
            # Define columns for display
            display_columns = ['watchName', 'project', 'Battery Level', 'Heart Rate', 'Sleep', 'Steps','lastSynced']
            display_columns = [col for col in display_columns if col in display_df.columns]
            
            # Use column config to define column formats
            column_config = {
                "watchName": "Watch Name",
                "project": "Project",
                "lastSynced": st.column_config.DatetimeColumn(
                    "Last Time Synced",
                    format="YYYY-MM-DD HH:mm",
                    help="Last time the server synced with the watch",
                ),
                "is_active": st.column_config.CheckboxColumn(
                    "Active",
                    help="Is the watch currently assigned to a student?",
                    disabled=True
                ),
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
                assigned_watches = display_df.filter(pl.col('assigned_student') == user_email).select('watchName').to_series().to_list()
            else:
                assigned_watches = []
            
            display_df = display_df.filter(pl.col('Last Sync').is_not_null()).filter(pl.col('is_active') == True)
            # Display using st.dataframe with column config
            # st.dataframe(
            #     display_df[display_columns],
            #     column_config=column_config,
            #     use_container_width=True,
            #     height=min(35 * len(display_df) + 38, 600),
            #     hide_index=True
            # )

            # Define column definitions with specific filter types
            column_defs = []
            for col in display_columns:
                # Get display name from column_config if available
                display_name = column_config.get(col, col)
                if isinstance(display_name, dict) and 'title' in display_name:
                    display_name = display_name['title']
                # elif isinstance(display_name, st.column_config._ColumnConfig):
                #     display_name = display_name.label or col
                
                column_def = {
                    "headerName": display_name,
                    "field": col,
                    "sortable": True,
                    "resizable": True
                }
                
                # Add special filter types based on column content
                if col == "Battery Level":
                    column_def["filter"] = "agNumberColumnFilter"
                    column_def["filterParams"] = {
                        "allowedCharPattern": "\\d\\-\\.",
                        "numberParser": True
                    }
                elif col in ["Heart Rate", "Steps"]:
                    column_def["filter"] = "agNumberColumnFilter"
                elif col == "is_active":
                    column_def["filter"] = "agSetColumnFilter"
                elif col == "watchName" or col == "project":
                    column_def["filter"] = "agTextColumnFilter"
                    column_def["filterParams"] = {
                        "filterOptions": ["contains", "notContains", "equals", "notEqual", "startsWith", "endsWith"],
                        "defaultOption": "contains"
                    }
                else:
                    column_def["filter"] = "agTextColumnFilter"
                
                column_defs.append(column_def)

            # Create comprehensive grid options
            grid_options = {
                "columnDefs": column_defs,
                "defaultColDef": {
                    "flex": 1,
                    "minWidth": 120,
                    "filter": True,
                    "floatingFilter": True,
                    "sortable": True,
                    "resizable": True
                },
                "pagination": True,
                "paginationPageSize": 20,
                "enableRangeSelection": True,
                "rowSelection": "multiple"
            }

            gd = GridOptionsBuilder.from_dataframe(
                    display_df[display_columns].to_pandas()
            )
            for col in display_columns:
                if col in ("Battery Level", "Heart Rate", "Steps"):
                    gd.configure_column(col, filter="agNumberColumnFilter")
                elif col == "is_active":
                    gd.configure_column(col, filter="agSetColumnFilter")
                else:
                    gd.configure_column(col, filter="agTextColumnFilter")


            aggrid_polars(
                display_df[display_columns],
            )
            # Render the AgGrid with improved options
            # AgGrid(
            #     display_df[display_columns].to_pandas(),
            #     gridOptions=grid_options,      # ← use this, not the blank builder
            #     fit_columns_on_grid_load=True,
            #     theme="streamlit",
            # )
            
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
                detail_df = latest_df.select(available_cols).clone()
                
                # Format datetime columns for display
                for col in ['lastCheck', 'lastSynced']:
                    if col in detail_df.columns:
                        try:
                            # Safer approach to handle NaT values - avoid strftime on null values
                            detail_df = detail_df.with_columns([
                                pl.when(pl.col(col).is_null())
                                .then(pl.lit("N/A"))
                                .otherwise(
                                    # Convert directly to string without using strftime
                                    pl.col(col).cast(pl.Utf8)
                                )
                                .alias(col)
                            ])
                        except Exception as e:
                            # If formatting fails, keep as is
                            pass
                
                # Display as dataframe
                # st.dataframe(detail_df, use_container_width=True)
                
                # gd = GridOptionsBuilder.from_dataframe(
                #     detail_df.to_pandas()
                # )
                # configure_filters_from_polars(gd, detail_df)
                # AgGrid(
                #     detail_df.to_pandas(),
                #     gridOptions=gd.build(),
                #     fit_columns_on_grid_load=True,
                #     theme="streamlit"
                # )
                # Use AgGrid for better filtering and sorting
                edited_df, grid_response = aggrid_polars(detail_df)
                # Show complete raw data from the sheet
                st.subheader("Complete Raw Data")
                if user_role == "Admin":
                    # Show all data for Admin
                    # st.dataframe(fitbit_log_df.to_pandas())
                    # gd = GridOptionsBuilder.from_dataframe(
                    #     fitbit_log_df.to_pandas()
                    # )
                    edited_flog, grid_response_flog = aggrid_polars(fitbit_log_df)
                    # AgGrid(
                    #     fitbit_log_df.to_pandas(),
                    #     gridOptions=gd.build(),
                    #     fit_columns_on_grid_load=True,
                    #     theme="streamlit"
                    # )
                    # Add download button for the raw data
                    csv = fitbit_log_df.write_csv().encode('utf-8')
                    st.download_button(
                        label="Download Raw Data as CSV",
                        data=csv,
                        file_name=f"fitbit_log_data_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    # Show filtered data for others
                    fitbit_log_df = fitbit_log_df.filter(pl.col('project') == user_project)
                    # st.dataframe(fitbit_log_df.to_pandas())
                    # gd = GridOptionsBuilder.from_dataframe(
                    #     fitbit_log_df.to_pandas()
                    # )
                    edited_flog, grid_response_flog = aggrid_polars(fitbit_log_df)
                    # AgGrid(
                    #     fitbit_log_df.to_pandas(),
                    #     gridOptions=gd.build(),
                    #     fit_columns_on_grid_load=True,
                    #     theme="streamlit"
                    # )
                    # Add download button for the filtered data
                    csv = fitbit_log_df.write_csv().encode('utf-8')
                    st.download_button(
                        label="Download Filtered Data as CSV",
                        data=csv,
                        file_name=f"fitbit_log_data_{user_project}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )

            # Add visualization section
            st.subheader("Visualizations")
            
            # Let user select a watch to view historical data
            watch_options = sorted(fitbit_log_df['watchName'].unique().to_list())
            if watch_options:
                selected_watch = st.selectbox("Select Watch for History:", watch_options)
                
                # Get historical data for the selected watch - get all records, not just latest
                watch_history = filtered_df.filter(pl.col('watchName') == selected_watch).sort('lastCheck')
                
                # Add debug info to help troubleshoot visualization issues
                st.write(f"Found {watch_history.height} historical records for {selected_watch}")
                
                if not watch_history.is_empty():
                    # Create tabs for different metrics
                    tab1, tab2, tab3, tab4 = st.tabs(["Battery", "Heart Rate", "Steps", "Sleep"])
                    
                    with tab1:
                        # Clean and convert battery values
                        # Handle both string and numeric types for battery values
                        battery_df = watch_history.with_columns(
                            pl.when(pl.col('lastBattaryVal').cast(pl.Utf8).str.contains('%'))
                            .then(
                                pl.col('lastBattaryVal')
                                .cast(pl.Utf8)
                                .str.replace('%', '')
                                .cast(pl.Float64, strict=False)
                            )
                            .otherwise(pl.col('lastBattaryVal').cast(pl.Float64, strict=False))
                            .alias('battery_num')
                        ).select(['lastCheck', 'battery_num']).drop_nulls()
                        
                        st.write(f"Battery data points: {battery_df.height}")
                        if not battery_df.is_empty():
                            # Ensure data is properly sorted by time
                            battery_df = battery_df.sort('lastCheck')
                            
                            # Convert to pandas for plotly compatibility
                            battery_pd_df = battery_df.to_pandas()
                            fig = px.line(battery_pd_df, x='lastCheck', y='battery_num', 
                                         title=f"Battery History - {selected_watch}",
                                         labels={'lastCheck': 'Time', 'battery_num': 'Battery Level (%)'},
                                         range_y=[0, 100])
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No battery data available for this watch")
                    
                    with tab2:
                        # Convert HR values to numeric with better handling
                        hr_df = watch_history.with_columns(
                            pl.col('lastHRVal').cast(pl.Float64, strict=False).alias('hr_num')
                        ).select(['lastCheck', 'hr_num']).drop_nulls()
                        
                        st.write(f"Heart rate data points: {hr_df.height}")
                        if not hr_df.is_empty():
                            # Ensure data is properly sorted by time
                            hr_df = hr_df.sort('lastCheck')
                            
                            # Convert to pandas for plotly compatibility
                            hr_pd_df = hr_df.to_pandas()
                            fig = px.line(hr_pd_df, x='lastCheck', y='hr_num', 
                                         title=f"Heart Rate History - {selected_watch}",
                                         labels={'lastCheck': 'Time', 'hr_num': 'Heart Rate (bpm)'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No heart rate data available for this watch")
                    
                    with tab3:
                        # Clean and convert steps values
                        steps_df = watch_history.with_columns(
                            pl.col('lastStepsVal').cast(pl.Float64, strict=False).alias('steps_num')
                        ).select(['lastCheck', 'steps_num']).drop_nulls()
                        
                        st.write(f"Steps data points: {steps_df.height}")
                        if not steps_df.is_empty():
                            # Ensure data is properly sorted by time
                            steps_df = steps_df.sort('lastCheck')
                            
                            # Convert to pandas for plotly compatibility
                            steps_pd_df = steps_df.to_pandas()
                            fig = px.bar(steps_pd_df, x='lastCheck', y='steps_num', 
                                        title=f"Steps History - {selected_watch}",
                                        labels={'lastCheck': 'Time', 'steps_num': 'Steps'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No steps data available for this watch")
                    
                    with tab4:
                        # Try both the calculated sleep duration and the stored one
                        sleep_col = 'calculated_sleep_dur' if 'calculated_sleep_dur' in watch_history.columns else 'lastSleepDur'
                        sleep_df = watch_history.with_columns(
                            pl.col(sleep_col).cast(pl.Float64, strict=False).alias('sleep_min')
                        ).select(['lastCheck', 'sleep_min']).drop_nulls()
                        
                        st.write(f"Sleep data points: {sleep_df.height}")
                        if not sleep_df.is_empty():
                            # Ensure data is properly sorted by time
                            sleep_df = sleep_df.sort('lastCheck')
                            
                            # Convert to pandas for plotly compatibility
                            sleep_pd_df = sleep_df.to_pandas()
                            fig = px.bar(sleep_pd_df, x='lastCheck', y='sleep_min', 
                                        title=f"Sleep Duration History - {selected_watch}",
                                        labels={'lastCheck': 'Date', 'sleep_min': 'Sleep Duration (min)'})
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No sleep data available for this watch")
                    
                    # If all visualizations are empty, show the raw data
                    if (battery_df.height + hr_df.height + steps_df.height + sleep_df.height) == 0:
                        st.warning("No visualization data available. Here's the raw data for troubleshooting:")
                        # st.dataframe(watch_history.select(['lastCheck', 'lastBattaryVal', 'lastHRVal', 'lastStepsVal', 'lastSleepDur']).head(10))
                        # gd = GridOptionsBuilder.from_dataframe(
                        #     watch_history.select(['lastCheck', 'lastBattaryVal', 'lastHRVal', 'lastStepsVal', 'lastSleepDur']).to_pandas()
                        # )
                        # configure_filters_from_polars(gd, watch_history)
                        edited_df_wh, grid_response_wh = aggrid_polars( watch_history.select(['lastCheck', 'lastBattaryVal', 'lastHRVal', 'lastStepsVal', 'lastSleepDur']))
                        # AgGrid(
                        #     watch_history.select(['lastCheck', 'lastBattaryVal', 'lastHRVal', 'lastStepsVal', 'lastSleepDur']).to_pandas(),
                        #     gridOptions=gd.build(),
                        #     fit_columns_on_grid_load=True,
                        #     theme="streamlit"
                        # )
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

def calculate_sleep_duration(start_time, end_time):
    """Calculate sleep duration between two timestamps in minutes"""
    if pd.isna(start_time) or pd.isna(end_time):
        return None
    
    try:
        # Ensure both are datetime objects
        if isinstance(start_time, str):
            start_time = pd.to_datetime(start_time)
        if isinstance(end_time, str):
            end_time = pd.to_datetime(end_time)
        
        # Calculate duration in minutes
        delta = end_time - start_time
        minutes = delta.total_seconds() / 60
        
        # Return duration in minutes (should be positive)
        return abs(minutes)
    except Exception:
        return None
