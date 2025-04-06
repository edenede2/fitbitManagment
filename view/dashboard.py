import streamlit as st
from mitosheet.streamlit.v1 import spreadsheet
import pandas as pd
import polars as pl
import datetime
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os
import time
import functools
from datetime import timedelta

from controllers.project_controller import ProjectController
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from entity.Watch import Watch, WatchFactory

# Add a cache decorator for API calls
@st.cache_data(ttl=300)  # Cache for 5 minutes
def cached_get_watches(user_email, user_role, user_project):
    project_controller = ProjectController()
    if user_role == "Admin":
        return project_controller.get_watches_for_project(user_project)
    elif user_role == "Manager":
        return project_controller.get_watches_for_project(user_project)
    elif user_role == "Student":
        return project_controller.get_watches_for_student(user_email)
    else:
        return pd.DataFrame()

# Cache watch data retrieval
@st.cache_data(ttl=600)  # Cache for 10 minutes
def fetch_watch_data(watch_name, signal_type, start_date, end_date, should_fetch=False):
    """
    Get data for a specific watch without using any Streamlit widgets.
    This function can be safely cached.
    """
    if not should_fetch:
        return pd.DataFrame()
    
    # Validate dates - don't allow future dates
    today = datetime.date.today()
    if isinstance(start_date, datetime.date) and start_date > today:
        st.warning(f"Start date {start_date} is in the future. Using today's date instead.")
        start_date = today
    if isinstance(end_date, datetime.date) and end_date > today:
        st.warning(f"End date {end_date} is in the future. Using today's date instead.")
        end_date = today
    
    # Get the watch details to create a Watch object
    watch_details = cached_get_watch_details(watch_name)
    if not watch_details:
        return pd.DataFrame()
    
    try:
        # Create a Watch object using the factory
        watch = WatchFactory.create_from_details(watch_details)
        df = pd.DataFrame()
        
        # Map signal type to appropriate endpoint and method
        if signal_type == "HR":
            # Convert date objects to strings in the format YYYY-MM-DD
            start_date_str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime.date) else start_date
            
            st.info(f"Fetching heart rate data for date: {start_date_str}")
            
            # Determine if we're fetching data for today, and if so, use current time as end_time
            is_today = start_date.strftime("%Y-%m-%d") == datetime.date.today().strftime("%Y-%m-%d")
            end_time = datetime.datetime.now().strftime("%H:%M") if is_today else "23:59"
            
            # Fetch heart rate data from Fitbit API
            try:
                data = watch.fetch_data(
                    'Heart Rate Intraday',
                    start_date=start_date_str,  # Pass date as string
                    start_time="00:00",
                    end_time=end_time
                )
                # Process data with Watch class method
                df = watch.get_data_as_dataframe('Heart Rate Intraday', data)
                
                # Rename columns for consistency with dashboard display
                if not df.empty and 'value' in df.columns:
                    df = df.rename(columns={'value': 'HR'})
                    df['syncDate'] = df['datetime']
            except Exception as hr_error:
                st.error(f"Heart Rate API error: {str(hr_error)}")
                return pd.DataFrame()
                
        elif signal_type == "steps":
            # Convert date objects to strings in the format YYYY-MM-DD
            start_date_str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime.date) else start_date
            
            # Determine if we're fetching data for today, and if so, use current time as end_time
            is_today = start_date.strftime("%Y-%m-%d") == datetime.date.today().strftime("%Y-%m-%d")
            end_time = datetime.datetime.now().strftime("%H:%M") if is_today else "23:59"
            
            # Fetch steps data from Fitbit API
            data = watch.fetch_data(
                'Steps Intraday',
                start_date=start_date_str,  # Pass date as string
                start_time="00:00",
                end_time=end_time
            )
            # Process data with Watch class method
            df = watch.get_data_as_dataframe('Steps Intraday', data)
            
            # Rename columns for consistency with dashboard display
            if not df.empty and 'value' in df.columns:
                df = df.rename(columns={'value': 'steps'})
                df['syncDate'] = df['datetime']
                
        elif signal_type == "sleep_duration":
            # Convert date objects to strings in the format YYYY-MM-DD
            start_date_str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime.date) else start_date
            end_date_str = end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime.date) else end_date
            
            # Fetch sleep data from Fitbit API
            data = watch.fetch_data(
                'Sleep',
                start_date=start_date_str,  # Pass date as string
                end_date=end_date_str       # Pass date as string
            )
            # Process data with Watch class method
            sleep_data = watch.process_data('Sleep', data)
            
            # Convert to DataFrame with proper formatting
            if sleep_data:
                df = pd.DataFrame(sleep_data)
                # Create a syncDate column for consistency
                df['syncDate'] = pd.to_datetime(df['start_time'])
                df['sleep_duration'] = df['duration'].apply(lambda x: x / (1000 * 60 * 60) if x else 0)  # Convert ms to hours
        
        # Add watch name column
        if not df.empty:
            df['name'] = watch_name
            
        return df
    except Exception as e:
        st.error(f"General error in fetch_watch_data: {str(e)}")
        return pd.DataFrame()

# Add a cache decorator for watch details
@st.cache_data(ttl=300)  # Cache for 5 minutes
def cached_get_watch_details(watch_name):
    project_controller = ProjectController()
    return project_controller.get_watch_details(watch_name)

def get_available_watches(user_email, user_role, user_project):
    """
    Get watches available to the user based on their role and project.
    
    Args:
        user_email (str): The email of the logged-in user
        user_role (str): The role of the user (Admin, Manager, Student, Guest)
        user_project (str): The project the user is associated with
    
    Returns:
        DataFrame: DataFrame of available watches
    """
    # Use cached function to avoid hitting API limits
    try:
        return cached_get_watches(user_email, user_role, user_project)
    except Exception as e:
        st.error(f"Error getting watches: {e}")
        # If we hit an API limit, wait and retry once
        time.sleep(2)
        try:
            return cached_get_watches(user_email, user_role, user_project)
        except:
            return pd.DataFrame()

def display_dashboard(user_email, user_role, user_project):
    """
    Display the Fitbit dashboard for the logged-in user.
    
    Args:
        user_email (str): The email of the logged-in user
        user_role (str): The role of the user (Admin, Manager, Student, Guest)
        user_project (str): The project the user is associated with
    """
    st.title("Fitbit Watch Dashboard")
    st.markdown("---")
    
    # Get available watches
    with st.spinner("Loading available watches..."):
        available_watches = get_available_watches(user_email, user_role, user_project)
    
    if available_watches.empty:
        st.warning("No watches available for your role and project")
        return
    
    # Display watch selector in the sidebar for easier navigation
    st.sidebar.subheader("Select Watch")
    watch_names = available_watches['name'].tolist()
    selected_watch = st.sidebar.selectbox("Choose a watch", watch_names)
    
    if selected_watch:
        # Display tabs for different views
        tab1, tab2 = st.tabs(["📊 Signal Data", "📱 Device Details"])
        
        with tab1:
            st.subheader(f"Signal Data for {selected_watch}")
            
            # Date range selector with better defaults
            col1, col2 = st.columns(2)
            with col1:
                # Keep as datetime object, don't convert to string
                start_date = st.date_input(
                    "Start Date", 
                    datetime.datetime.now() - datetime.timedelta(days=7),
                    max_value=datetime.datetime.now()
                )
            with col2:
                end_date = st.date_input(
                    "End Date", 
                    datetime.datetime.now(),
                    min_value=start_date,
                    max_value=datetime.datetime.now()
                )
            
            # Create a session state variable to track current view date if not exists
            if 'current_view_date' not in st.session_state:
                st.session_state.current_view_date = start_date
            
            # Date navigation within the selected range
            col1, col2, col3 = st.columns([1, 3, 1])
            
            with col1:
                if st.button("◀️ Previous Day"):
                    # Move backward one day, but not before start_date
                    if st.session_state.current_view_date > start_date:
                        st.session_state.current_view_date -= datetime.timedelta(days=1)
            
            with col2:
                # Display current view date
                current_date_str = st.session_state.current_view_date.strftime("%B %d, %Y")
                st.markdown(f"<h3 style='text-align: center;'>📅 {current_date_str}</h3>", unsafe_allow_html=True)
                
                # Show progress in date range
                date_range = (end_date - start_date).days
                if date_range > 0:
                    current_progress = (st.session_state.current_view_date - start_date).days / date_range
                    st.progress(min(1.0, max(0.0, current_progress)))
            
            with col3:
                if st.button("Next Day ▶️"):
                    # Move forward one day, but not after end_date
                    if st.session_state.current_view_date < end_date:
                        st.session_state.current_view_date += datetime.timedelta(days=1)
            
            # Signal selector
            signal_options = ["Heart Rate", "Steps", "Sleep"]
            selected_signal = st.selectbox("Select Signal Type", signal_options)
            
            # Map selected signal to data column
            signal_map = {
                "Heart Rate": "HR",
                "Steps": "steps",
                "Sleep": "sleep_duration"
            }
            
            signal_column = signal_map.get(selected_signal)
            
            # Move the button outside of the cached function
            load_data_button = st.button("Load Data")
            
            # Get and display data with caching to avoid API limits - use current view date
            with st.spinner(f"Loading {selected_signal} data directly from Fitbit API..."):
                # Pass the current view date to the function
                data = fetch_watch_data(
                    selected_watch, 
                    signal_column, 
                    st.session_state.current_view_date,  # Use the navigation date 
                    st.session_state.current_view_date,  # Same day for intraday data
                    should_fetch=load_data_button
                )
                
                # Optionally show raw data for debugging
                if load_data_button and data.empty:
                    st.error(f"No data returned for {selected_signal}")
                    
                    # Debug information
                    watch_details = cached_get_watch_details(selected_watch)
                    if watch_details:
                        try:
                            watch = WatchFactory.create_from_details(watch_details)
                            
                            if signal_column == "HR":
                                raw_data = watch.fetch_data(
                                    'Heart Rate Intraday',
                                    start_date=st.session_state.current_view_date,
                                    start_time="00:00",
                                    end_time="23:59"
                                )
                                st.json(raw_data)
                        except Exception as e:
                            st.error(f"Error fetching raw data: {e}")
            
            if not data.empty:
                # Display with mitosheet
                st.subheader(f"{selected_signal} Data Table")
                spreadsheet(data)
                
                # Show a plotly chart
                st.subheader(f"{selected_signal} Visualization")
                
                # Create visualization based on signal type
                if signal_column == "HR":
                    fig = px.line(data, x='syncDate', y='HR', 
                                 title=f'Heart Rate for {selected_watch}',
                                 labels={'syncDate': 'Date/Time', 'HR': 'Heart Rate (bpm)'},
                                 line_shape='spline')
                    fig.update_traces(line=dict(color='firebrick', width=2))
                elif signal_column == "steps":
                    fig = px.bar(data, x='syncDate', y='steps', 
                                title=f'Steps for {selected_watch}',
                                labels={'syncDate': 'Date/Time', 'steps': 'Step Count'},
                                color_discrete_sequence=['green'])
                elif signal_column == "sleep_duration":
                    fig = px.bar(data, x='syncDate', y='sleep_duration', 
                                title=f'Sleep Duration for {selected_watch}',
                                labels={'syncDate': 'Date/Time', 'sleep_duration': 'Sleep (hours)'},
                                color_discrete_sequence=['darkblue'])
                
                # Improve layout
                fig.update_layout(
                    xaxis_title="Date and Time",
                    yaxis_title=selected_signal,
                    plot_bgcolor='rgba(240,240,240,0.5)',
                    font=dict(family="Arial", size=14),
                    hovermode='closest',
                    margin=dict(t=50, b=50, l=40, r=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Display summary statistics
                st.subheader("Summary Statistics")
                col1, col2, col3 = st.columns(3)
                
                if signal_column in data.columns:
                    with col1:
                        st.metric("Average", f"{data[signal_column].mean():.2f}")
                    with col2:
                        st.metric("Minimum", f"{data[signal_column].min():.2f}")
                    with col3:
                        st.metric("Maximum", f"{data[signal_column].max():.2f}")
            elif load_data_button:
                st.info(f"No {selected_signal} data available for the selected date range")
        
        with tab2:
            st.subheader(f"Device Details: {selected_watch}")
            
            # Get and display watch details
            with st.spinner("Loading watch details directly from Fitbit API..."):
                watch_details = cached_get_watch_details(selected_watch)
                
                # Create Watch object and update with latest information from API
                if watch_details:
                    try:
                        watch = WatchFactory.create_from_details(watch_details)
                        watch.update_device_info()
                        
                        # Update watch_details with fresh data from the API
                        watch_details['lastBatteryLevel'] = watch.battery_level
                        watch_details['lastSynced'] = watch.last_sync_time.isoformat() if watch.last_sync_time else ""
                        watch_details['lastHeartRate'] = watch.get_current_hourly_HR() or ""
                        watch_details['lastSteps'] = watch.get_current_hourly_steps() or ""
                        sleep_start, sleep_end = watch.get_last_sleep_start_end()
                        watch_details['lastSleepStart'] = sleep_start or ""
                        watch_details['lastSleepEnd'] = sleep_end or ""
                        watch_details['lastSleepDuration'] = watch.get_last_sleep_duration() or ""
                    except Exception as e:
                        st.error(f"Error updating watch data from API: {e}")
            
            if watch_details:
                # Display in a nice format with expanders
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 📋 Basic Information")
                    st.info(f"**Name:** {watch_details.get('name', '')}")
                    st.info(f"**Project:** {watch_details.get('project', '')}")
                    
                    # Battery level with gauge chart
                    battery_level = watch_details.get('lastBatteryLevel', '0')
                    try:
                        battery_level = int(battery_level)
                    except:
                        battery_level = 0
                        
                    st.markdown("### 🔋 Battery Status")
                    
                    fig = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = battery_level,
                        domain = {'x': [0, 1], 'y': [0, 1]},
                        title = {'text': "Battery Level (%)"},
                        gauge = {
                            'axis': {'range': [0, 100]},
                            'bar': {'color': "darkgreen"},
                            'steps': [
                                {'range': [0, 20], 'color': "red"},
                                {'range': [20, 50], 'color': "orange"},
                                {'range': [50, 100], 'color': "lightgreen"}
                            ],
                            'threshold': {
                                'line': {'color': "red", 'width': 4},
                                'thickness': 0.75,
                                'value': 20
                            }
                        }
                    ))

                    fig.update_layout(height=250, margin=dict(l=10, r=10, t=50, b=10))
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.markdown("### 📊 Latest Metrics")
                    st.info(f"**Last Synced:** {watch_details.get('lastSynced', '')}")
                    st.info(f"**Heart Rate:** {watch_details.get('lastHeartRate', '')} bpm")
                    st.info(f"**Steps:** {watch_details.get('lastSteps', '')}")
                    
                    st.markdown("### 💤 Sleep Information")
                    st.info(f"**Last Sleep Start:** {watch_details.get('lastSleepStart', '')}")
                    st.info(f"**Last Sleep End:** {watch_details.get('lastSleepEnd', '')}")
                    st.info(f"**Sleep Duration:** {watch_details.get('lastSleepDuration', '')} hours")
                
                # Status warnings section
                st.markdown("### ⚠️ Device Status")
                
                status_ok = True
                
                # Check battery level
                if watch_details.get('lastBatteryLevel'):
                    try:
                        battery_level = int(watch_details.get('lastBatteryLevel', 0))
                        if battery_level < 20:
                            st.warning(f"🔋 Battery level is low ({battery_level}%). Please charge the device.")
                            status_ok = False
                    except:
                        pass
                
                # Calculate time since last sync
                if watch_details.get('lastSynced'):
                    try:
                        last_sync = pd.to_datetime(watch_details.get('lastSynced'))
                        time_since_sync = datetime.datetime.now() - last_sync
                        
                        if time_since_sync.total_seconds() > 86400:  # More than 24 hours
                            st.warning(f"⏰ Device hasn't synced in {time_since_sync.days} days and {time_since_sync.seconds//3600} hours. Please check connection.")
                            status_ok = False
                    except:
                        pass
                
                if status_ok:
                    st.success("✅ All systems normal. Device is functioning properly.")
                
                # User assignment section (if Admin or Manager)
                if user_role in ["Admin", "Manager"]:
                    with st.expander("Student Assignment"):
                        st.info("This section allows assignment of this watch to students")
                        # Fetch students for this project
                        # Add assignment UI here
            else:
                st.warning("Could not retrieve watch details. Device may be offline or not registered.")
