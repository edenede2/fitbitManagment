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
import re

from controllers.project_controller import ProjectController
from controllers.agGridHelper import aggrid_polars
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from entity.Watch import Watch, WatchFactory
from model.config import get_secrets
from entity.AsyncSheetsManager import AsyncSheetsManager

# Increase cache time to reduce API calls
# @st.cache_data(ttl=1800)  # Cache for 30 minutes instead of 5
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

# Add warm-up function for background prefetching
def prefetch_watch_data(user_email, user_role, user_project):
    """Prefetch watches data in the background to warm up cache"""
    try:
        # This will populate the cache without showing any errors to the user
        project_controller = ProjectController()
        if user_role == "Admin":
            return project_controller.get_watches_for_project(user_project)
        elif user_role == "Manager":
            return project_controller.get_watches_for_project(user_project)
        elif user_role == "Student":
            return project_controller.get_watches_for_student(user_email)
        return pd.DataFrame()
    except Exception:
        # Silent fail for background tasks
        return pd.DataFrame()

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
        elif signal_type == "missing_values":
            # Convert date objects to strings in the format YYYY-MM-DD
            start_date_str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime.date) else start_date
            end_date_str = end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime.date) else end_date
            
            # Show a message to the user that this might take some time
            st.info(f"Scanning for missing heart rate data from {start_date_str} to {end_date_str}. This may take a moment...")
            
            # Fetch missing values data from Fitbit API
            try:
                # Use the Watch methods we added to find days with missing data
                bad_days = watch.find_bad_days(
                    start_date=start_date_str,
                    end_date=end_date_str,
                    p_thresh=0.95  # Default threshold - 95% data required
                )
                
                # Process data with Watch class method
                df = watch.process_data('Missing Values', bad_days)
                
                # Convert to DataFrame with proper formatting
                if df:
                    df = pd.DataFrame(df)
                    
                    # Add percentage column for better visualization
                    df['percentage_missing'] = (df['missing_count'] / 1440 * 100).round(2)
                    
                    # Sort by date
                    df = df.sort_values('date')
            except Exception as e:
                st.error(f"Error fetching missing values: {str(e)}")
                df = pd.DataFrame()
                
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
    # Check if we need to use cache or have already warmed it up
    cache_key = f"watches_cache_{user_email}_{user_role}_{user_project}"
    if cache_key not in st.session_state:
        st.session_state[cache_key] = False
        # Start background prefetch after first load
        import threading
        threading.Thread(
            target=prefetch_watch_data,
            args=(user_email, user_role, user_project),
            daemon=True
        ).start()
    
    # Start timing the operation
    start_time = time.time()
    
    # Use cached function to avoid hitting API limits
    try:
        watches_df = cached_get_watches(user_email, user_role, user_project)
        
        # Log the time taken
        elapsed_time = time.time() - start_time
        
        # Only display timing info first time or if it's slow
        if not st.session_state[cache_key] or elapsed_time > 2.0:
            st.info(f"Watches loaded in {elapsed_time:.2f} seconds")
            
        # Mark that we've used the cache successfully
        st.session_state[cache_key] = True
        
        return watches_df
    except Exception as e:
        elapsed_time = time.time() - start_time
        st.error(f"Error getting watches after {elapsed_time:.2f} seconds: {e}")
        
        # If we need to retry, only wait if it's a rate limit error
        if "rate limit" in str(e).lower() or "quota" in str(e).lower():
            st.warning("API limit reached. Waiting to retry...")
            time.sleep(2)
            
            # Retry with timing
            retry_start = time.time()
            try:
                watches_df = cached_get_watches(user_email, user_role, user_project)
                retry_elapsed = time.time() - retry_start
                st.info(f"Retry succeeded in {retry_elapsed:.2f} seconds")
                return watches_df
            except Exception as retry_e:
                retry_elapsed = time.time() - retry_start
                st.error(f"Retry failed after {retry_elapsed:.2f} seconds: {retry_e}")
                return pd.DataFrame()
        else:
            # For non-rate-limit errors, return empty frame immediately
            return pd.DataFrame()

def display_dashboard(user_email, user_role, user_project, sp: Spreadsheet) -> None:
    """
    Display the Fitbit dashboard for the logged-in user.
    
    Args:
        user_email (str): The email of the logged-in user
        user_role (str): The role of the user (Admin, Manager, Student, Guest)
        user_project (str): The project the user is associated with
    """
    # Time the entire dashboard loading process
    dashboard_start_time = time.time()
    if "fitbit_watches" not in st.session_state:
        df = sp.get_sheet("fitbit", sheet_type="fitbit").to_dataframe("polars")
        dict_details_by_name = {}
        for row in df.iter_rows(named=True):
            # Debug what watches exist
            watch_in_sheet = row["name"]
            watch_project = row["project"]
            if user_project != watch_project:
                if user_role != "Admin":
                    continue
                
                    
            if watch_in_sheet:
                dict_details_by_name[watch_in_sheet] = row
        st.session_state.fitbit_watches = dict_details_by_name
    if "selected_watch" not in st.session_state:
        # Fix: dict_keys is not subscriptable, convert to list first or use next(iter())
        if st.session_state.fitbit_watches:
            st.session_state.selected_watch = next(iter(st.session_state.fitbit_watches.keys()))
        else:
            st.session_state.selected_watch = None
    st.title("Fitbit Watch Dashboard")
    st.markdown("---")
    
    # Add option to debug slow performance
    if st.checkbox("Show Performance Debug Info", value=False):
        st.write("This will display information about slow operations.")
        with st.expander("Project Controller Performance", expanded=True):
            # Add placeholder for project controller debug info
            if "controller_debug" not in st.session_state:
                st.session_state.controller_debug = []
            
            for debug_msg in st.session_state.controller_debug:
                st.text(debug_msg)
    
    # Get available watches
    with st.spinner("Loading available watches...",show_time=True):
        if 'available_watches' not in st.session_state:
            st.session_state.available_watches = st.session_state.fitbit_watches.keys()
    
        if len(st.session_state.available_watches) <= 0:
            st.warning("No watches available for your role and project")
            return
    
    # Log total dashboard load time only on initial load or if it's slow
    dashboard_load_time = time.time() - dashboard_start_time
    if dashboard_load_time > 2.0:  # Only show timing info if loading is slow
        st.info(f"Dashboard initialized in {dashboard_load_time:.2f} seconds")
    
    # Display watch selector in the main page (not sidebar)
    st.subheader("Select Watch")
    
    # Fix: Convert dictionary keys to a list of watch names
    watch_names = sorted(list(st.session_state.available_watches))
    
    # Initialize session state for selected watch if it doesn't exist
    if 'selected_watch' not in st.session_state:
        st.session_state.selected_watch = watch_names[0] if watch_names else None
    
    # Update session state when selection changes
    selected_watch = st.selectbox("Choose a watch", watch_names, 
                                index=watch_names.index(st.session_state.selected_watch) if st.session_state.selected_watch in watch_names else 0)
    
    if selected_watch != st.session_state.selected_watch:
        st.session_state.selected_watch = selected_watch
    
    # Display active status for the selected watch
    if st.session_state.selected_watch:
        st.write(f"Selected watch: {selected_watch}")

        if "watch_details" not in st.session_state:
            st.session_state.watch_details = {}

        if st.session_state.selected_watch not in st.session_state.watch_details:
            st.session_state.watch_details[st.session_state.selected_watch] = st.session_state.fitbit_watches[st.session_state.selected_watch]
        
        st.write("Watch Details:")
        st.json(st.session_state.watch_details[st.session_state.selected_watch])
        if isinstance(st.session_state.watch_details[st.session_state.selected_watch].get('isActive'), str):
            # Convert string to boolean
            is_active = True if (st.session_state.watch_details[st.session_state.selected_watch].get('isActive', '')).lower() == 'true' else False
        elif isinstance(st.session_state.watch_details[st.session_state.selected_watch].get('isActive'), bool):
            is_active = st.session_state.watch_details[st.session_state.selected_watch].get('isActive')
        else:
            is_active = st.session_state.watch_details[st.session_state.selected_watch].get('isActive', False)
        active_status = "🟢 Active" if is_active else "🔴 Inactive"
        st.info(f"Watch Status: {active_status}")
        
        # Display tabs for different views
        tab1, tab2 = st.tabs(["📊 Signal Data", "📱 Device Details"])
        
        with tab1:
            st.subheader(f"Signal Data for {st.session_state.selected_watch}")
            
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
                    max_value=datetime.datetime.now()
                )
            
            # Track previous selections to detect changes
            if 'prev_watch' not in st.session_state:
                st.session_state.prev_watch = st.session_state.selected_watch
            if 'prev_start_date' not in st.session_state:
                st.session_state.prev_start_date = start_date
            if 'prev_end_date' not in st.session_state:
                st.session_state.prev_end_date = end_date
            
            # Signal selector
            signal_options = ["missingValues","Heart Rate", "Steps", "Sleep"]
            selected_signal = st.selectbox("Select Signal Type", signal_options)
            
            # Track previous signal selection
            if 'prev_signal' not in st.session_state:
                st.session_state.prev_signal = selected_signal
            
            # Map selected signal to data column
            signal_map = {
                "Heart Rate": "HR",
                "Steps": "steps",
                "Sleep": "sleep_duration",
                "missingValues": "missing_values"
            }
            
            signal_column = signal_map.get(selected_signal)
            
            # Better session state management
            if "load_data_button" not in st.session_state:
                st.session_state.load_data_button = False
            if "loading_complete" not in st.session_state:
                st.session_state.loading_complete = False
            if "loaded_dates" not in st.session_state:
                st.session_state.loaded_dates = []
            
            # Check for changes in selection that should reset the load button state
            selection_changed = (st.session_state.prev_watch != st.session_state.selected_watch or 
                                st.session_state.prev_start_date != start_date or 
                                st.session_state.prev_end_date != end_date or
                                st.session_state.prev_signal != selected_signal)
            
            if selection_changed:
                if st.session_state.load_data_button:
                    st.session_state.load_data_button = False
                    st.session_state.loading_complete = False
                    st.session_state.loaded_dates = []
                    st.session_state.current_data = None
                    st.session_state.loaded_watch = None
                    st.session_state.loaded_signal = None
                
                # Update previous selections
                st.session_state.prev_watch = st.session_state.selected_watch
                st.session_state.prev_start_date = start_date
                st.session_state.prev_end_date = end_date
                st.session_state.prev_signal = selected_signal
            
            # Show debugging info in an expander
            with st.expander("Debug Info", expanded=False):
                st.write("Button state:", st.session_state.load_data_button)
                st.write("Loading complete:", st.session_state.loading_complete)
                st.write("Selected watch:", st.session_state.selected_watch)
                st.write("Selected signal:", signal_column)
                st.write("Selection changed:", selection_changed)
            
            # Create a button that triggers loading
            load_button_clicked = st.button("Load Data")
            
            # Set the flag when button is clicked
            if load_button_clicked:
                st.session_state.load_data_button = True
                
            # Process data when button is clicked but loading is not complete
            if st.session_state.load_data_button and not st.session_state.loading_complete:
                # Calculate date range
                date_range = []
                current_date = start_date
                while current_date <= end_date:
                    date_range.append(current_date)
                    current_date += datetime.timedelta(days=1)
                
                # Initialize container for all data
                all_data = pd.DataFrame()
                
                # Use a with st.spinner block to show loading status
                with st.spinner(f"Fetching {selected_signal} data for {len(date_range)} days...",show_time=True):
                    # Add a progress bar
                    progress_bar = st.progress(0)
                    if signal_column not in ["sleep_duration", "missing_values"]:
                        # Process each date
                        for i, single_date in enumerate(date_range):
                            # Update progress
                            progress_bar.progress((i+1)/len(date_range))
                            
                            # Format date for display
                            date_str = single_date.strftime("%Y-%m-%d")
                            st.text(f"Processing {date_str} ({i+1}/{len(date_range)})")
                            
                            # Unique key for this date's data
                            day_data_key = f"{st.session_state.selected_watch}_{signal_column}_{date_str}"
                            
                            # Fetch data
                            day_data = fetch_watch_data(
                                st.session_state.selected_watch, 
                                signal_column, 
                                single_date,
                                single_date,
                                should_fetch=True
                            )
                            
                            # Store in session state
                            if not day_data.empty:
                                st.session_state[day_data_key] = day_data
                                if date_str not in st.session_state.loaded_dates:
                                    st.session_state.loaded_dates.append(date_str)
                                all_data = pd.concat([all_data, day_data])
                    else:
                        # Special case for sleep data
                        st.text(f"Processing {signal_column} data for {st.session_state.selected_watch} from {start_date} to {end_date}")
                        date_str = start_date.strftime("%Y-%m-%d")
                        # Unique key for this date's data
                        day_data_key = f"{st.session_state.selected_watch}_{signal_column}_{date_str}"
                        # Fetch sleep data
                        sleep_data = fetch_watch_data(
                            st.session_state.selected_watch, 
                            signal_column, 
                            start_date,
                            end_date,
                            should_fetch=True
                        )
                        st.write(sleep_data)
                        # Store in session state
                        if not sleep_data.empty:
                            st.session_state[day_data_key] = sleep_data
                            all_data = pd.concat([all_data, sleep_data])
                            if date_str not in st.session_state.loaded_dates:
                                st.session_state.loaded_dates.append(date_str)
                        else:
                            st.warning("No sleep data found for the selected date range.")
                            st.session_state.loaded_dates = []
                            st.session_state.current_data = None
                            st.session_state.loaded_watch = None
                            st.session_state.loaded_signal = None                    
                    st.write(all_data)
                    # Store combined data
                    if not all_data.empty:
                        st.session_state.current_data = all_data
                        st.session_state.loaded_watch = st.session_state.selected_watch
                        st.session_state.loaded_signal = signal_column
                    else:
                        st.warning("No data found for the selected date range.")
                        st.session_state.loaded_dates = []
                        st.session_state.current_data = None
                        st.session_state.loaded_watch = None
                        st.session_state.loaded_signal = None
                
                # Mark loading as complete to prevent reloading on rerun
                st.session_state.loading_complete = True
                # Force a rerun to display the data
                st.rerun()
            
            # Display the loaded data after loading is complete
            if st.session_state.loading_complete and "loaded_watch" in st.session_state:
                # If there's no data, show a warning
                if st.session_state.current_data is None or st.session_state.current_data.empty:
                    st.warning("No data to show.")
                    st.session_state.loading_complete = False
                    st.session_state.loaded_dates = []
                    st.session_state.current_data = None
                    st.session_state.loaded_watch = None
                    st.session_state.loaded_signal = None
                    st.session_state.load_data_button = False
                elif st.session_state.loaded_watch != st.session_state.selected_watch:
                    st.warning("Data loaded for a different watch. Please select the correct watch and run again.")
                    st.session_state.loading_complete = False
                    st.session_state.loaded_dates = []
                    st.session_state.current_data = None
                    st.session_state.loaded_watch = None
                    st.session_state.loaded_signal = None
                    st.session_state.load_data_button = False
                else:
                    st.success(f"Data loaded successfully for {len(st.session_state.loaded_dates)} dates")
                    
                    # Display data for each date in expanders
                    for date_str in st.session_state.loaded_dates:
                        day_data_key = f"{st.session_state.selected_watch}_{signal_column}_{date_str}"
                        
                        if day_data_key in st.session_state:
                            with st.expander(f"Data for {date_str}", expanded=False):
                                if not st.session_state[day_data_key].empty:
                                    st.subheader(f"{selected_signal} for {date_str}")
                                    
                                    # Safe way to use spreadsheet
                                    try:
                                        spreadsheet(st.session_state[day_data_key])
                                    except Exception as e:
                                        st.error(f"Error with spreadsheet: {str(e)}")
                                        st.dataframe(st.session_state[day_data_key])
                                    
                                    # Create visualization
                                    if signal_column == "HR":
                                        fig = px.line(st.session_state[day_data_key], x='syncDate', y='HR',
                                                    title=f'Heart Rate for {date_str}')
                                        st.plotly_chart(fig, use_container_width=True)
                                    elif signal_column == "steps":
                                        fig = px.bar(st.session_state[day_data_key], x='syncDate', y='steps',
                                                    title=f'Steps for {date_str}')
                                        st.plotly_chart(fig, use_container_width=True)
                                    elif signal_column == "sleep_duration":
                                        
                                        df = pl.DataFrame(st.session_state[day_data_key]).with_columns(
                                            pl.col("duration").pipe(lambda x: x / (1000 * 60 * 60))
                                        )
                                        aggrid_polars(df,
                                                    key=f"sleep_duration_{date_str}",
                                                    selection_mode="single")
                                    elif signal_column == "missing_values":
                                        # Create a bar chart showing missing minutes per day
                                        fig = px.bar(
                                            st.session_state[day_data_key], 
                                            x='date', 
                                            y='missing_values',
                                            title=f'Missing Heart Rate Data (minutes) for {date_str}',
                                            labels={
                                                'date': 'Date',
                                                'missing_values': 'Missing Minutes'
                                            },
                                            color='percentage_missing',
                                            color_continuous_scale='Reds',
                                        )
                                        fig.update_layout(
                                            xaxis_title='Date',
                                            yaxis_title='Missing Minutes (out of 1440)',
                                            coloraxis_colorbar_title='% Missing'
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                        
                                        # Also display as a table for detailed analysis
                                        st.write("### Detailed Missing Data")
                                        detail_df = st.session_state[day_data_key].copy()
                                        detail_df['available_minutes'] = 1440 - detail_df['missing_values']
                                        detail_df['available_percentage'] = (detail_df['available_minutes'] / 1440 * 100).round(2)
                                        st.dataframe(
                                            detail_df[['date', 'missing_values', 'available_minutes', 'percentage_missing', 'available_percentage']],
                                            use_container_width=True
                                        )

                                else:
                                    st.info(f"No data for {date_str}")
                            break
                    
                    # # Add a clear button to reset loading state
                    # if st.button("Clear Data"):
                    #     st.session_state.load_data_button = False
                    #     st.session_state.loading_complete = False
                    #     st.session_state.loaded_dates = []
                    #     st.session_state.current_data = None
                    #     st.session_state.loaded_watch = None
                    #     st.session_state.loaded_signal = None
                    #     st.rerun()
        
        with tab2:
            st.subheader(f"Device Details: {st.session_state.selected_watch}")
            
            # Add a refresh button to explicitly fetch fresh data
            refresh_device = st.button("🔄 Refresh Device Data")
            
            # Get and display watch details
            with st.spinner("Loading watch details...",show_time=True):
                if 'watch_details' not in st.session_state:
                    st.session_state.watch_details = {}
                if st.session_state.selected_watch not in st.session_state.watch_details:
                    st.session_state.watch_details[st.session_state.selected_watch] = st.session_state.fitbit_watches[st.session_state.selected_watch]
                
                # Create Watch object and update with latest information from API only when refresh is clicked
                if st.session_state.watch_details[st.session_state.selected_watch]:
                    try:
                        watch = WatchFactory.create_from_details(st.session_state.watch_details[st.session_state.selected_watch])
                        
                        # Only make API calls when refresh button is clicked
                        if refresh_device:
                            with st.spinner("Fetching latest data from Fitbit API...",show_time=True):
                                # Force fetch fresh data from the API
                                watch.update_device_info(force_fetch=True)
                                
                                # Update watch_details with fresh data from the API
                                st.session_state.watch_details[st.session_state.selected_watch]['lastBatteryLevel'] = watch.battery_level
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSynced'] = watch.last_sync_time.isoformat() if watch.last_sync_time else ""
                                st.session_state.watch_details[st.session_state.selected_watch]['lastHeartRate'] = watch.get_current_hourly_HR(force_fetch=True) or ""
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSteps'] = watch.get_current_hourly_steps(force_fetch=True) or ""
                                sleep_start, sleep_end = watch.get_last_sleep_start_end(force_fetch=True)
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepStart'] = sleep_start or ""
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepEnd'] = sleep_end or ""
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepDuration'] = watch.get_last_sleep_duration(force_fetch=True) or ""
                                
                                st.success("✅ Device data refreshed successfully!")
                        else:
                            # Use existing data from watch_details without making API calls
                            # If some values are missing in watch_details, initialize them without API calls
                            if 'lastBatteryLevel' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastBatteryLevel'] = watch.battery_level
                            if 'lastSynced' not in st.session_state.watch_details[st.session_state.selected_watch] and watch.last_sync_time:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSynced'] = watch.last_sync_time.isoformat()
                            
                            # These will use cached values without making API calls
                            if 'lastHeartRate' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastHeartRate'] = ""
                            if 'lastSteps' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSteps'] = ""
                            if 'lastSleepStart' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepStart'] = ""
                            if 'lastSleepEnd' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepEnd'] = ""
                            if 'lastSleepDuration' not in st.session_state.watch_details[st.session_state.selected_watch]:
                                st.session_state.watch_details[st.session_state.selected_watch]['lastSleepDuration'] = ""
                    except Exception as e:
                        st.error(f"Error with watch data: {e}")
                
                if refresh_device:
                    st.info("Using freshly fetched data from Fitbit API")
                else:
                    st.info("Using cached data. Click 'Refresh Device Data' for real-time information.")
                
                if st.session_state.watch_details[st.session_state.selected_watch]:
                    # Display in a nice format with expanders
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("### 📋 Basic Information")
                        st.info(f"**Name:** {st.session_state.watch_details[st.session_state.selected_watch].get('name', '')}")
                        st.info(f"**Project:** {st.session_state.watch_details[st.session_state.selected_watch].get('project', '')}")
                        st.info(f"**Status:** {active_status}")  # Added isActive status display
                        
                        # Battery level with gauge chart
                        battery_level = st.session_state.watch_details[st.session_state.selected_watch].get('lastBatteryLevel', '0')
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
                        # st.write(f"**selected_details:** {st.session_state.watch_details[st.session_state.selected_watch]}")
                        st.info(f"**Last Synced:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastSynced', '')}")
                        st.info(f"**Heart Rate:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastHeartRate', '')} bpm")
                        st.info(f"**Steps:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastSteps', '')}")
                        
                        st.markdown("### 💤 Sleep Information")
                        st.info(f"**Last Sleep Start:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastSleepStart', '')}")
                        st.info(f"**Last Sleep End:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastSleepEnd', '')}")
                        st.info(f"**Sleep Duration:** {st.session_state.watch_details[st.session_state.selected_watch].get('lastSleepDuration', '')} hours")
                    
                    # Status warnings section - removed the "all systems normal" success message
                    st.markdown("### ⚠️ Device Status")
                    
                    # Check battery level
                    if st.session_state.watch_details[st.session_state.selected_watch].get('lastBatteryLevel'):
                        try:
                            battery_level = int(st.session_state.watch_details[st.session_state.selected_watch].get('lastBatteryLevel', 0))
                            if battery_level < 20:
                                st.warning(f"🔋 Battery level is low ({battery_level}%). Please charge the device.")
                        except:
                            pass
                    
                    # Calculate time since last sync
                    if st.session_state.watch_details[st.session_state.selected_watch].get('lastSynced'):
                        try:
                            last_sync = pd.to_datetime(st.session_state.watch_details[st.session_state.selected_watch].get('lastSynced'))
                            time_since_sync = datetime.datetime.now() - last_sync
                            
                            if time_since_sync.total_seconds() > 86400:  # More than 24 hours
                                st.warning(f"⏰ Device hasn't synced in {time_since_sync.days} days and {time_since_sync.seconds//3600} hours. Please check connection.")
                        except:
                            pass
                    
                    # User assignment section (if Admin or Manager)
                    if user_role in ["Admin", "Manager"]:
                        with st.expander("Student Assignment"):
                            st.info("This section allows assignment of this watch to students")
                            try:
                                # Initialize AsyncSheetsManager if not already set up
                                if "async_sheets_manager" not in st.session_state:
                                    sheets_manager = AsyncSheetsManager.get_instance()
                                    connected = sheets_manager.connect(
                                        "FitbitData", 
                                        get_secrets().get('spreadsheet_key')
                                    )
                                    sheets_manager.start_worker()
                                    st.session_state.async_sheets_manager = sheets_manager
                                else:
                                    sheets_manager = st.session_state.async_sheets_manager
                                
                                # Get or initialize the watch's chat messages in session state
                                watch_chat_key = f"chat_{st.session_state.selected_watch}"
                                if watch_chat_key not in st.session_state:
                                    # Try to fetch existing messages for this watch
                                    try:
                                        student_sheet = sp.get_sheet("chats", sheet_type="chats")
                                        student_df = student_sheet.to_dataframe(engine='polars')
                                        
                                        if student_df.is_empty() or "watchName" not in student_df.columns:
                                            st.session_state[watch_chat_key] = []
                                        else:
                                            # Filter to get only messages for this watch
                                            watch_df = student_df.filter(pl.col("watchName") == st.session_state.selected_watch)
                                            st.session_state[watch_chat_key] = watch_df.to_dicts() if not watch_df.is_empty() else []
                                    except Exception as e:
                                        # If there's an error, start with an empty chat
                                        st.warning(f"Error loading chat history: {str(e)}")
                                        st.session_state[watch_chat_key] = []
                                
                                # Initialize a message counter if it doesn't exist
                                if "message_counter" not in st.session_state:
                                    st.session_state.message_counter = 0
                                
                                # Display any debug info from the sheets manager
                                debug_info = sheets_manager.get_debug_info()
                                if debug_info and st.checkbox("Show debug info", value=False):
                                    with st.expander("Sheet Saving Debug Info"):
                                        for msg in debug_info:
                                            st.text(msg)
                                
                                # Display chat messages directly from session state
                                st.subheader("Chat Messages")
                                
                                # Display all messages for this watch
                                messages = st.session_state[watch_chat_key]
                                if not messages:
                                    st.info("No messages yet. Start a conversation!")
                                else:
                                    # Show messages in a scrollable container
                                    with st.container():
                                        for msg in messages:
                                            with st.chat_message("human"):
                                                st.markdown(f"**{msg.get('user', 'Unknown')}**: {msg.get('content', '')}")
                                                st.markdown(f"**date:** {msg.get('datetime', 'Unknown date')}")
                                                st.divider()
                                
                                # Use a form for message input to better control submission
                                with st.form(key=f"chat_form_{st.session_state.selected_watch}", clear_on_submit=True):
                                    new_message = st.text_area("Type your message", key="message_text", height=100)
                                    submit_button = st.form_submit_button("Send Message")
                                    
                                    if submit_button and new_message.strip():
                                        # Format datetime as string for consistency
                                        now = datetime.datetime.now()
                                        dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                                        
                                        # Create new message dictionary
                                        new_row = {
                                            "watchName": st.session_state.selected_watch,
                                            "user": user_email,
                                            "content": new_message,
                                            "datetime": dt_string
                                        }
                                        
                                        # Update session state immediately for UI responsiveness
                                        current_messages = st.session_state[watch_chat_key]
                                        current_messages.append(new_row)
                                        st.session_state[watch_chat_key] = current_messages
                                        
                                        # Queue for background saving
                                        sheets_manager.add_message(new_row)
                                        
                                        # Provide immediate feedback to user
                                        st.success("Message sent! Saving to sheet in background.")
                                        
                                        # Increment counter for unique widget keys
                                        st.session_state.message_counter += 1
                            except Exception as e:
                                st.error(f"Error with chat functionality: {str(e)}")
                                import traceback
                                st.code(traceback.format_exc())

                else:
                    st.warning("Could not retrieve watch details. Device may be offline or not registered.")
