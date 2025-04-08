import streamlit as st
import pandas as pd
import datetime
from entity.Sheet import GoogleSheetsAdapter, Spreadsheet

def nova_qualtrics_management(user_email, user_role, user_project, spreadsheet):
    """
    Display the NOVA Qualtrics Management page.
    
    Shows data from EMA, late_nums, and suspicious_nums sheets with summary statistics.
    Allows users to accept numbers from late_nums and suspicious_nums sheets.
    
    Args:
        user_email (str): The email of the current user
        user_role (str): The role of the current user
        user_project (str): The project the user belongs to
        spreadsheet: The spreadsheet entity
    """
    # Check access permissions
    if not _check_access(user_role, user_project):
        st.error("You don't have permission to access this page")
        return
    
    # Page title and description
    st.title("📊 NOVA Qualtrics Management")
    st.write("Manage Qualtrics responses, late numbers, and suspicious numbers")
    
    # Initialize session state for selected numbers
    if 'selected_late_nums' not in st.session_state:
        st.session_state.selected_late_nums = set()
    if 'selected_suspicious_nums' not in st.session_state:
        st.session_state.selected_suspicious_nums = set()
    
    # Load sheets data
    with st.spinner("Loading data..."):
        ema_df = _get_sheet(spreadsheet, "EMA", "EMA").to_dataframe()
        late_nums_df = _get_sheet(spreadsheet, "late_nums", "late_nums").to_dataframe()
        suspicious_nums_df = _get_sheet(spreadsheet, "suspicious_nums", "suspicious_nums").to_dataframe()
    
    # Display summary statistics
    _display_summary_statistics(ema_df, late_nums_df, suspicious_nums_df)
    
    # Create tabs for different data views
    tabs = st.tabs(["EMA Responses", "Late Numbers", "Suspicious Numbers"])
    
    # EMA Tab
    with tabs[0]:
        _display_ema_data(ema_df)
    
    # Late Numbers Tab
    with tabs[1]:
        _display_late_nums(late_nums_df)
    
    # Suspicious Numbers Tab
    with tabs[2]:
        _display_suspicious_nums(suspicious_nums_df)
    
    # Display form for accepting numbers
    _display_accept_form(spreadsheet, late_nums_df, suspicious_nums_df)

def _check_access(user_role, user_project):
    """Check if user has access to this page"""
    if user_role == "Admin":
        return True
    
    if user_role == "managment" and "nova" in user_project:
        return True
    
    return False

def _get_sheet(spreadsheet, sheet_name, sheet_type):
    """Get a sheet from the spreadsheet"""
    try:
        sheet = spreadsheet.get_sheet(sheet_name, sheet_type)
        return sheet
    except Exception as e:
        st.error(f"Error loading {sheet_name} sheet: {e}")
        return None

def _display_summary_statistics(ema_df, late_nums_df, suspicious_nums_df):
    """Display summary statistics for the data"""
    st.subheader("Summary Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    # EMA Statistics
    with col1:
        st.write("**EMA Responses**")
        if ema_df is not None and not ema_df.empty:
            total_records = len(ema_df)
            
            # Get status counts if available
            status_counts = {}
            if 'status' in ema_df.columns:
                status_counts = ema_df['status'].value_counts().to_dict()
            
            # Display metrics
            st.metric("Total Records", total_records)
            
            if status_counts:
                st.write("Status Distribution:")
                for status, count in status_counts.items():
                    st.write(f"- {status}: {count}")
        else:
            st.write("No data available")
    
    # Late Numbers Statistics
    with col2:
        st.write("**Late Numbers**")
        if late_nums_df is not None and not late_nums_df.empty:
            total_records = len(late_nums_df)
            
            # Calculate average hours late if available
            avg_hours_late = 0
            if 'hoursLate' in late_nums_df.columns:
                try:
                    avg_hours_late = late_nums_df['hoursLate'].astype(float).mean()
                except:
                    pass
            
            # Count accepted numbers
            accepted_count = 0
            if 'accept' in late_nums_df.columns:
                accepted_count = late_nums_df['accept'].str.upper().eq('TRUE').sum()
            
            # Display metrics
            st.metric("Total Late Numbers", total_records)
            st.metric("Accepted Numbers", accepted_count)
            if avg_hours_late > 0:
                st.metric("Avg Hours Late", f"{avg_hours_late:.1f}")
        else:
            st.write("No data available")
    
    # Suspicious Numbers Statistics
    with col3:
        st.write("**Suspicious Numbers**")
        if suspicious_nums_df is not None and not suspicious_nums_df.empty:
            total_records = len(suspicious_nums_df)
            
            # Count accepted numbers
            accepted_count = 0
            if 'accept' in suspicious_nums_df.columns:
                accepted_count = suspicious_nums_df['accept'].str.upper().eq('TRUE').sum()
            
            # Display metrics
            st.metric("Total Suspicious", total_records)
            st.metric("Accepted Numbers", accepted_count)
            st.metric("Pending Numbers", total_records - accepted_count)
        else:
            st.write("No data available")

def _display_ema_data(ema_df):
    """Display EMA data with filters"""
    st.header("EMA Responses")
    
    if ema_df is not None and not ema_df.empty:
        # Filters
        with st.expander("Filters", expanded=False):
            # Filter by date if date columns exist
            date_cols = [col for col in ema_df.columns if 'endDate' in col.lower()]
            if date_cols:
                filter_date_col = st.selectbox("Filter by date", date_cols)
                if filter_date_col:
                    try:
                        # Try to get min and max dates
                        min_date = pd.to_datetime(ema_df[filter_date_col]).min()
                        max_date = pd.to_datetime(ema_df[filter_date_col]).max()
                        
                        # Date range slider
                        date_range = st.date_input(
                            "Date range",
                            value=(min_date, max_date),
                            min_value=min_date,
                            max_value=max_date
                        )
                        
                        if len(date_range) == 2:
                            start_date, end_date = date_range
                            mask = (pd.to_datetime(ema_df[filter_date_col]) >= pd.to_datetime(start_date)) & \
                                   (pd.to_datetime(ema_df[filter_date_col]) <= pd.to_datetime(end_date))
                            ema_df = ema_df[mask]
                    except:
                        st.warning(f"Could not parse dates in column {filter_date_col}")
            
            # Filter by status if status column exists
            if 'status' in ema_df.columns:
                statuses = ema_df['status'].unique().tolist()
                selected_statuses = st.multiselect("Filter by status", statuses, default=statuses)
                if selected_statuses:
                    ema_df = ema_df[ema_df['status'].isin(selected_statuses)]
            
            # Search by phone number
            num_cols = [col for col in ema_df.columns if 'num' in col.lower()]
            if num_cols:
                search_col = num_cols[0]
                search_text = st.text_input("Search by number")
                if search_text:
                    ema_df = ema_df[ema_df[search_col].astype(str).str.contains(search_text)]
        
        # Display data
        st.dataframe(ema_df, use_container_width=True)
        
        # Export option
        if st.button("Export to CSV"):
            csv = ema_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                data=csv,
                file_name="ema_data.csv",
                mime="text/csv",
            )
    else:
        st.info("No EMA data available")

@st.fragment
def _check_box_control(num, key):
    """Control checkbox selection and deselection"""
    is_selected = num in st.session_state.selected_late_nums
    if st.checkbox(f"Select {num}", value=is_selected, key=key, label_visibility="collapsed"):
        st.session_state.selected_late_nums.add(num)
    else:
        if num in st.session_state.selected_late_nums:
            st.session_state.selected_late_nums.remove(num)

    
def _display_late_nums(late_nums_df):
    """Display late numbers with selection options"""
    st.header("Late Numbers")
    
    if late_nums_df is not None and not late_nums_df.empty:
        st.write("Select numbers to accept:")
        
        # Display each number with a selection checkbox
        for i, row in late_nums_df.iterrows():
            num = str(row.get('nums', ''))
            if not num:  # Skip empty records
                continue
            
            # Check if already accepted
            is_accepted = str(row.get('accepted', '')).upper() == 'TRUE'
            
            col1, col2, col3, col4 = st.columns([0.1, 0.4, 0.3, 0.2])
            
            with col1:
                if is_accepted:
                    st.write("✅")
                else:
                    _check_box_control(num, f"late_{i}_{num}")
            
            with col2:
                st.write(f"**{num}**")
                
            with col3:
                sent_time = row.get('sentTime', 'N/A')
                st.write(f"Sent: {sent_time}")
                
            with col4:
                hours_late = row.get('hoursLate', 'N/A')
                st.write(f"{hours_late} hours late")
                
            st.divider()
    else:
        st.info("No late numbers available")

def _display_suspicious_nums(suspicious_nums_df):
    """Display suspicious numbers with selection options"""
    st.header("Suspicious Numbers")
    
    if suspicious_nums_df is not None and not suspicious_nums_df.empty:
        st.write("Select numbers to accept:")
        
        # Display each number with a selection checkbox
        for i, row in suspicious_nums_df.iterrows():
            num = str(row.get('nums', ''))
            if not num:  # Skip empty records
                continue
            
            # Check if already accepted
            is_accepted = str(row.get('accepted', '')).upper() == 'TRUE'
            
            col1, col2, col3 = st.columns([0.1, 0.5, 0.4])
            
            with col1:
                if is_accepted:
                    st.write("✅")
                else:
                    _check_box_control(num, f"suspicious_{i}_{num}")
            with col2:
                st.write(f"**{num}**")
                
            with col3:
                filled_time = row.get('filledTime', 'N/A')
                st.write(f"Filled: {filled_time}")
                
            st.divider()
    else:
        st.info("No suspicious numbers available")

def _display_accept_form(spreadsheet, late_nums_df, suspicious_nums_df):
    """Display form for accepting selected numbers"""
    st.subheader("Accept Selected Numbers")
    
    # Count selected numbers
    total_selected = len(st.session_state.selected_late_nums) + len(st.session_state.selected_suspicious_nums)
    
    if total_selected > 0:
        st.write(f"You have selected {total_selected} numbers to accept:")
        
        if st.session_state.selected_late_nums:
            st.write(f"**Late Numbers:**")
            for num in st.session_state.selected_late_nums:
                st.write(f"- {num}")
        
        if st.session_state.selected_suspicious_nums:
            st.write(f"**Suspicious Numbers:**")
            for num in st.session_state.selected_suspicious_nums:
                st.write(f"- {num}")
        
        # Save button
        if st.button("Save Accepted Numbers"):
            st.json(st.session_state.selected_late_nums)
            with st.spinner("Updating data..."):
                # Update late_nums sheet
                if not late_nums_df.empty and st.session_state.selected_late_nums:
                    _update_accepted_numbers(
                        spreadsheet, 
                        late_nums_df, 
                        st.session_state.selected_late_nums,
                        "late_nums"
                    )
                
                # Update suspicious_nums sheet
                if not suspicious_nums_df.empty and st.session_state.selected_suspicious_nums:
                    _update_accepted_numbers(
                        spreadsheet, 
                        suspicious_nums_df, 
                        st.session_state.selected_suspicious_nums,
                        "suspicious_nums"
                    )
                
                # Clear selected numbers
                st.session_state.selected_late_nums = set()
                st.session_state.selected_suspicious_nums = set()
                
                st.success("Numbers accepted successfully!")
                # st.rerun()
    else:
        st.info("No numbers selected. Select numbers from the Late Numbers or Suspicious Numbers tabs.")

def _update_accepted_numbers(spreadsheet: Spreadsheet, df, selected_numbers, sheet_name):
    """Update the 'accept' field for selected numbers in the DataFrame and save to sheet"""
    if df is None or df.empty:
        return

    # Create a copy to avoid modifying the original dataframe
    updated_df = df.copy()
    changes_made = False
    
    # Update 'accept' field for selected numbers
    for idx, row in updated_df.iterrows():
        num = row.get('nums', '')
        if num in selected_numbers and str(row.get('accepted', '')).upper() != 'TRUE':
            updated_df.at[idx, 'accepted'] = 'TRUE'
            changes_made = True
    
    # Only update the sheet if changes were made
    if changes_made:
        # Convert DataFrame to list of dictionaries for Sheet API
        updated_data = updated_df.to_dict('records')
        
        # Update the sheet with new data
        spreadsheet.update_sheet(sheet_name, updated_data)
        GoogleSheetsAdapter.save(spreadsheet, sheet_name)
