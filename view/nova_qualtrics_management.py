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
        ema_sheet = _get_sheet(spreadsheet, "EMA", "EMA")
        late_nums_sheet = _get_sheet(spreadsheet, "late_nums", "late_nums")
        suspicious_nums_sheet = _get_sheet(spreadsheet, "suspicious_nums", "suspicious_nums")
    
    # Display summary statistics
    _display_summary_statistics(ema_sheet, late_nums_sheet, suspicious_nums_sheet)
    
    # Create tabs for different data views
    tabs = st.tabs(["EMA Responses", "Late Numbers", "Suspicious Numbers"])
    
    # EMA Tab
    with tabs[0]:
        _display_ema_data(ema_sheet)
    
    # Late Numbers Tab
    with tabs[1]:
        _display_late_nums(late_nums_sheet)
    
    # Suspicious Numbers Tab
    with tabs[2]:
        _display_suspicious_nums(suspicious_nums_sheet)
    
    # Display form for accepting numbers
    _display_accept_form(spreadsheet, late_nums_sheet, suspicious_nums_sheet)

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

def _display_summary_statistics(ema_sheet, late_nums_sheet, suspicious_nums_sheet):
    """Display summary statistics for the data"""
    st.subheader("Summary Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    # EMA Statistics
    with col1:
        st.write("**EMA Responses**")
        if ema_sheet and hasattr(ema_sheet, 'data') and ema_sheet.data:
            data = ema_sheet.data
            total_records = len(data)
            
            # Get status counts if available
            status_counts = {}
            if data and 'status' in data[0]:
                for record in data:
                    status = record.get('status', 'Unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1
            
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
        if late_nums_sheet and hasattr(late_nums_sheet, 'data') and late_nums_sheet.data:
            data = late_nums_sheet.data
            total_records = len(data)
            
            # Calculate average hours late if available
            avg_hours_late = 0
            if data and 'hoursLate' in data[0]:
                total_hours = 0
                count = 0
                for record in data:
                    try:
                        hours = float(record.get('hoursLate', 0))
                        total_hours += hours
                        count += 1
                    except:
                        pass
                
                if count > 0:
                    avg_hours_late = total_hours / count
            
            # Count accepted numbers
            accepted_count = sum(1 for record in data if str(record.get('accept', '')).upper() == 'TRUE')
            
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
        if suspicious_nums_sheet and hasattr(suspicious_nums_sheet, 'data') and suspicious_nums_sheet.data:
            data = suspicious_nums_sheet.data
            total_records = len(data)
            
            # Count accepted numbers
            accepted_count = sum(1 for record in data if str(record.get('accept', '')).upper() == 'TRUE')
            
            # Display metrics
            st.metric("Total Suspicious", total_records)
            st.metric("Accepted Numbers", accepted_count)
            st.metric("Pending Numbers", total_records - accepted_count)
        else:
            st.write("No data available")

def _display_ema_data(ema_sheet):
    """Display EMA data with filters"""
    st.header("EMA Responses")
    
    if ema_sheet and hasattr(ema_sheet, 'data') and ema_sheet.data:
        # Convert to dataframe for display
        df = pd.DataFrame(ema_sheet.data)
        
        # Filters
        with st.expander("Filters", expanded=False):
            # Filter by date if date columns exist
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            if date_cols:
                filter_date_col = st.selectbox("Filter by date", date_cols)
                if filter_date_col:
                    try:
                        # Try to get min and max dates
                        min_date = pd.to_datetime(df[filter_date_col].min())
                        max_date = pd.to_datetime(df[filter_date_col].max())
                        
                        # Date range slider
                        date_range = st.date_input(
                            "Date range",
                            value=(min_date, max_date),
                            min_value=min_date,
                            max_value=max_date
                        )
                        
                        if len(date_range) == 2:
                            start_date, end_date = date_range
                            mask = (pd.to_datetime(df[filter_date_col]) >= pd.to_datetime(start_date)) & \
                                   (pd.to_datetime(df[filter_date_col]) <= pd.to_datetime(end_date))
                            df = df[mask]
                    except:
                        st.warning(f"Could not parse dates in column {filter_date_col}")
            
            # Filter by status if status column exists
            if 'status' in df.columns:
                statuses = df['status'].unique().tolist()
                selected_statuses = st.multiselect("Filter by status", statuses, default=statuses)
                if selected_statuses:
                    df = df[df['status'].isin(selected_statuses)]
            
            # Search by phone number
            num_cols = [col for col in df.columns if 'num' in col.lower()]
            if num_cols:
                search_col = num_cols[0]
                search_text = st.text_input("Search by number")
                if search_text:
                    df = df[df[search_col].astype(str).str.contains(search_text)]
        
        # Display data
        st.dataframe(df, use_container_width=True)
        
        # Export option
        if st.button("Export to CSV"):
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                data=csv,
                file_name="ema_data.csv",
                mime="text/csv",
            )
    else:
        st.info("No EMA data available")

def _display_late_nums(late_nums_sheet):
    """Display late numbers with selection options"""
    st.header("Late Numbers")
    
    if late_nums_sheet and hasattr(late_nums_sheet, 'data') and late_nums_sheet.data:
        st.write("Select numbers to accept:")
        
        # Display each number with a selection checkbox
        for i, record in enumerate(late_nums_sheet.data):
            num = record.get('nums', '')
            if not num:  # Skip empty records
                continue
            
            # Check if already accepted
            is_accepted = str(record.get('accept', '')).upper() == 'TRUE'
            
            col1, col2, col3, col4 = st.columns([0.1, 0.4, 0.3, 0.2])
            
            with col1:
                if is_accepted:
                    st.write("✅")
                else:
                    is_selected = num in st.session_state.selected_late_nums
                    if st.checkbox("", value=is_selected, key=f"late_{i}_{num}"):
                        st.session_state.selected_late_nums.add(num)
                    else:
                        if num in st.session_state.selected_late_nums:
                            st.session_state.selected_late_nums.remove(num)
            
            with col2:
                st.write(f"**{num}**")
                
            with col3:
                sent_time = record.get('sentTime', 'N/A')
                st.write(f"Sent: {sent_time}")
                
            with col4:
                hours_late = record.get('hoursLate', 'N/A')
                st.write(f"{hours_late} hours late")
                
            st.divider()
    else:
        st.info("No late numbers available")

def _display_suspicious_nums(suspicious_nums_sheet):
    """Display suspicious numbers with selection options"""
    st.header("Suspicious Numbers")
    
    if suspicious_nums_sheet and hasattr(suspicious_nums_sheet, 'data') and suspicious_nums_sheet.data:
        st.write("Select numbers to accept:")
        
        # Display each number with a selection checkbox
        for i, record in enumerate(suspicious_nums_sheet.data):
            num = record.get('nums', '')
            if not num:  # Skip empty records
                continue
            
            # Check if already accepted
            is_accepted = str(record.get('accepted', '')).upper() == 'TRUE'
            
            col1, col2, col3 = st.columns([0.1, 0.5, 0.4])
            
            with col1:
                if is_accepted:
                    st.write("✅")
                else:
                    is_selected = num in st.session_state.selected_suspicious_nums
                    if st.checkbox("", value=is_selected, key=f"suspicious_{i}_{num}"):
                        st.session_state.selected_suspicious_nums.add(num)
                    else:
                        if num in st.session_state.selected_suspicious_nums:
                            st.session_state.selected_suspicious_nums.remove(num)
            
            with col2:
                st.write(f"**{num}**")
                
            with col3:
                filled_time = record.get('filledTime', 'N/A')
                st.write(f"Filled: {filled_time}")
                
            st.divider()
    else:
        st.info("No suspicious numbers available")

def _display_accept_form(spreadsheet, late_nums_sheet, suspicious_nums_sheet):
    """Display form for accepting selected numbers"""
    st.subheader("Accept Selected Numbers")
    
    # Count selected numbers
    total_selected = len(st.session_state.selected_late_nums) + len(st.session_state.selected_suspicious_nums)
    
    if total_selected > 0:
        st.write(f"You have selected {total_selected} numbers to accept:")
        
        if st.session_state.selected_late_nums:
            st.write(f"**Late Numbers:** {', '.join(st.session_state.selected_late_nums)}")
        
        if st.session_state.selected_suspicious_nums:
            st.write(f"**Suspicious Numbers:** {', '.join(st.session_state.selected_suspicious_nums)}")
        
        # Save button
        if st.button("Save Accepted Numbers"):
            with st.spinner("Updating data..."):
                # Update late_nums sheet
                if late_nums_sheet and st.session_state.selected_late_nums:
                    _update_accepted_numbers(
                        spreadsheet, 
                        late_nums_sheet, 
                        st.session_state.selected_late_nums,
                        "late_nums"
                    )
                
                # Update suspicious_nums sheet
                if suspicious_nums_sheet and st.session_state.selected_suspicious_nums:
                    _update_accepted_numbers(
                        spreadsheet, 
                        suspicious_nums_sheet, 
                        st.session_state.selected_suspicious_nums,
                        "suspicious_nums"
                    )
                
                # Clear selected numbers
                st.session_state.selected_late_nums = set()
                st.session_state.selected_suspicious_nums = set()
                
                st.success("Numbers accepted successfully!")
                st.rerun()
    else:
        st.info("No numbers selected. Select numbers from the Late Numbers or Suspicious Numbers tabs.")

def _update_accepted_numbers(spreadsheet: Spreadsheet, sheet, selected_numbers, sheet_name):
    """Update the 'accept' field for selected numbers in the sheet"""
    # Make a copy of the data to track changes
    updated_data = []
    changes_made = False
    
    # Go through each record, mark selected numbers as accepted
    for record in sheet.data:
        num = record.get('nums', '')
        if num in selected_numbers and str(record.get('accept', '')).upper() != 'TRUE':
            # Create a copy of the record to modify
            updated_record = record.copy()
            updated_record['accept'] = 'TRUE'
            updated_data.append(updated_record)
            changes_made = True
        else:
            # Keep the record as is
            updated_data.append(record)
            GoogleSheetsAdapter().save(updated_data)
    
    # Only update the sheet if changes were made
    if changes_made:
        # Update the sheet with new data
        spreadsheet.update_sheet(sheet_name, updated_data)
