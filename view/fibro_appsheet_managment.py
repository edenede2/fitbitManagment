import streamlit as st
import pandas as pd
import polars as pl
from datetime import datetime
import plotly.express as px
from entity.Sheet import GoogleSheetsAdapter, Spreadsheet, Sheet
from controllers.agGridHelper import aggrid_polars
def load_fibro_datatable(user_email, user_role, user_project, spreadsheet):
    pass  # Replace this with the existing subject management code

def display_fibro_ema_data(spreadsheet: Spreadsheet):
    """
    Displays EMA (Ecological Momentary Assessment) data from the Google Sheet.
    Data is sourced from the sheet specified in secrets.fibro_ema_sheet.
    Uses Polars for data processing.
    """
    st.header("Fibromyalgia EMA Data Visualization")
    
    # Load data from the Google Sheet using Polars with better error handling
    with st.spinner("Loading EMA data from Google Sheets...",show_time=True):
        try:
            # First get data as pandas to handle mixed types better
            pandas_df = spreadsheet.get_sheet("for_analysis", "for_analysis").to_dataframe(engine="pandas")
            # st.write(f"Loaded {pandas_df}")
            if pandas_df is not None and not pandas_df.empty:
                # Handle mixed types issues before converting to polars
                # First, detect object columns that might contain mixed types
                for col in pandas_df.columns:
                    # Convert any column that might have mixed types to string
                    # This prevents PyArrow conversion errors
                    if pandas_df[col].dtype == 'object':
                        pandas_df[col] = pandas_df[col].astype(str)
                        # Replace 'nan' strings with None
                        pandas_df[col] = pandas_df[col].replace('nan', None)
                        pandas_df[col] = pandas_df[col].replace('None', None)
                        pandas_df[col] = pandas_df[col].replace('', None)
                
                # Use pandas directly instead of polars for now to avoid conversion issues
                st.session_state.fibro_ema_data = pandas_df
                st.success("EMA data loaded successfully!")
            else:
                st.error("No data found in the EMA sheet.")
                return
        except Exception as e:
            st.error(f"Error loading EMA data: {str(e)}")
            st.exception(e)  # Show full traceback for debugging
            return
    
    # Get the data from session state
    df = st.session_state.fibro_ema_data
    
    # Get unique user IDs for filtering - handle potential None values
    try:
        if "User Id" in df.columns:
            # Using pandas methods instead of polars
            user_ids = sorted([id for id in df["User Id"].unique() if pd.notna(id) and id])
        else:
            st.warning("No 'User Id' column found in data")
            user_ids = []
    except Exception as e:
        st.warning(f"Error processing User IDs: {str(e)}")
        user_ids = []
    
    # User ID selection
    col1, col2 = st.columns([1, 3])
    with col1:
        selected_user = st.selectbox(
            "Select User ID", 
            options=["All Users"] + list(user_ids)
        )
    
    # Filter data based on user selection
    if selected_user != "All Users":
        # Using pandas filtering
        filtered_df = df[df["User Id"] == selected_user]
        st.write(f"Showing data for User: **{selected_user}** ({len(filtered_df)} records)")
    else:
        filtered_df = df
        st.write(f"Showing data for **All Users** ({len(filtered_df)} records)")
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Data Table", "Summary Statistics", "Visualizations"])
    
    with tab1:
        # Prepare data for display
        display_df = filtered_df
        
        # Format datetime for better readability
        if "Date Time" in display_df.columns:
            try:
                display_df["Formatted Date"] = pd.to_datetime(display_df["Date Time"]).dt.strftime("%Y-%m-%d %H:%M")
            except Exception as e:
                st.warning(f"Could not format DateTime: {str(e)}")
        
        # Create a more compact display for User Id (truncate if needed)
        if "User Id" in display_df.columns:
            display_df["User"] = display_df["User Id"].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
        
        # Select columns to display
        if st.checkbox("Show all columns", value=False):
            cols_to_display = display_df.columns
        else:
            # Define default columns to show
            default_cols = ["User", "Formatted Date"]
            # Add additional important columns that might exist
            for col in ["Pain Level", "Fatigue Level", "Mood", "Sleep Quality"]:
                if col in display_df.columns:
                    default_cols.append(col)
            
            cols_to_display = default_cols
        
        # Show the filtered dataframe
        if "Formatted Date" in cols_to_display:
            display_pd_df = display_df[cols_to_display].sort_values("Formatted Date", ascending=False)
        else:
            display_pd_df = display_df[cols_to_display]
        
        # st.dataframe(display_pd_df, use_container_width=True)
        # Use aggrid for better display
        aggrid_polars(pl.DataFrame(display_pd_df), key="main_data_table")    
        # Option to download data
        st.download_button(
            "Download Filtered Data",
            filtered_df.to_csv(index=False).encode("utf-8"),
            file_name=f"fibro_ema_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
    
    with tab2:
        st.subheader("Summary Statistics")
        
        # User submission counts using pandas
        user_counts = df["User Id"].value_counts().reset_index()
        user_counts.columns = ["User Id", "Submission Count"]
        
        st.write("#### Submissions by User")
        aggrid_polars(pl.DataFrame(user_counts), key="all_users_counts")    
        
        # Submissions by date using pandas
        if "Date Time" in df.columns:
            try:
                df["Date"] = pd.to_datetime(df["Date Time"]).dt.date
                date_counts = df["Date"].value_counts().reset_index()
                date_counts.columns = ["Date", "Submission Count"]
                date_counts = date_counts.sort_values("Date")
                
                st.write("#### Submissions by Date")
                aggrid_polars(pl.DataFrame(date_counts), key="date_counts")    
            except Exception as e:
                st.warning(f"Could not analyze by date: {str(e)}")
        
        # Show numeric column statistics if available
        numeric_cols = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col])]
        if numeric_cols:
            st.write("#### Numeric Data Statistics")
            stats_df = filtered_df[numeric_cols].describe()
            aggrid_polars(pl.DataFrame(stats_df), key="numeric_stats")  

            user_counts = filtered_df["User Id"].value_counts().reset_index()
            user_counts.columns = ["User Id", "Submission Count"]
            aggrid_polars(pl.DataFrame(user_counts), key="filtered_user_counts")
    
    with tab3:
        st.subheader("Data Visualizations")
        
        # Time series visualization
        if "Date Time" in filtered_df.columns and not filtered_df.empty:
            try:
                filtered_df["Date"] = pd.to_datetime(filtered_df["Date Time"]).dt.date
                
                # Submissions over time
                date_counts = filtered_df["Date"].value_counts().reset_index()
                date_counts.columns = ["Date", "Count"]
                date_counts = date_counts.sort_values("Date")
                
                fig1 = px.line(
                    date_counts, 
                    x="Date", 
                    y="Count", 
                    title=f"EMA Submissions Over Time {'(All Users)' if selected_user == 'All Users' else f'(User: {selected_user})'}"
                )
                st.plotly_chart(fig1, use_container_width=True)
                
                # If there are any numeric columns, create additional visualizations
                numeric_cols = [col for col in filtered_df.columns 
                              if pd.api.types.is_numeric_dtype(filtered_df[col])
                              and col not in ["User Id", "Date Time"]]
                
                if numeric_cols:
                    selected_metric = st.selectbox("Select metric to visualize:", numeric_cols)
                    
                    if selected_metric:
                        # Daily average of selected metric
                        daily_avg = filtered_df.groupby("Date")[selected_metric].mean().reset_index()
                        
                        fig2 = px.line(
                            daily_avg, 
                            x="Date", 
                            y=selected_metric,
                            title=f"Daily Average {selected_metric} {'(All Users)' if selected_user == 'All Users' else f'(User: {selected_user})'}"
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                        
                        # Distribution of selected metric
                        fig3 = px.histogram(
                            filtered_df, 
                            x=selected_metric,
                            title=f"Distribution of {selected_metric} Values"
                        )
                        st.plotly_chart(fig3, use_container_width=True)
            
            except Exception as e:
                st.error(f"Error creating visualization: {str(e)}")
        else:
            st.info("No datetime data available for time-based visualizations")


def fibro_appsheet_management(user_email, user_role, user_project, spreadsheet):
    """
    Main function for Fibromyalgia AppSheet management.
    Displays subject management and EMA data visualization tabs.
    
    Args:
        user_email (str): Current user's email
        user_role (str): Current user's role
        user_project (str): Current user's project
        spreadsheet: Spreadsheet object
    """
    st.title("FIBRO AppSheet Management")
    # Call the new EMA data visualization function
    display_fibro_ema_data(spreadsheet)
