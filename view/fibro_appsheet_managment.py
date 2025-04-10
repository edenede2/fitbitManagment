import streamlit as st
import pandas as pd
import polars as pl
from datetime import datetime
import plotly.express as px
from entity.Sheet import GoogleSheetsAdapter, Spreadsheet, Sheet

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
    with st.spinner("Loading EMA data from Google Sheets..."):
        try:
            # First get data as pandas to handle mixed types better
            pandas_df = spreadsheet.get_sheet("for_analysis", "for_analysis").to_dataframe(engine="pandas")
            
            if pandas_df is not None and not pandas_df.empty:
                # Clean data before converting to polars
                # Replace empty strings with None in all object columns
                for col in pandas_df.select_dtypes(include=['object']).columns:
                    pandas_df[col] = pandas_df[col].replace('', None)
                
                # Convert to polars with explicit schema inference on all rows
                df = pl.from_pandas(pandas_df)
                
                st.session_state.fibro_ema_data = df
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
            user_ids = sorted([id for id in df.select("User Id").unique().to_series().to_list() if id])
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
        filtered_df = df.filter(pl.col("User Id") == selected_user)
        st.write(f"Showing data for User: **{selected_user}** ({filtered_df.height} records)")
    else:
        filtered_df = df
        st.write(f"Showing data for **All Users** ({filtered_df.height} records)")
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Data Table", "Summary Statistics", "Visualizations"])
    
    with tab1:
        # Prepare data for display
        # Polars operations are immutable by default, no need for copy()
        display_df = filtered_df
        
        # Format datetime for better readability
        if "Date Time" in display_df.columns:
            try:
                display_df = display_df.with_columns([
                    pl.col("Date Time").cast(pl.Datetime).alias("Date Time"),
                    pl.col("Date Time").cast(pl.Datetime).dt.strftime("%Y-%m-%d %H:%M").alias("Formatted Date")
                ])
            except Exception as e:
                st.warning(f"Could not format DateTime: {str(e)}")
        
        # Create a more compact display for User Id (truncate if needed)
        if "User Id" in display_df.columns:
            display_df = display_df.with_columns([
                pl.when(pl.col("User Id").str.lengths() > 10)
                .then(pl.col("User Id").str.slice(0, 10) + "...")
                .otherwise(pl.col("User Id"))
                .alias("User")
            ])
        
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
            # Sort by date and select columns
            display_pd_df = (display_df
                .select(cols_to_display)
                .sort("Formatted Date", descending=True)
                .to_pandas())
        else:
            display_pd_df = display_df.select(cols_to_display).to_pandas()
            
        st.dataframe(display_pd_df, use_container_width=True)
        
        # Option to download data
        # Convert to pandas for CSV export (simpler)
        pandas_df = filtered_df.to_pandas()
        st.download_button(
            "Download Filtered Data",
            pandas_df.to_csv(index=False).encode("utf-8"),
            file_name=f"fibro_ema_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
    
    with tab2:
        st.subheader("Summary Statistics")
        
        # User submission counts using Polars
        user_counts = (df
            .group_by("User Id")
            .agg(pl.count().alias("Submission Count"))
            .sort("Submission Count", descending=True)
            .to_pandas())
        
        st.write("#### Submissions by User")
        st.dataframe(user_counts, use_container_width=True)
        
        # Submissions by date using Polars
        if "Date Time" in df.columns:
            try:
                date_df = df.with_columns([
                    pl.col("Date Time").cast(pl.Datetime).dt.date().alias("Date")
                ])
                
                date_counts = (date_df
                    .group_by("Date")
                    .agg(pl.count().alias("Submission Count"))
                    .sort("Date")
                    .to_pandas())
                
                st.write("#### Submissions by Date")
                st.dataframe(date_counts, use_container_width=True)
            except Exception as e:
                st.warning(f"Could not analyze by date: {str(e)}")
        
        # Show numeric column statistics if available
        numeric_cols = [col for col in filtered_df.columns if filtered_df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
        if numeric_cols:
            st.write("#### Numeric Data Statistics")
            # Convert to pandas for describe() method
            stats_df = filtered_df.select(numeric_cols).to_pandas().describe()
            st.dataframe(stats_df, use_container_width=True)
    
    with tab3:
        st.subheader("Data Visualizations")
        
        # Time series visualization
        if "Date Time" in filtered_df.columns and not filtered_df.is_empty():
            try:
                # Prepare data with Polars
                viz_df = filtered_df.with_columns([
                    pl.col("Date Time").cast(pl.Datetime).dt.date().alias("Date")
                ])
                
                # Submissions over time
                date_counts = (viz_df
                    .group_by("Date")
                    .agg(pl.count().alias("Count"))
                    .sort("Date")
                    .to_pandas())
                
                fig1 = px.line(
                    date_counts, 
                    x="Date", 
                    y="Count", 
                    title=f"EMA Submissions Over Time {'(All Users)' if selected_user == "All Users" else f'(User: {selected_user})'}"
                )
                st.plotly_chart(fig1, use_container_width=True)
                
                # If there are any numeric columns, create additional visualizations
                numeric_cols = [col for col in viz_df.columns 
                              if viz_df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]
                              and col not in ["User Id", "Date Time"]]
                
                if numeric_cols:
                    selected_metric = st.selectbox("Select metric to visualize:", numeric_cols)
                    
                    if selected_metric:
                        # Daily average of selected metric
                        daily_avg = (viz_df
                            .group_by("Date")
                            .agg(pl.mean(selected_metric).alias(selected_metric))
                            .sort("Date")
                            .to_pandas())
                        
                        fig2 = px.line(
                            daily_avg, 
                            x="Date", 
                            y=selected_metric,
                            title=f"Daily Average {selected_metric} {'(All Users)' if selected_user == "All Users" else f'(User: {selected_user})'}"
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                        
                        # Distribution of selected metric
                        # Convert to pandas for histogram (simpler with plotly)
                        viz_pandas_df = viz_df.select(selected_metric).to_pandas()
                        fig3 = px.histogram(
                            viz_pandas_df, 
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
