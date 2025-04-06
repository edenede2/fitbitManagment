#!/usr/bin/env python3

import sys
from pathlib import Path
import datetime
import os
import traceback
import polars as pl
import pandas as pd  # Add explicit pandas import

# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Import entity components directly (no Spreadsheet_io dependency)
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, ServerLogFile
from entity.Watch import Watch, WatchFactory
from dotenv import load_dotenv

def get_watch_details() -> pl.DataFrame:
    """
    Fetches watch details from the spreadsheet and returns them as a Polars DataFrame.
    Only returns active watches.
    
    Returns:
        pl.DataFrame: A DataFrame containing active watch details.
    """
    # Load environment variables for API key
    load_dotenv()
    
    # Get spreadsheet key from environment
    spreadsheet_key = os.getenv("SPREADSHEET_KEY")
    if not spreadsheet_key:
        raise ValueError("SPREADSHEET_KEY not found in environment variables")
    
    # Create new Spreadsheet instance directly
    spreadsheet = Spreadsheet(name="FitbitData", api_key=spreadsheet_key)
    GoogleSheetsAdapter.connect(spreadsheet)
    
    # Get the fitbit sheet
    fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")
    
    # Convert to DataFrame and filter for active watches
    df = fitbit_sheet.to_dataframe(engine="pandas")
    
    # Ensure consistent column naming - rename 'name' column to match expected format
    if 'name' in df.columns and 'project' in df.columns:
        # Copy to avoid SettingWithCopyWarning
        df = df.copy()
        # Ensure both name and project columns exist and use consistent names
        print(f"DataFrame columns before: {df.columns.tolist()}")
    
    active_watches = df[df['isActive'].str.upper() != 'FALSE'].copy() if 'isActive' in df.columns else df
    
    # Log the result for debugging
    print(f"Found {len(active_watches)} active watches with columns: {active_watches.columns.tolist()}")
    
    # Convert to polars DataFrame for return
    return pl.from_pandas(active_watches)

def save_to_csv(data: pl.DataFrame) -> None:
    """
    Saves watch data to a CSV file, appending to existing data.
    
    Args:
        data (pl.DataFrame): The watch data to save.
    """
    # Create directory if it doesn't exist
    csv_dir = Path(project_root) / "data"
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert all data to strings to avoid type mismatches
    data_str = data.select([
        pl.col(col).cast(pl.Utf8) for col in data.columns
    ])
    
    # Create filename with today's date
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    csv_file = csv_dir / f"fitbit_data_{today}.csv"
    
    # Append or create CSV file
    if csv_file.exists():
        try:
            existing_data = pl.read_csv(csv_file)
            
            # Ensure column types are consistent by converting all to strings
            existing_data_str = existing_data.select([
                pl.col(col).cast(pl.Utf8) for col in existing_data.columns
            ])
            
            # Check if schemas match (column names)
            if set(existing_data_str.columns) != set(data_str.columns):
                print(f"Warning: Column mismatch between existing data and new data")
                # Align columns if needed
                common_cols = list(set(existing_data_str.columns).intersection(set(data_str.columns)))
                existing_data_str = existing_data_str.select(common_cols)
                data_str = data_str.select(common_cols)
            
            combined_data = pl.concat([existing_data_str, data_str], how="vertical")
            combined_data.write_csv(csv_file)
            print(f"Updated daily CSV file with {len(data)} new records")
        except Exception as e:
            print(f"Error appending to daily CSV: {e}")
            # If appending fails, just write the new data
            data_str.write_csv(csv_file)
            print(f"Created new daily CSV file with {len(data)} records")
    else:
        data_str.write_csv(csv_file)
        print(f"Created new daily CSV file with {len(data)} records")
    
    # Also save to a complete history file
    history_file = csv_dir / "fitbit_data_complete.csv"
    if history_file.exists():
        try:
            existing_data = pl.read_csv(history_file)
            
            # Ensure column types are consistent
            existing_data_str = existing_data.select([
                pl.col(col).cast(pl.Utf8) for col in existing_data.columns
            ])
            
            # Check if schemas match
            if set(existing_data_str.columns) != set(data_str.columns):
                print(f"Warning: Column mismatch between history data and new data")
                # Align columns if needed
                common_cols = list(set(existing_data_str.columns).intersection(set(data_str.columns)))
                existing_data_str = existing_data_str.select(common_cols)
                data_str = data_str.select(common_cols)
                
            combined_data = pl.concat([existing_data_str, data_str], how="vertical")
            combined_data.write_csv(history_file)
            print(f"Updated history CSV file with {len(data)} new records")
        except Exception as e:
            print(f"Error appending to history CSV: {e}")
            # If appending fails, just write the new data
            data_str.write_csv(history_file)
            print(f"Created new history CSV file with {len(data)} records")
    else:
        data_str.write_csv(history_file)
        print(f"Created new history CSV file with {len(data)} records")

def hourly_data_collection():
    """Main function to run hourly data collection"""
    print(f"[{datetime.datetime.now()}] Starting hourly data collection...")
    
    try:
        # Get watch data
        watch_data = get_watch_details()
        
        if not watch_data.is_empty():
            # Save to CSV for historical tracking
            save_to_csv(watch_data)
            
            # Update log using ServerLogFile
            log_file = ServerLogFile()
            result = log_file.update_fitbits_log(watch_data)
            
            if result:
                print(f"[{datetime.datetime.now()}] Successfully updated log data")
                
                # Get statistics about watch failures
                stats = log_file.get_summary_statistics()
                if stats:
                    print("Watch Status Summary:")
                    print(f"  Total watches: {stats.get('total_watches', 0)}")
                    print(f"  Watches with sync failures: {stats.get('sync_failures', 0)}")
                    print(f"  Watches with heart rate failures: {stats.get('hr_failures', 0)}")
                    print(f"  Watches with sleep failures: {stats.get('sleep_failures', 0)}")
                    print(f"  Watches with steps failures: {stats.get('steps_failures', 0)}")
                    print(f"  Total watches with any failure: {stats.get('total_failures', 0)}")
            else:
                print(f"[{datetime.datetime.now()}] Failed to update log data")
                
            print(f"Data collection completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("No active watches found or error retrieving data")
    
    except Exception as e:
        print(f"Error during data collection: {traceback.format_exc()}")
        raise

def main():
    """Entry point function"""
    try:
        hourly_data_collection()
        print(f"[{datetime.datetime.now()}] Data collection completed successfully!")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error during data collection process:")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
