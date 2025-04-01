import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

import polars as pl
from typing import Optional, Dict, Any

# Now the imports should work
from Spreadsheet_io.sheets import Spreadsheet, serverLogFile, fitbitLog
from Watch.Watch import Watch

import streamlit as st
import datetime

def get_watch_details() -> pl.DataFrame:
    """
    Fetches watch details from the spreadsheet and returns them as a Polars DataFrame.
    
    Returns:
        pl.DataFrame: A DataFrame containing watch details.
    """
    SP = Spreadsheet.get_instance()
    watch_details = SP.get_fitbits_details()
    
    # Define the schema to avoid type mismatches
    schema = {
        'project': pl.Utf8,
        'name': pl.Utf8,
        'syncDate': pl.Utf8,
        'battery': pl.Utf8,
        'HR': pl.Utf8,
        'steps': pl.Utf8,
        'sleep_start': pl.Utf8,
        'sleep_end': pl.Utf8,
        'sleep_duration': pl.Utf8,
        'isActive': pl.Utf8,
    }
    
    # Initialize with empty DataFrame with proper schema
    new_rows = pl.DataFrame(schema=schema)
    
    for row in watch_details:
        if row['isActive'] == 'FALSE':
            continue
        watch = Watch(row)
        
        # Convert all values to strings to maintain type consistency
        watch_dict = {
            'project': str(watch.get_project() or ""),
            'name': str(watch.get_name() or ""),
            'syncDate': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'battery': str(watch.get_current_battery() or ""),
            'HR': str(watch.get_current_hourly_HR() or ""),
            'steps': str(watch.get_current_hourly_steps() or ""),
            'sleep_start': str(watch.get_last_sleep_start_end()[0] or ""),
            'sleep_end': str(watch.get_last_sleep_start_end()[1] or ""),
            'sleep_duration': str(watch.get_last_sleep_duration() or ""),
            'isActive': str(watch.get_is_active() or ""),
        }
        
        # Create DataFrame with the same schema
        row_df = pl.DataFrame([watch_dict], schema=schema)
        
        # Concatenate with consistent schemas
        new_rows = pl.concat([new_rows, row_df], how="vertical")
        
    return new_rows

def update_log() -> None:
    """
    Updates the log of a specific watch in the spreadsheet.
    """
    SP = Spreadsheet.get_instance()
    watch_data = get_watch_details()
    fb_log = serverLogFile()
    fb_log.update_fitbits_log(watch_data)

def save_to_csv(data: pl.DataFrame) -> None:
    """
    Saves watch data to a CSV file, appending to existing data.
    
    Args:
        data (pl.DataFrame): The watch data to save.
    """
    # Create directory if it doesn't exist
    csv_dir = Path("/home/psylab-6028/fitbitmanagment/fitbitManagment/data")
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

def update_worksheet_3(data: pl.DataFrame) -> None:
    """
    Updates worksheet 3 in the spreadsheet with the latest watch data.
    
    Args:
        data (pl.DataFrame): The latest watch data.
    """
    SP = Spreadsheet.get_instance()
    
    # Get current time for timestamps
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get existing log data to access failure counters
    log_path = st.secrets.get("fitbit_log_path", "fitbit_log.csv")
    existing_failure_data = {}
    
    if os.path.exists(log_path):
        try:
            log_df = pl.read_csv(log_path)
            # Create a dictionary with watch IDs as keys and failure counts as values
            for row in log_df.iter_rows(named=True):
                watch_id = row.get("ID", "")
                if watch_id:
                    existing_failure_data[watch_id] = {
                        "CurrentFailedSync": int(row.get("CurrentFailedSync", 0)),
                        "TotalFailedSync": int(row.get("TotalFailedSync", 0)),
                        "CurrentFailedHR": int(row.get("CurrentFailedHR", 0)),
                        "TotalFailedHR": int(row.get("TotalFailedHR", 0)),
                        "CurrentFailedSleep": int(row.get("CurrentFailedSleep", 0)),
                        "TotalFailedSleep": int(row.get("TotalFailedSleep", 0)),
                        "CurrentFailedSteps": int(row.get("CurrentFailedSteps", 0)),
                        "TotalFailedSteps": int(row.get("TotalFailedSteps", 0))
                    }
        except Exception as e:
            print(f"Error reading log file: {e}")
    
    # Transform data to match expected column structure
    transformed_data = []
    
    for row in data.iter_rows(named=True):
        # Create watch ID
        watch_id = f"{row.get('project', '')}-{row.get('name', '')}"
        
        # Get existing failure counts or use defaults
        failure_data = existing_failure_data.get(watch_id, {})
        
        # Calculate current failure counters
        curr_failed_sync = 0 if row.get("syncDate") else failure_data.get("CurrentFailedSync", 0) + 1
        total_failed_sync = failure_data.get("TotalFailedSync", 0) + (0 if row.get("syncDate") else 1)
        
        curr_failed_hr = 0 if row.get("HR") else failure_data.get("CurrentFailedHR", 0) + 1
        total_failed_hr = failure_data.get("TotalFailedHR", 0) + (0 if row.get("HR") else 1)
        
        curr_failed_sleep = 0 if (row.get("sleep_start") and row.get("sleep_end")) else failure_data.get("CurrentFailedSleep", 0) + 1
        total_failed_sleep = failure_data.get("TotalFailedSleep", 0) + (0 if (row.get("sleep_start") and row.get("sleep_end")) else 1)
        
        curr_failed_steps = 0 if row.get("steps") else failure_data.get("CurrentFailedSteps", 0) + 1
        total_failed_steps = failure_data.get("TotalFailedSteps", 0) + (0 if row.get("steps") else 1)
        
        watch_dict = {
            "project": row.get("project", ""),
            "watchName": row.get("name", ""),
            "lastCheck": now,
            "lastSynced": row.get("syncDate", now),
            "lastBattary": now if row.get("battery") else "",
            "lastHR": now if row.get("HR") else "",
            "lastSleepStartDateTime": row.get("sleep_start", ""),
            "lastSleepEndDateTime": row.get("sleep_end", ""),
            "lastSteps": now if row.get("steps") else "",
            "lastBattaryVal": row.get("battery", ""),
            "lastHRVal": row.get("HR", ""),
            "lastHRSeq": "",  # Would need to calculate or get from another source
            "lastSleepDur": row.get("sleep_duration", ""),
            "lastStepsVal": row.get("steps", ""),
            "CurrentFailedSync": curr_failed_sync,
            "TotalFailedSync": total_failed_sync,
            "CurrentFailedHR": curr_failed_hr,
            "TotalFailedHR": total_failed_hr,
            "CurrentFailedSleep": curr_failed_sleep,
            "TotalFailedSleep": total_failed_sleep,
            "CurrentFailedSteps": curr_failed_steps,
            "TotalFailedSteps": total_failed_steps,
            "ID": watch_id
        }
        transformed_data.append(watch_dict)
    
    # Update the spreadsheet
    SP.update_worksheet_3(transformed_data)

def hourly_data_collection() -> None:
    """
    Main function to collect watch data hourly, save to CSV, and update the spreadsheet.
    Should be scheduled to run every hour.
    """
    # Get watch data
    watch_data = get_watch_details()
    
    if not watch_data.is_empty():
        # Save data to CSV for historical purposes
        save_to_csv(watch_data)
        
        # Update worksheet 3 with the latest data
        update_worksheet_3(watch_data)
        
        # Update log
        update_log()
        
        print(f"Data collection completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("No active watches found or error retrieving data")

if __name__ == "__main__":
    # When run directly, perform hourly data collection
    hourly_data_collection()

