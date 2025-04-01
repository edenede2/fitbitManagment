import polars as pl
from typing import Optional, Dict, Any
import os
from pathlib import Path

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
    
    new_rows = pl.DataFrame()

    for row in watch_details:
        if row['isActive'] == 'FALSE':
            continue
        watch = Watch(row)

        watch_dict = {
            'project': watch.get_project(),
            'name': watch.get_name(),
            'syncDate': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'battery': watch.get_current_battery(),
            'HR': watch.get_current_hourly_HR(),
            'steps': watch.get_current_hourly_steps(),
            'sleep_start': watch.get_last_sleep_start_end()[0],
            'sleep_end': watch.get_last_sleep_start_end()[1],
            'sleep_duration': watch.get_last_sleep_duration(),
            'isActive': watch.get_is_active(),
        }
        if new_rows.empty:
            new_rows = pl.DataFrame(watch_dict)
        else:
            new_rows = pl.concat([new_rows, pl.DataFrame(watch_dict)], how="vertical")
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
    
    # Create filename with today's date
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    csv_file = csv_dir / f"fitbit_data_{today}.csv"
    
    # Append or create CSV file
    if csv_file.exists():
        existing_data = pl.read_csv(csv_file)
        combined_data = pl.concat([existing_data, data], how="vertical")
        combined_data.write_csv(csv_file)
    else:
        data.write_csv(csv_file)
    
    # Also save to a complete history file
    history_file = csv_dir / "fitbit_data_complete.csv"
    if history_file.exists():
        existing_data = pl.read_csv(history_file)
        combined_data = pl.concat([existing_data, data], how="vertical")
        combined_data.write_csv(history_file)
    else:
        data.write_csv(history_file)

def update_worksheet_3(data: pl.DataFrame) -> None:
    """
    Updates worksheet 3 in the spreadsheet with the latest watch data.
    
    Args:
        data (pl.DataFrame): The latest watch data.
    """
    SP = Spreadsheet.get_instance()
    # Convert Polars DataFrame to list of dictionaries for the spreadsheet
    data_list = data.to_dicts()
    SP.update_worksheet_3(data_list)

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

