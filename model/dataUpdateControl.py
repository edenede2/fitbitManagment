import polars as pl
from typing import Optional, Dict, Any

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
            'sleep_duration': watch.get_last_sleep_duration()
        }
        new_rows = pl.concat([new_rows, pl.DataFrame(watch_dict)], how="vertical")
    return new_rows

def update_log(watch_data: Dict[str, Any], row_index: int) -> None:
    """
    Updates the log of a specific watch in the spreadsheet.
    
    Args:
        watch_data (Dict[str, Any]): The data to update in the log.
        row_index (int): The index of the row to update.
    """
    SP = Spreadsheet.get_instance()
    get_watch_details
    fb_log = fitbitLog()

    