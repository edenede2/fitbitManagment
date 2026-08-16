#!/usr/bin/env python3
"""
Test the datetime tracking functionality
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

def test_datetime_tracking():
    """Test the updated datetime tracking functionality"""

    print("Testing datetime tracking functionality...")

    # Simulate old tracking data (date format)
    old_tracking_data = {
        'watch_name': ['nova027', 'nova028'],
        'data_type': ['steps', 'sleep'],
        'last_download_date': ['2025-06-30', '2025-06-29']
    }
    old_df = pd.DataFrame(old_tracking_data)

    print("Old tracking data:")
    print(old_df)
    print()

    # Simulate migration to datetime format
    print("Migrating to datetime format...")
    if 'last_download_date' in old_df.columns:
        old_df['last_download_datetime'] = old_df['last_download_date'] + ' 00:00:00'
        old_df = old_df.drop('last_download_date', axis=1)

    print("Migrated tracking data:")
    print(old_df)
    print()

    # Test parsing datetime values
    print("Testing datetime parsing:")
    for idx, row in old_df.iterrows():
        dt_str = row['last_download_datetime']
        parsed_dt = pd.to_datetime(dt_str)
        print(f"  {row['watch_name']} {row['data_type']}: {dt_str} -> {parsed_dt}")

    print()

    # Test adding new datetime record
    print("Testing new datetime record:")
    current_dt = datetime.now()
    new_row = pd.DataFrame({
        'watch_name': ['nova029'],
        'data_type': ['heart_rate'],
        'last_download_datetime': [current_dt.strftime('%Y-%m-%d %H:%M:%S')]
    })

    updated_df = pd.concat([old_df, new_row], ignore_index=True)
    print("Updated tracking data:")
    print(updated_df)

    print("\nTest completed successfully!")

if __name__ == "__main__":
    test_datetime_tracking()
