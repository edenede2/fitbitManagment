#!/usr/bin/env python3
"""
Migrate the existing download tracking file to use datetime format
"""
import pandas as pd
from pathlib import Path

def migrate_tracking_file():
    """Migrate the tracking file from date to datetime format"""

    tracking_file = Path("data/download_tracking.csv")

    if not tracking_file.exists():
        print("No tracking file found - nothing to migrate")
        return

    print("Loading existing tracking file...")
    df = pd.read_csv(tracking_file)
    print(f"Loaded {len(df)} records")
    print("Original columns:", list(df.columns))

    if 'last_download_date' in df.columns and 'last_download_datetime' not in df.columns:
        print("Migrating from date to datetime format...")

        # Convert date to datetime (assume midnight)
        df['last_download_datetime'] = df['last_download_date'] + ' 00:00:00'

        # Drop the old column
        df = df.drop('last_download_date', axis=1)

        print("New columns:", list(df.columns))
        print("Sample data:")
        print(df.head())

        # Save the updated file
        df.to_csv(tracking_file, index=False)
        print(f"Migrated tracking file saved to {tracking_file}")

    elif 'last_download_datetime' in df.columns:
        print("File already in datetime format - no migration needed")

    else:
        print("Unexpected file format - creating new datetime format")
        new_df = pd.DataFrame(columns=['watch_name', 'data_type', 'last_download_datetime'])
        new_df.to_csv(tracking_file, index=False)
        print("Created new datetime-format tracking file")

if __name__ == "__main__":
    migrate_tracking_file()
