#!/usr/bin/env python3
"""
Test the complete datetime tracking system
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

def test_tracking_system():
    """Test the complete tracking system with datetime support"""

    print("Testing complete datetime tracking system...")

    try:
        # Test import
        from firbitfilesOrgenizer import (
            load_download_tracking,
            get_last_download_date,
            update_download_tracking,
            save_download_tracking
        )
        print("✓ Successfully imported tracking functions")

        # Test loading tracking data
        tracking_df = load_download_tracking()
        print(f"✓ Loaded tracking data: {len(tracking_df)} records")
        print(f"  Columns: {list(tracking_df.columns)}")

        # Test getting last download datetime
        if len(tracking_df) > 0:
            sample_watch = tracking_df.iloc[0]['watch_name']
            sample_data_type = tracking_df.iloc[0]['data_type']
            last_download_dt = get_last_download_date(tracking_df, sample_watch, sample_data_type)
            print(f"✓ Retrieved last download for {sample_watch} {sample_data_type}: {last_download_dt}")
            print(f"  Type: {type(last_download_dt)}")

            if last_download_dt:
                print(f"  Can extract date: {last_download_dt.date()}")

        # Test updating tracking with new datetime
        test_datetime = datetime.now()
        updated_df = update_download_tracking(tracking_df, "test_watch", "test_data", test_datetime)
        print(f"✓ Updated tracking data: {len(updated_df)} records")

        # Find the test record
        test_mask = (updated_df['watch_name'] == 'test_watch') & (updated_df['data_type'] == 'test_data')
        if test_mask.any():
            test_record = updated_df[test_mask].iloc[0]
            print(f"  Test record datetime: {test_record['last_download_datetime']}")

        print("\n✅ All datetime tracking tests passed!")

    except Exception as e:
        print(f"❌ Error in datetime tracking test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_tracking_system()
