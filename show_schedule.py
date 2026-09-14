#!/usr/bin/env python3
"""
Visual schedule showing the timing of both Fitbit scripts.
"""

from datetime import datetime, timedelta

def show_schedule():
    """Show the schedule for both scripts over a few hours"""
    print("Wearable Research Manager - Hourly Schedule")
    print("=" * 60)
    print()

    # Start from current hour
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    print("TIME     | SCRIPT                    | PURPOSE")
    print("-" * 60)

    for i in range(6):  # Show next 6 hours
        # Top of hour - run_data_collection.py
        time_str = current_time.strftime("%H:%M")
        print(f"{time_str}     | run_data_collection.py    | Monitoring & Alerts")

        # 30 minutes past - firbitfilesOrgenizer.py
        half_hour = current_time + timedelta(minutes=30)
        time_str = half_hour.strftime("%H:%M")
        print(f"{time_str}     | firbitfilesOrgenizer.py   | Data Downloads")

        print("-" * 60)
        current_time += timedelta(hours=1)

    print()
    print("Benefits of this schedule:")
    print("✓ No resource conflicts between scripts")
    print("✓ 30-minute separation allows each script to complete")
    print("✓ Data collection happens after monitoring checks")
    print("✓ Both scripts run hourly as needed")
    print()
    print("Cronjob entries:")
    print("0 * * * *  → run_data_collection.py")
    print("30 * * * * → firbitfilesOrgenizer.py")

if __name__ == "__main__":
    show_schedule()
