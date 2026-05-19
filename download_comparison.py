#!/usr/bin/env python3
"""
Quick comparison test to show the difference between different initial download periods.
"""

from datetime import datetime, timedelta

def show_download_comparison():
    """Show the difference in download periods"""
    current_date = datetime.now().date()

    print("Fitbit Organizer Download Period Comparison")
    print("=" * 50)

    # Original 30-day approach
    start_30 = current_date - timedelta(days=30)
    print(f"OLD: 30-day initial download")
    print(f"  From: {start_30}")
    print(f"  To:   {current_date}")
    print(f"  Days: 30")
    print(f"  API calls per data type: ~30-60 (depending on endpoint)")
    print()

    # New 24-hour approach
    start_1 = current_date - timedelta(days=1)
    print(f"NEW: 24-hour initial download")
    print(f"  From: {start_1}")
    print(f"  To:   {current_date}")
    print(f"  Days: 1")
    print(f"  API calls per data type: ~1-2")
    print()

    # Benefits
    print("Benefits of 24-hour approach:")
    print("✓ 30x fewer API calls for initial setup")
    print("✓ Much faster first run")
    print("✓ Lower chance of hitting rate limits")
    print("✓ Still captures recent data for immediate monitoring")
    print("✓ Subsequent hourly runs will build up historical data")
    print()

    # Usage examples
    print("Usage Examples:")
    print("# Standard run (1 day initial)")
    print("python3 firbitfilesOrgenizer.py")
    print()
    print("# Custom initial period")
    print("python3 firbitfilesOrgenizer.py --initial-days 7")
    print()
    print("# Test mode")
    print("python3 firbitfilesOrgenizer.py --test-mode")

if __name__ == "__main__":
    show_download_comparison()
