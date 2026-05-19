#!/usr/bin/env python3
"""
Test script to verify the fitbit organizer can run properly.
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from dotenv import load_dotenv
import os

def test_environment():
    """Test if environment variables are loaded correctly."""
    print("Testing environment variables...")

    # Load environment variables
    load_dotenv()

    # Check for required environment variable
    spreadsheet_key = os.getenv("SPREADSHEET_KEY")
    if spreadsheet_key:
        print(f"✓ SPREADSHEET_KEY found (length: {len(spreadsheet_key)})")
        return True
    else:
        print("✗ SPREADSHEET_KEY not found in environment variables")
        return False

def test_imports():
    """Test if all required imports work."""
    print("Testing imports...")

    try:
        from firbitfilesOrgenizer import get_active_watches, load_download_tracking
        print("✓ Main functions imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_directory_access():
    """Test if the save directory is accessible."""
    print("Testing directory access...")

    save_path = Path("/media/psylab-6028/DATA")

    if save_path.exists():
        if save_path.is_dir():
            print(f"✓ Save directory exists and is accessible: {save_path}")
            return True
        else:
            print(f"✗ Save path exists but is not a directory: {save_path}")
            return False
    else:
        print(f"✗ Save directory does not exist: {save_path}")
        print("Creating directory...")
        try:
            save_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created save directory: {save_path}")
            return True
        except Exception as e:
            print(f"✗ Failed to create save directory: {e}")
            return False

def test_tracking_file():
    """Test if tracking file can be created."""
    print("Testing tracking file...")

    try:
        from firbitfilesOrgenizer import load_download_tracking, save_download_tracking
        tracking_df = load_download_tracking()
        print(f"✓ Tracking file loaded successfully ({len(tracking_df)} records)")

        # Test saving
        save_download_tracking(tracking_df)
        print("✓ Tracking file saved successfully")
        return True
    except Exception as e:
        print(f"✗ Error with tracking file: {e}")
        return False

def test_spreadsheet_connection():
    """Test if we can connect to the spreadsheet."""
    print("Testing spreadsheet connection...")

    try:
        from firbitfilesOrgenizer import get_active_watches
        active_watches = get_active_watches()
        print(f"✓ Successfully connected to spreadsheet, found {len(active_watches)} active watches")
        return True
    except Exception as e:
        print(f"✗ Error connecting to spreadsheet: {e}")
        return False

def test_script_options():
    """Test the script's command line options."""
    print("Testing script command line options...")

    try:
        import subprocess

        # Test help option
        result = subprocess.run(['python3', 'firbitfilesOrgenizer.py', '--help'],
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and 'initial-days' in result.stdout:
            print("✓ Command line options working correctly")
            return True
        else:
            print("✗ Command line options not working")
            return False
    except Exception as e:
        print(f"✗ Error testing command line options: {e}")
        return False

def main():
    """Run all tests."""
    print("Running Fitbit Organizer Tests...")
    print("=" * 50)

    tests = [
        test_environment,
        test_imports,
        test_directory_access,
        test_tracking_file,
        test_spreadsheet_connection,
        test_script_options
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
            results.append(False)
        print()

    print("=" * 50)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")

    if passed == total:
        print("✓ All tests passed! The fitbit organizer should work correctly.")
        return 0
    else:
        print("✗ Some tests failed. Please check the configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
