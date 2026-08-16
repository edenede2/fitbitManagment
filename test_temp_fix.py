#!/usr/bin/env python3
"""
Test the temporary directory fix
"""
import sys
from pathlib import Path

# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tempfile import TemporaryDirectory

def test_temp_directory_lifecycle():
    """Test the improved temporary directory handling"""

    print("Testing temporary directory lifecycle...")

    # Simulate the old approach (problematic)
    def old_approach():
        temp_dir = TemporaryDirectory()
        temp_path = Path(temp_dir.name)
        temp_path.mkdir(exist_ok=True)
        print(f"Old approach - temp_path exists: {temp_path.exists()}")
        return temp_path  # Returns path but temp_dir goes out of scope

    # Simulate the new approach (fixed)
    def new_approach():
        temp_dir = TemporaryDirectory()
        temp_path = Path(temp_dir.name)
        temp_path.mkdir(exist_ok=True)
        print(f"New approach - temp_path exists: {temp_path.exists()}")
        return temp_dir, temp_path  # Returns both objects

    print("\n=== Testing Old Approach ===")
    old_path = old_approach()
    print(f"After function return - old_path exists: {old_path.exists()}")

    print("\n=== Testing New Approach ===")
    new_temp_dir, new_path = new_approach()
    print(f"After function return - new_path exists: {new_path.exists()}")

    # Clean up
    new_temp_dir.cleanup()
    print(f"After cleanup - new_path exists: {new_path.exists()}")

    print("\nTest completed!")

if __name__ == "__main__":
    test_temp_directory_lifecycle()
