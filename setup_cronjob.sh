#!/bin/bash
# Cronjob setup script for Fitbit Data Collection
# This script sets up the hourly cronjob for Fitbit data collection

# Configuration
SCRIPT_PATH="/home/psylab-6028/fitbitmanagment/fitbitManagment/firbitfilesOrgenizer.py"
LOG_PATH="/home/psylab-6028/fitbitmanagment/fitbitManagment/logs"
PYTHON_PATH="/usr/bin/python3"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_PATH"

# Create the cronjob entry
CRONJOB_ENTRY="30 * * * * cd /home/psylab-6028/fitbitmanagment/fitbitManagment && $PYTHON_PATH $SCRIPT_PATH >> $LOG_PATH/fitbit_organizer.log 2>&1"

echo "Setting up cronjob for Fitbit data collection..."

# Check if cronjob already exists
if crontab -l 2>/dev/null | grep -F "$SCRIPT_PATH" > /dev/null; then
    echo "Cronjob already exists. Updating..."
    # Remove existing entry and add new one
    (crontab -l 2>/dev/null | grep -v "$SCRIPT_PATH"; echo "$CRONJOB_ENTRY") | crontab -
else
    echo "Adding new cronjob..."
    # Add new entry to existing crontab
    (crontab -l 2>/dev/null; echo "$CRONJOB_ENTRY") | crontab -
fi

echo "Cronjob setup complete!"
echo "The script will run every hour at 30 minutes past the hour (e.g., 01:30, 02:30, 03:30...)"
echo "This avoids conflicts with run_data_collection.py which runs on the hour"
echo "Logs will be written to: $LOG_PATH/fitbit_organizer.log"

# Display current crontab
echo ""
echo "Current crontab entries:"
crontab -l

# Make the script executable
chmod +x "$SCRIPT_PATH"

echo ""
echo "Setup completed successfully!"
echo ""
echo "SCHEDULING:"
echo "• Fitbit Organizer: Runs at 30 minutes past each hour (XX:30)"
echo "• Run Data Collection: Runs at the top of each hour (XX:00)"
echo "This prevents scheduling conflicts between the two scripts."
echo ""
echo "To manually test the script, run:"
echo "cd /home/psylab-6028/fitbitmanagment/fitbitManagment && python3 firbitfilesOrgenizer.py"
echo ""
echo "To monitor the logs, run:"
echo "tail -f $LOG_PATH/fitbit_organizer.log"
