# Fitbit Data Collection System

## Overview

The `firbitfilesOrgenizer.py` script has been modified to run as an hourly cronjob that automatically downloads Fitbit data from active watches and saves them as ZIP files. This system integrates with the existing Google Sheets infrastructure and provides incremental data collection with minimal API usage.

## Key Features

1. **Hourly Execution**: Runs every hour at 30 minutes past the hour (avoids conflicts with run_data_collection.py)
2. **Active Watch Detection**: Automatically retrieves active watches from Google Sheets
3. **Smart Initial Downloads**: Only downloads 24 hours of data for new watches (configurable)
4. **Incremental Downloads**: Only downloads new data since the last run
5. **ZIP File Management**: Appends new data to existing ZIP files or creates new ones
6. **Rate Limit Compliance**: Minimizes API requests to stay within Fitbit rate limits
7. **Tracking System**: Maintains CSV tracking of download history for each watch/data type
8. **Error Handling**: Comprehensive error handling and logging
9. **Flexible Configuration**: Customizable initial download periods for different scenarios

## Data Types Collected

The system collects the following data types for each active watch:

- **Sleep Data**: Monthly resolution (30-day chunks)
- **Steps**: Daily resolution
- **Heart Rate**: Daily resolution (intraday)
- **HRV (Heart Rate Variability)**: Daily resolution
- **Temperature**: Monthly resolution (30-day chunks)
- **Breathing Rate**: Monthly resolution (30-day chunks)
- **Calories**: Daily resolution

## File Structure

```
/home/psylab-6028/fitbitmanagment/fitbitManagment/
├── firbitfilesOrgenizer.py          # Main script (modified for cronjob)
├── test_fitbit_organizer.py         # Test script to verify setup
├── setup_cronjob.sh                 # Cronjob setup script
├── data/
│   └── download_tracking.csv        # Tracks last download dates
├── logs/
│   └── fitbit_organizer.log         # Execution logs
└── .env                             # Environment variables

/media/psylab-6028/DATA/             # ZIP file storage location
├── [watch_name_1]_fitbit_data.zip
├── [watch_name_2]_fitbit_data.zip
└── ...
```

## Setup Instructions

### 1. Prerequisites

Ensure all dependencies are installed:
```bash
cd /home/psylab-6028/fitbitmanagment/fitbitManagment
pip install -r requirements.txt
```

### 2. Environment Variables

The script uses the same `.env` file as the existing system. Ensure it contains:
```
SPREADSHEET_KEY=your_google_sheets_key
```

### 3. Test the Setup

Run the test script to verify everything is configured correctly:
```bash
cd /home/psylab-6028/fitbitmanagment/fitbitManagment
python3 test_fitbit_organizer.py
```

This should show all tests passing and connect to the spreadsheet.

### 4. Setup the Cronjob

Run the setup script to configure the hourly cronjob:
```bash
cd /home/psylab-6028/fitbitmanagment/fitbitManagment
./setup_cronjob.sh
```

This will:
- Create the logs directory
- Add the cronjob entry to run every hour at 30 minutes past the hour
- Make the script executable
- Display the current crontab

## Scheduling Details

The Fitbit Organizer is scheduled to run at **30 minutes past each hour** (e.g., 01:30, 02:30, 03:30...) to avoid conflicts with the existing `run_data_collection.py` script that runs at the top of each hour (XX:00).

**Schedule Overview:**
- **00:00** - `run_data_collection.py` (monitoring and alerts)
- **00:30** - `firbitfilesOrgenizer.py` (data downloads)
- **01:00** - `run_data_collection.py` (monitoring and alerts)
- **01:30** - `firbitfilesOrgenizer.py` (data downloads)
- And so on...

This ensures both scripts have dedicated time slots and don't interfere with each other.

### 5. Manual Testing

To manually test the script:
```bash
cd /home/psylab-6028/fitbitmanagment/fitbitManagment
python3 firbitfilesOrgenizer.py
```

#### Test with Different Initial Download Periods

To test with different initial download periods:
```bash
# Test with only 1 day (24 hours) - recommended for testing
python3 firbitfilesOrgenizer.py --initial-days 1

# Test with 7 days for a more complete initial dataset
python3 firbitfilesOrgenizer.py --initial-days 7

# Run in test mode (automatically uses 1 day)
python3 firbitfilesOrgenizer.py --test-mode
```

#### Available Command Line Options

- `--initial-days N`: Set number of days for initial download (default: 1)
- `--test-mode`: Run in test mode with minimal downloads (1 day)
- `--help`: Show all available options

## How It Works

### 1. Active Watch Detection

The script connects to the Google Sheets spreadsheet and retrieves all watches where `isActive != 'FALSE'`. It extracts:
- Watch name
- Fitbit API token
- Project information

### 2. Download Tracking

For each watch, the script maintains a tracking CSV with columns:
- `watch_name`: Name of the watch
- `data_type`: Type of data (sleep, steps, heart_rate, etc.)
- `last_download_date`: Date of last successful download

### 3. Incremental Downloads

For each watch and data type:
- **First run**: Downloads data from the last 24 hours only (configurable, default: 1 day)
- **Subsequent runs**: Downloads only new data since last download

This approach minimizes API usage and reduces the risk of hitting rate limits during initial setup.

### 4. Rate Limit Management

The script implements several strategies to minimize API usage:
- Groups data by resolution (daily vs monthly)
- Uses appropriate date ranges for each endpoint
- Adds delays between API calls
- Skips downloads when no new data is available

### 5. ZIP File Management

For each watch, the script:
- Creates a ZIP file named `[watch_name]_fitbit_data.zip`
- Appends new data to existing ZIP files
- Maintains the original directory structure within the ZIP

## Monitoring and Maintenance

### Log Monitoring

View real-time logs:
```bash
tail -f /home/psylab-6028/fitbitmanagment/fitbitManagment/logs/fitbit_organizer.log
```

### Check Cronjob Status

View current cronjobs:
```bash
crontab -l
```

### Download Tracking

View the download tracking file:
```bash
cat /home/psylab-6028/fitbitmanagment/fitbitManagment/data/download_tracking.csv
```

### ZIP File Location

Check generated ZIP files:
```bash
ls -la /media/psylab-6028/DATA/
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure the script is executable
   ```bash
   chmod +x /home/psylab-6028/fitbitmanagment/fitbitManagment/firbitfilesOrgenizer.py
   ```

2. **Environment Variables Not Found**: Check the `.env` file exists and contains required variables

3. **Google Sheets Connection Failed**: Verify the service account credentials and spreadsheet key

4. **Storage Directory Not Accessible**: Ensure `/media/psylab-6028/DATA` exists and is writable

### Manual Cronjob Setup

If the automatic setup doesn't work, manually add to crontab:
```bash
crontab -e
```

Add this line:
```
30 * * * * cd /home/psylab-6028/fitbitmanagment/fitbitManagment && /usr/bin/python3 /home/psylab-6028/fitbitmanagment/fitbitManagment/firbitfilesOrgenizer.py >> /home/psylab-6028/fitbitmanagment/fitbitManagment/logs/fitbit_organizer.log 2>&1
```

**Cron Schedule Explanation:**
- `30 * * * *` = At 30 minutes past every hour
- This avoids conflicts with `run_data_collection.py` which runs at `0 * * * *` (top of each hour)

## Configuration

### Changing Initial Download Period

The default initial download period is 24 hours (1 day) for new watches. To change this globally, modify the `INITIAL_DOWNLOAD_DAYS` variable in `firbitfilesOrgenizer.py`:

```python
INITIAL_DOWNLOAD_DAYS = 1  # Number of days to download for first-time watches (24 hours)
```

You can also specify this at runtime:
```bash
# Download 7 days for new watches
python3 firbitfilesOrgenizer.py --initial-days 7

# Test mode (always uses 1 day)
python3 firbitfilesOrgenizer.py --test-mode
```

### Changing Data Types

To modify which data types are collected, edit the `DATA_TYPE_CONFIGS` dictionary in `firbitfilesOrgenizer.py`:

```python
DATA_TYPE_CONFIGS = {
    'sleep': {
        'endpoint': 'Sleep',
        'resolution': 'monthly',
        'max_days_per_request': 30,
        'folder': 'Sleep'
    },
    # Add or modify data types here
}
```

### Changing Download Frequency

To change from hourly at :30 to a different frequency, modify the cronjob:
- Every 30 minutes: `*/30 * * * *` (but this may conflict with run_data_collection.py)
- Every 2 hours at :30: `30 */2 * * *`
- Every 4 hours at :30: `30 */4 * * *`
- Daily at 2:30 AM: `30 2 * * *`

**Note**: Keep the 30-minute offset to avoid conflicts with `run_data_collection.py`

### Changing Storage Location

To change where ZIP files are saved, modify the `FITBIT_ZIP_SAVE_PATH` variable in `firbitfilesOrgenizer.py`:

```python
FITBIT_ZIP_SAVE_PATH = Path("/your/new/storage/path")
```

## Security Considerations

1. **API Tokens**: Tokens are retrieved from the Google Sheets and used temporarily
2. **File Permissions**: Ensure only authorized users can access the ZIP files
3. **Log Security**: Logs may contain sensitive information - secure appropriately
4. **Environment Variables**: Keep the `.env` file secure with proper permissions

## Performance Notes

- The script processes one watch at a time to avoid overwhelming the API
- API calls include small delays to respect rate limits
- ZIP file operations use temporary directories to minimize disk usage
- The tracking system prevents redundant downloads

## Integration with Existing System

This system is designed to work alongside the existing `run_data_collection.py` script:
- Both use the same Google Sheets infrastructure
- Both respect the same active watch filtering
- The fitbit organizer focuses on raw data collection
- The run_data_collection script focuses on monitoring and alerts

This ensures data integrity and prevents conflicts between the two systems.
