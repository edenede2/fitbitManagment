#!/usr/bin/env python3

import sys
from pathlib import Path
import datetime
import os
import traceback
import polars as pl
import pandas as pd  # Add explicit pandas import
import json  # Add import for watch status tracking

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# Add project root to Python path if necessary
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Import entity components directly (no Spreadsheet_io dependency)
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter, ServerLogFile, SheetFactory
from entity.Watch import Watch, WatchFactory
from dotenv import load_dotenv

def get_watch_status_history():
    """
    Load watch status history from a local JSON file.
    This tracks which watches were active in previous runs.
    
    Returns:
        dict: Dictionary mapping watch IDs to their previous status
    """
    status_file = Path(project_root) / "data" / "watch_status_history.json"
    
    if status_file.exists():
        try:
            with open(status_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading watch status file: {e}")
            return {}
    else:
        return {}

def save_watch_status_history(status_data):
    """
    Save watch status history to a local JSON file.
    
    Args:
        status_data (dict): Dictionary mapping watch IDs to their status
    """
    status_file = Path(project_root) / "data" / "watch_status_history.json"
    
    # Create directory if it doesn't exist
    status_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(status_file, 'w') as f:
            json.dump(status_data, f)
        print(f"Watch status history saved to {status_file}")
        return True
    except Exception as e:
        print(f"Error saving watch status file: {e}")
        return False

def analyze_whatsapp_messages():
    """
    Analyzes WhatsApp messages to find late responses and suspicious numbers.
    Updates the LateNums and SuspiciousNums sheets accordingly.
    """
    print(f"[{datetime.datetime.now()}] Starting WhatsApp message analysis...")
    
    try:
        # Load environment variables for API keys
        load_dotenv()
        
        # Get spreadsheet keys from environment
        spreadsheet_key = os.getenv("SPREADSHEET_KEY")
        bulldog_spreadsheet_key = os.getenv("BULLDOG_SPREADSHEET_KEY")
        
        if not spreadsheet_key or not bulldog_spreadsheet_key:
            print("Missing required spreadsheet keys in environment variables")
            return False
        
        # Create spreadsheet instances
        alert_spreadsheet = Spreadsheet(name="FitbitData", api_key=spreadsheet_key)
        GoogleSheetsAdapter.connect(alert_spreadsheet)
        
        whatsapp_spreadsheet = Spreadsheet(name="BulldogData", api_key=bulldog_spreadsheet_key)
        GoogleSheetsAdapter.connect(whatsapp_spreadsheet)
        
        # Get required sheets
        bulldog_sheet = whatsapp_spreadsheet.get_sheet("bulldog", sheet_type="bulldog")
        alert_sheet = alert_spreadsheet.get_sheet("EMA", sheet_type="EMA")
        
        # Get the existing suspicious and late number sheets to check for accepted entries
        existing_suspicious_nums = None
        existing_late_nums = None
        existing_suspicious_nums_df = None
        existing_late_nums_df = None
        
        try:
            if "suspicious_nums" in alert_spreadsheet.sheets:
                existing_suspicious_nums = alert_spreadsheet.get_sheet("suspicious_nums", sheet_type="suspicious_nums")
                existing_suspicious_nums_df = existing_suspicious_nums.to_dataframe(engine="polars")
                # Ensure accepted column is string type
                if 'accepted' in existing_suspicious_nums_df.columns:
                    existing_suspicious_nums_df = existing_suspicious_nums_df.with_columns(
                        pl.col('accepted').cast(pl.Utf8).alias('accepted')
                    )
                print(f"Loaded suspicious_nums sheet with schema: {existing_suspicious_nums_df.schema}")
            
            if "late_nums" in alert_spreadsheet.sheets:
                existing_late_nums = alert_spreadsheet.get_sheet("late_nums", sheet_type="late_nums")
                existing_late_nums_df = existing_late_nums.to_dataframe(engine="polars")
                # Ensure accepted column is string type
                if 'accepted' in existing_late_nums_df.columns:
                    existing_late_nums_df = existing_late_nums_df.with_columns(
                        pl.col('accepted').cast(pl.Utf8).alias('accepted')
                    )
                print(f"Loaded late_nums sheet with schema: {existing_late_nums_df.schema}")
        except Exception as e:
            print(f"Error retrieving existing number sheets: {e}")
            print(traceback.format_exc())
        
        # Get threshold from qualtrics_alerts_config or use default
        hours_threshold = 48  # Default threshold
        config_sheet = alert_spreadsheet.get_sheet("qualtrics_alerts_config", sheet_type="qualtrics_alerts_config")
        config_df = config_sheet.to_dataframe(engine="polars")
        
        if not config_df.is_empty() and 'hoursThr' in config_df.columns:
            try:
                hours_threshold = float(config_df.select(pl.col('hoursThr')).row(0)[0])
                print(f"Using hours threshold from config: {hours_threshold}")
            except (ValueError, IndexError) as e:
                print(f"Error reading threshold from config, using default: {e}")
        
        # Use AlertAnalyzer to identify late responses and suspicious numbers
        from entity.Sheet import AlertAnalyzer
        recent_messages, suspicious_numbers = AlertAnalyzer.analyze_whatsapp_messages(
            bulldog_sheet, alert_sheet, hours_threshold
        )
        
        # Prepare late responses data (messages within threshold but close to expiring)
        late_threshold = hours_threshold * 0.75  # Consider "late" when 75% of time has passed
        late_responses = recent_messages.filter(
            pl.col('hours_left') < (hours_threshold - late_threshold)
        ).select([ 'phone', 'time', 'hours_left'])
        
        # Format late responses for the LateNums sheet
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        late_nums_data = []
        for row in late_responses.iter_rows(named=True):
            # Skip if this number is already accepted in existing late_nums
            should_skip = False
            if existing_late_nums_df is not None and not existing_late_nums_df.is_empty():
                try:
                    # Safely filter with type handling
                    matches = existing_late_nums_df.filter(
                        (pl.col('nums').cast(pl.Utf8) == str(row['phone'])) & 
                        (pl.col('accepted').cast(pl.Utf8).str.to_uppercase() == 'TRUE')
                    )
                    if not matches.is_empty():
                        should_skip = True
                except Exception as e:
                    print(f"Error filtering late_nums: {e}")
                    # Continue without filtering
            
            if not should_skip:
                late_nums_data.append({
                    'nums': row['phone'],
                    'sentTime': row['time'],
                    'hoursLate': f"{row['hours_left']:.2f}",
                    'lastUpdated': now,
                    'accepted': 'FALSE'
                })
        
        # Format suspicious numbers for the SuspiciousNums sheet
        suspicious_nums_data = []
        for row in suspicious_numbers.iter_rows(named=True):
            # Skip if this number is already accepted in existing suspicious_nums
            should_skip = False
            if existing_suspicious_nums_df is not None and not existing_suspicious_nums_df.is_empty():
                try:
                    # Safely filter with type handling
                    matches = existing_suspicious_nums_df.filter(
                        (pl.col('nums').cast(pl.Utf8) == str(row['phone'])) & 
                        (pl.col('accepted').cast(pl.Utf8).str.to_uppercase() == 'TRUE')
                    )
                    if not matches.is_empty():
                        should_skip = True
                except Exception as e:
                    print(f"Error filtering suspicious_nums: {e}")
                    # Continue without filtering
            
            if not should_skip:
                # Check if 'endDate' exists, otherwise use current time
                filled_time = row.get('endDate', now) if hasattr(row, 'get') else now
                suspicious_nums_data.append({
                    'nums': row['phone'],
                    'filledTime': filled_time,
                    'lastUpdated': now,
                    'accepted': row.get('accepted', 'FALSE')
                })
        
        # Make sure we have the sheets before updating
        try:
            # Update LateNums sheet
            if late_nums_data:
                # Check if sheet exists and create if not
                if "late_nums" not in alert_spreadsheet.sheets:
                    print("Creating late_nums sheet")
                    late_nums_sheet = SheetFactory.create_sheet("late_nums", "late_nums")
                    alert_spreadsheet.sheets["late_nums"] = late_nums_sheet
                
                # Update sheet data
                alert_spreadsheet.sheets["late_nums"].data = late_nums_data
                GoogleSheetsAdapter.save(alert_spreadsheet, "late_nums")
                print(f"Updated LateNums sheet with {len(late_nums_data)} records")
            else:
                print("No late responses found")
            
            # Update SuspiciousNums sheet
            if suspicious_nums_data:
                # Check if sheet exists and create if not
                if "suspicious_nums" not in alert_spreadsheet.sheets:
                    print("Creating suspicious_nums sheet")
                    suspicious_nums_sheet = SheetFactory.create_sheet("suspicious_nums", "suspicious_nums")
                    alert_spreadsheet.sheets["suspicious_nums"] = suspicious_nums_sheet
                
                # Update sheet data
                alert_spreadsheet.sheets["suspicious_nums"].data = suspicious_nums_data 
                GoogleSheetsAdapter.save(alert_spreadsheet, "suspicious_nums")
                print(f"Updated SuspiciousNums sheet with {len(suspicious_nums_data)} records")
            else:
                print("No suspicious numbers found")
        except Exception as sheet_error:
            print(f"Error updating sheets: {sheet_error}")
            print(traceback.format_exc())
        
        # Generate a report
        report = AlertAnalyzer.generate_alert_report(recent_messages, suspicious_numbers)
        
        # Save report to file
        report_dir = Path(project_root) / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_file = report_dir / f"whatsapp_report_{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        with open(report_file, "w") as f:
            f.write(report)
        
        print(f"[{datetime.datetime.now()}] WhatsApp message analysis completed successfully")
        print(f"Report saved to {report_file}")
        return True
    
    except Exception as e:
        print(f"Error during WhatsApp message analysis: {e}")
        print(traceback.format_exc())
        return False
    
def get_watch_details() -> pl.DataFrame:
    """
    Fetches watch details from the spreadsheet and returns them as a Polars DataFrame.
    Only returns active watches.
    
    Returns:
        pl.DataFrame: A DataFrame containing active watch details.
    """
    # Load environment variables for API key
    load_dotenv()
    
    # Get spreadsheet key from environment
    spreadsheet_key = os.getenv("SPREADSHEET_KEY")
    if not spreadsheet_key:
        raise ValueError("SPREADSHEET_KEY not found in environment variables")
    
    # Create new Spreadsheet instance directly
    spreadsheet = Spreadsheet(name="FitbitData", api_key=spreadsheet_key)
    GoogleSheetsAdapter.connect(spreadsheet)
    
    # Get the fitbit sheet
    fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")
    
    # Convert to DataFrame and filter for active watches
    df = fitbit_sheet.to_dataframe(engine="pandas")
    
    # Ensure consistent column naming - rename 'name' column to match expected format
    if 'name' in df.columns and 'project' in df.columns:
        # Copy to avoid SettingWithCopyWarning
        df = df.copy()
        # Ensure both name and project columns exist and use consistent names
        print(f"DataFrame columns before: {df.columns.tolist()}")
    
    active_watches = df[df['isActive'].str.upper() != 'FALSE'].copy() if 'isActive' in df.columns else df
    
    # Log the result for debugging
    print(f"Found {len(active_watches)} active watches with columns: {active_watches.columns.tolist()}")
    
    # Convert to polars DataFrame for return
    return pl.from_pandas(active_watches)

def save_to_csv(data: pl.DataFrame) -> None:
    """
    Saves watch data to a CSV file, appending to existing data.
    
    Args:
        data (pl.DataFrame): The watch data to save.
    """
    # Create directory if it doesn't exist
    csv_dir = Path(project_root) / "data"
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert all data to strings to avoid type mismatches
    data_str = data.select([
        pl.col(col).cast(pl.Utf8) for col in data.columns
    ])
    
    # Create filename with today's date
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    csv_file = csv_dir / f"fitbit_data_{today}.csv"
    
    # Append or create CSV file
    if csv_file.exists():
        try:
            existing_data = pl.read_csv(csv_file)
            
            # Ensure column types are consistent by converting all to strings
            existing_data_str = existing_data.select([
                pl.col(col).cast(pl.Utf8) for col in existing_data.columns
            ])
            
            # Check if schemas match (column names)
            if set(existing_data_str.columns) != set(data_str.columns):
                print(f"Warning: Column mismatch between existing data and new data")
                # Align columns if needed
                common_cols = list(set(existing_data_str.columns).intersection(set(data_str.columns)))
                existing_data_str = existing_data_str.select(common_cols)
                data_str = data_str.select(common_cols)
            
            combined_data = pl.concat([existing_data_str, data_str], how="vertical")
            combined_data.write_csv(csv_file)
            print(f"Updated daily CSV file with {len(data)} new records")
        except Exception as e:
            print(f"Error appending to daily CSV: {e}")
            # If appending fails, just write the new data
            data_str.write_csv(csv_file)
            print(f"Created new daily CSV file with {len(data)} records")
    else:
        data_str.write_csv(csv_file)
        print(f"Created new daily CSV file with {len(data)} records")
    
    # Also save to a complete history file
    history_file = csv_dir / "fitbit_data_complete.csv"
    if history_file.exists():
        try:
            existing_data = pl.read_csv(history_file)
            
            # Ensure column types are consistent
            existing_data_str = existing_data.select([
                pl.col(col).cast(pl.Utf8) for col in existing_data.columns
            ])
            
            # Check if schemas match
            if set(existing_data_str.columns) != set(data_str.columns):
                print(f"Warning: Column mismatch between history data and new data")
                # Align columns if needed
                common_cols = list(set(existing_data_str.columns).intersection(set(data_str.columns)))
                existing_data_str = existing_data_str.select(common_cols)
                data_str = data_str.select(common_cols)
                
            combined_data = pl.concat([existing_data_str, data_str], how="vertical")
            combined_data.write_csv(history_file)
            print(f"Updated history CSV file with {len(data)} new records")
        except Exception as e:
            print(f"Error appending to history CSV: {e}")
            # If appending fails, just write the new data
            data_str.write_csv(history_file)
            print(f"Created new history CSV file with {len(data)} records")
    else:
        data_str.write_csv(history_file)
        print(f"Created new history CSV file with {len(data)} records")


def send_email_alert(recipient_email, subject, message_body):
    """
    Sends an email alert to the specified recipient.
    
    Args:
        recipient_email: Email address to send the alert to
        subject: Email subject line
        message_body: HTML content of the email
    
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Load SMTP configuration from environment variables
        load_dotenv()
        sender_email = os.getenv("SENDER_EMAIL_ADDRESS")
        sender_password = os.getenv("SENDER_EMAIL_PASSWORD")
        smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        
        if not sender_email or not sender_password:
            print("Missing email configuration in environment variables")
            return False
        
        # Create email message
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = recipient_email
        # message["To"] = "edenede2@gmail.com"
        
        # Create HTML version of the message
        html_part = MIMEText(message_body, "html")
        message.attach(html_part)
        
        # Connect to SMTP server and send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
            # server.sendmail(sender_email, "edenede2@gmail.com", message.as_string())
        
        print(f"Successfully sent email alert to {recipient_email}")
        return True
    
    except Exception as e:
        print(f"Error sending email alert: {e}")
        print(traceback.format_exc())
        return False

def is_end_date_passed(end_date_str):
    """
    Check if the given end date has passed.
    
    Args:
        end_date_str: End date string in various possible formats
        
    Returns:
        bool: True if end date has passed or cannot be parsed, False otherwise
    """
    if not end_date_str:
        # No end date means it never expires
        return False
        
    current_date = datetime.datetime.now().date()
    
    # Try different date formats
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            end_date = datetime.datetime.strptime(end_date_str, date_format).date()
            # Return True if current date is past the end date
            return current_date > end_date
        except ValueError:
            continue
    
    # If we couldn't parse the date, log a warning and assume it's not passed
    print(f"Warning: Could not parse end date '{end_date_str}'")
    return False

def get_student_email_for_watch(fitbit_data, watch_name):
    """
    Get student email for a specific watch if available.
    
    Args:
        fitbit_data: DataFrame containing fitbit data
        watch_name: Name of the watch to find student email for
        
    Returns:
        str: Student email or None if not found
    """
    if fitbit_data.is_empty():
        return None
        
    # Make sure both required columns exist
    if 'name' not in fitbit_data.columns or 'currentStudent' not in fitbit_data.columns:
        return None
    
    # Find the watch in the data
    matching_watches = fitbit_data.filter(pl.col('name') == watch_name)
    
    if matching_watches.is_empty():
        return None
    
    # Get the student email from the first match
    student_email = matching_watches.select('currentStudent').row(0)[0]
    
    # Only return if there's an actual value
    if student_email and str(student_email).strip():
        return str(student_email).strip()
    
    return None

def check_fitbit_alerts(log_data, config_data, fitbit_data=None):
    """
    Check Fitbit data against alert thresholds and send email alerts.
    Only processes the most recent log entry for each watch.
    
    Args:
        log_data: DataFrame containing Fitbit log data
        config_data: DataFrame containing alert configuration
        fitbit_data: Optional DataFrame containing Fitbit device data with student emails
        
    Returns:
        dict: Summary of alerts sent
    """
    alerts_sent = {}
    
    try:
        # Skip if either data frame is empty
        if log_data.is_empty() or config_data.is_empty():
            print("No data available for Fitbit alerts check")
            return alerts_sent
        
        # Current date for checking end dates
        current_date = datetime.datetime.now().date()
        
        # Get most recent log entry for each watch
        # First ensure lastCheck is properly formatted as datetime for sorting
        try:
            log_data = log_data.with_columns(
                pl.col('lastCheck').str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False)
            )
        except Exception as e:
            print(f"Warning: Could not convert lastCheck to datetime: {e}")
            # If conversion fails, keep original format
        
        # Group by watchName and get the most recent entry for each watch
        print("Finding most recent log entry for each watch...")
        
        # Create a dictionary to store the most recent entry for each watch
        most_recent_logs = {}
        watch_names = log_data.get_column('watchName').unique()
        
        for watch_name in watch_names:
            # Filter logs for this watch
            watch_logs = log_data.filter(pl.col('watchName') == watch_name)
            
            # Sort by lastCheck in descending order
            if 'lastCheck' in watch_logs.columns:
                try:
                    watch_logs = watch_logs.sort('lastCheck', descending=True)
                except:
                    # If sorting fails, try other approaches
                    try:
                        # Try converting to string first
                        watch_logs = watch_logs.with_columns(
                            pl.col('lastCheck').cast(pl.Utf8)
                        ).sort('lastCheck', descending=True)
                    except:
                        print(f"Warning: Could not sort logs by lastCheck for watch {watch_name}")
            
            # Get the first (most recent) row
            if not watch_logs.is_empty():
                most_recent_logs[watch_name] = watch_logs.row(0, named=True)
        
        print(f"Found most recent log entries for {len(most_recent_logs)} watches")
        
        # Process only the most recent log entry for each watch
        for watch_name, log_row in most_recent_logs.items():
            project = log_row.get('project', '')
            
            if not project:
                continue
                
            # Find the most specific configuration for this watch
            watch_specific_config = None
            project_config = None
            
            for config_row in config_data.iter_rows(named=True):
                if config_row.get('project', '') != project:
                    continue
                    
                # Check if end date has passed
                end_date = config_row.get('endDate', '')
                if is_end_date_passed(end_date):
                    continue
                
                # Check if this config is specific to this watch
                config_watch = config_row.get('watch', '')
                if config_watch and config_watch == watch_name:
                    # This is a watch-specific config
                    watch_specific_config = config_row
                    break  # Use this configuration and stop looking
                elif not config_watch:
                    # This is a project-wide config - save it but keep looking for watch-specific
                    project_config = config_row
            
            # Use watch-specific config if available, otherwise use project config
            config = watch_specific_config or project_config
            if not config:
                continue
                
            # Get thresholds from config
            current_sync_thr = int(config.get('currentSyncThr', 0) or 0)
            total_sync_thr = int(config.get('totalSyncThr', 0) or 0)
            current_hr_thr = int(config.get('currentHrThr', 0) or 0)
            total_hr_thr = int(config.get('totalHrThr', 0) or 0)
            current_sleep_thr = int(config.get('currentSleepThr', 0) or 0)
            total_sleep_thr = int(config.get('totalSleepThr', 0) or 0)
            current_steps_thr = int(config.get('currentStepsThr', 0) or 0)
            total_steps_thr = int(config.get('totalStepsThr', 0) or 0)
            battery_thr = int(config.get('batteryThr', 0) or 0)
            
            # Check if any threshold has been exceeded
            alert_needed = False
            
            if current_sync_thr > 0 and int(log_row.get('CurrentFailedSync', 0) or 0) >= current_sync_thr:
                alert_needed = True
            elif total_sync_thr > 0 and int(log_row.get('TotalFailedSync', 0) or 0) >= total_sync_thr:
                alert_needed = True
            elif current_hr_thr > 0 and int(log_row.get('CurrentFailedHR', 0) or 0) >= current_hr_thr:
                alert_needed = True
            elif total_hr_thr > 0 and int(log_row.get('TotalFailedHR', 0) or 0) >= total_hr_thr:
                alert_needed = True
            elif current_sleep_thr > 0 and int(log_row.get('CurrentFailedSleep', 0) or 0) >= current_sleep_thr:
                alert_needed = True
            elif total_sleep_thr > 0 and int(log_row.get('TotalFailedSleep', 0) or 0) >= total_sleep_thr:
                alert_needed = True
            elif current_steps_thr > 0 and int(log_row.get('CurrentFailedSteps', 0) or 0) >= current_steps_thr:
                alert_needed = True
            elif total_steps_thr > 0 and int(log_row.get('TotalFailedSteps', 0) or 0) >= total_steps_thr:
                alert_needed = True
            
            # Check battery level if available
            if battery_thr > 0 and log_row.get('lastBattaryVal', ''):
                try:
                    battery_level = int(log_row.get('lastBattaryVal', 100))
                    if battery_level <= battery_thr:
                        alert_needed = True
                except (ValueError, TypeError):
                    pass  # Skip battery check if value cannot be converted
            
            if alert_needed:
                # The rest of the function remains the same
                # Determine recipients
                recipients = []
                
                # First check if config has an email, otherwise use manager
                config_email = config.get('email', '')
                if config_email and config_email.strip():
                    recipients.append(config_email.strip())
                else:
                    manager_email = config.get('manager', '')
                    if manager_email and manager_email.strip():
                        recipients.append(manager_email.strip())
                
                # Add student email if available
                student_email = None
                if fitbit_data is not None:
                    student_email = get_student_email_for_watch(fitbit_data, watch_name)
                    if student_email:
                        recipients.append(student_email)
                
                # Only proceed if we have recipients
                if recipients:
                    # Create alert message
                    html = f"""
                    <html>
                    <head>
                        <style>
                            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                            .alert {{ color: #D8000C; background-color: #FFD2D2; padding: 10px; margin-bottom: 15px; }}
                            table {{ border-collapse: collapse; width: 100%; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                        </style>
                    </head>
                    <body>
                        <h2>Fitbit Alert: Watch {watch_name}</h2>
                        <p>The following watch has exceeded failure thresholds:</p>
                        
                        <table>
                            <tr>
                                <th>Watch Name</th>
                                <th>Project</th>
                                <th>Last Check</th>
                                <th>Last Sync</th>
                            </tr>
                            <tr>
                                <td>{watch_name}</td>
                                <td>{project}</td>
                                <td>{log_row.get('lastCheck', 'Unknown')}</td>
                                <td>{log_row.get('lastSynced', 'Unknown')}</td>
                            </tr>
                        </table>
                        
                        <h3>Alert Details</h3>
                        <table>
                            <tr>
                                <th>Metric</th>
                                <th>Current Failures</th>
                                <th>Total Failures</th>
                                <th>Threshold (Current/Total)</th>
                                <th>Last Value</th>
                            </tr>
                    """
                    
                    # Add sync information
                    if current_sync_thr > 0 or total_sync_thr > 0:
                        html += f"""
                        <tr>
                            <td>Sync</td>
                            <td>{log_row.get('CurrentFailedSync', 0)}</td>
                            <td>{log_row.get('TotalFailedSync', 0)}</td>
                            <td>{current_sync_thr}/{total_sync_thr}</td>
                            <td>{log_row.get('lastSynced', 'Unknown')}</td>
                        </tr>
                        """
                    
                    # Add HR information
                    if current_hr_thr > 0 or total_hr_thr > 0:
                        html += f"""
                        <tr>
                            <td>Heart Rate</td>
                            <td>{log_row.get('CurrentFailedHR', 0)}</td>
                            <td>{log_row.get('TotalFailedHR', 0)}</td>
                            <td>{current_hr_thr}/{total_hr_thr}</td>
                            <td>{log_row.get('lastHRVal', 'Unknown')}</td>
                        </tr>
                        """
                    
                    # Add Sleep information
                    if current_sleep_thr > 0 or total_sleep_thr > 0:
                        html += f"""
                        <tr>
                            <td>Sleep</td>
                            <td>{log_row.get('CurrentFailedSleep', 0)}</td>
                            <td>{log_row.get('TotalFailedSleep', 0)}</td>
                            <td>{current_sleep_thr}/{total_sleep_thr}</td>
                            <td>{log_row.get('lastSleepDur', 'Unknown')}</td>
                        </tr>
                        """
                    
                    # Add Steps information
                    if current_steps_thr > 0 or total_steps_thr > 0:
                        html += f"""
                        <tr>
                            <td>Steps</td>
                            <td>{log_row.get('CurrentFailedSteps', 0)}</td>
                            <td>{log_row.get('TotalFailedSteps', 0)}</td>
                            <td>{current_steps_thr}/{total_steps_thr}</td>
                            <td>{log_row.get('lastStepsVal', 'Unknown')}</td>
                        </tr>
                        """
                    
                    # Add Battery information
                    if battery_thr > 0:
                        html += f"""
                        <tr>
                            <td>Battery</td>
                            <td>N/A</td>
                            <td>N/A</td>
                            <td>{battery_thr}%</td>
                            <td>{log_row.get('lastBattaryVal', 'Unknown')}%</td>
                        </tr>
                        """
                    
                    # Close the HTML
                    html += """
                        </table>
                        
                        <p>This is an automated alert from the Fitbit Management System.</p>
                    </body>
                    </html>
                    """
                    
                    # Send the email
                    subject = f"Fitbit Alert: Watch {watch_name} in Project {project}"
                    result = send_email_alert(", ".join(recipients), subject, html)
                    
                    # Track results
                    if result:
                        if project not in alerts_sent:
                            alerts_sent[project] = {
                                'watches': [],
                                'recipients': [],
                                'count': 0
                            }
                            
                        alerts_sent[project]['watches'].append(watch_name)
                        alerts_sent[project]['recipients'] = list(set(alerts_sent[project]['recipients'] + recipients))
                        alerts_sent[project]['count'] += 1
                        
                        print(f"Sent alert for watch {watch_name} to {', '.join(recipients)}")
        
        # Summarize alerts sent
        if alerts_sent:
            print("Alert summary:")
            for project, details in alerts_sent.items():
                print(f"  Project {project}: {details['count']} alerts sent to {len(details['recipients'])} recipients")
        
        return alerts_sent
        
    except Exception as e:
        print(f"Error checking Fitbit alerts: {e}")
        print(traceback.format_exc())
        return alerts_sent

def check_qualtrics_alerts(suspicious_numbers, config_data):
    """
    Check Qualtrics data against alert thresholds and send email alerts.
    
    Args:
        suspicious_numbers: DataFrame containing suspicious numbers
        config_data: DataFrame containing alert configuration
        
    Returns:
        dict: Summary of alerts sent
    """
    alerts_sent = {}
    
    try:
        # Only consider numbers with 'accepted' set to FALSE - case insensitive check
        suspicious_numbers = suspicious_numbers.filter(
            pl.col('accepted').str.to_uppercase() == 'FALSE'
        )
        
        # Skip if either data frame is empty
        if suspicious_numbers.is_empty() or config_data.is_empty():
            print("No data available for Qualtrics alerts check")
            return alerts_sent
        
        # Group data by project
        project_configs = {}
        for row in config_data.iter_rows(named=True):
            project = row.get('project', '')
            if not project:
                continue
                
            # Store config for this project
            project_configs[project] = {
                'hoursThr': float(row.get('hoursThr', 48) or 48),  # Default to 48 hours
                'manager': row.get('manager', '')
            }
        
        # For each project configuration
        for project, config in project_configs.items():
            manager_email = config['manager']
            hours_threshold = config['hoursThr']
            
            if not manager_email:
                print(f"No manager email configured for project: {project}")
                continue
            
            # Filter suspicious numbers based on time passed
            current_time = datetime.datetime.now()
            
            # Create HTML report if there are suspicious numbers
            if suspicious_numbers.height > 0:
                html = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                        .alert {{ color: #D8000C; background-color: #FFD2D2; padding: 10px; margin-bottom: 15px; }}
                        table {{ border-collapse: collapse; width: 100%; }}
                        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                        th {{ background-color: #f2f2f2; }}
                        tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    </style>
                </head>
                <body>
                    <h2>Qualtrics Alert: Project {project}</h2>
                    <p>The following phone numbers have not received messages within {hours_threshold} hours of completing the survey:</p>
                    
                    <table>
                        <tr>
                            <th>Phone Number</th>
                            <th>Survey Completion Time</th>
                        </tr>
                """
                
                # Add each suspicious number to the table
                for row in suspicious_numbers.iter_rows(named=True):
                    html += f"""
                    <tr>
                        <td>{row['nums']}</td>
                        <td>{row.get('filledTime', 'Unknown')}</td>
                    </tr>
                    """
                
                # Close the HTML
                html += """
                    </table>
                    <p>This is an automated alert from the Fitbit Management System.</p>
                </body>
                </html>
                """
                
                # Send the email
                subject = f"Qualtrics Alert: Project {project} has {suspicious_numbers.height} unreached respondents"
                result = send_email_alert(manager_email, subject, html)
                
                # Track results
                if result:
                    alerts_sent[project] = {
                        'manager': manager_email,
                        'suspicious_numbers': suspicious_numbers.height
                    }
        
        return alerts_sent
        
    except Exception as e:
        print(f"Error checking Qualtrics alerts: {e}")
        print(traceback.format_exc())
        return alerts_sent

def check_late_nums_alerts(late_numbers, config_data):
    """
    Check late response numbers against alert thresholds and send email alerts.
    
    Args:
        late_numbers: DataFrame containing late response numbers
        config_data: DataFrame containing alert configuration
        
    Returns:
        dict: Summary of alerts sent
    """
    alerts_sent = {}
    
    try:
        # Only consider numbers with 'accepted' set to FALSE - case insensitive check
        late_numbers = late_numbers.filter(
            pl.col('accepted').str.to_uppercase() == 'FALSE'
        )
        
        # Skip if either data frame is empty
        if late_numbers.is_empty() or config_data.is_empty():
            print("No data available for late numbers alerts check")
            return alerts_sent
        
        # Group data by project
        project_configs = {}
        for row in config_data.iter_rows(named=True):
            project = row.get('project', '')
            if not project:
                continue
                
            # Store config for this project
            project_configs[project] = {
                'hoursThr': float(row.get('hoursThr', 48) or 48),  # Default to 48 hours
                'manager': row.get('manager', '')
            }
        
        # For each project configuration
        for project, config in project_configs.items():
            manager_email = config['manager']
            hours_threshold = config['hoursThr']
            
            if not manager_email:
                print(f"No manager email configured for project: {project}")
                continue
            
            # Create HTML report if there are late numbers
            if late_numbers.height > 0:
                html = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                        .alert {{ color: #D8000C; background-color: #FFD2D2; padding: 10px; margin-bottom: 15px; }}
                        table {{ border-collapse: collapse; width: 100%; }}
                        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                        th {{ background-color: #f2f2f2; }}
                        tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    </style>
                </head>
                <body>
                    <h2>Late Response Alert: Project {project}</h2>
                    <p>The following phone numbers are approaching the {hours_threshold} hour threshold for WhatsApp responses:</p>
                    
                    <table>
                        <tr>
                            <th>Phone Number</th>
                            <th>Message Sent Time</th>
                            <th>Hours Left</th>
                        </tr>
                """
                
                # Add each late number to the table
                for row in late_numbers.iter_rows(named=True):
                    html += f"""
                    <tr>
                        <td>{row['nums']}</td>
                        <td>{row.get('sentTime', 'Unknown')}</td>
                        <td>{row.get('hoursLate', 'Unknown')}</td>
                    </tr>
                    """
                
                # Close the HTML
                html += """
                    </table>
                    <p>This is an automated alert from the Fitbit Management System.</p>
                </body>
                </html>
                """
                
                # Send the email
                subject = f"Late Response Alert: Project {project} has {late_numbers.height} pending responses"
                result = send_email_alert(manager_email, subject, html)
                
                # Track results
                if result:
                    alerts_sent[project] = {
                        'manager': manager_email,
                        'late_numbers': late_numbers.height
                    }
        
        return alerts_sent
        
    except Exception as e:
        print(f"Error checking late numbers alerts: {e}")
        print(traceback.format_exc())
        return alerts_sent

def hourly_data_collection():
    """Main function to run hourly data collection and alerts"""
    print(f"[{datetime.datetime.now()}] Starting hourly data collection...")
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Get spreadsheet key from environment
        spreadsheet_key = os.getenv("SPREADSHEET_KEY")
        if not spreadsheet_key:
            print("Missing SPREADSHEET_KEY in environment variables")
            return
        
        # Create spreadsheet instances
        spreadsheet = Spreadsheet(name="FitbitData", api_key=spreadsheet_key)
        GoogleSheetsAdapter.connect(spreadsheet)
        
        # Step 1: Get watch data and previous status history
        watch_data = get_watch_details()
        previous_status = get_watch_status_history()
        
        if not watch_data.is_empty():
            # Create current status mapping
            current_status = {}
            
            # We need a unique identifier for each watch to track status
            # First check if 'id' column exists, otherwise use a combination of project and watchName
            id_column = 'id' if 'id' in watch_data.columns else 'deviceId'
            
            # Map of watch ID to activity status
            for row in watch_data.iter_rows(named=True):
                watch_id = str(row.get(id_column, ''))
                if not watch_id and 'project' in watch_data.columns and 'name' in watch_data.columns:
                    watch_id = f"{row.get('project', '')}-{row.get('name', '')}"
                
                watch_name = row.get('name', row.get('watchName', ''))
                is_active = str(row.get('isActive', '')).upper() != 'FALSE'
                
                current_status[watch_id] = {
                    'active': is_active,
                    'name': watch_name
                }
            
            # Identify watches that became inactive since last run
            newly_inactive_watches = []
            for watch_id, status in previous_status.items():
                # If watch was active before but is now inactive or not present
                if status.get('active', False) and (
                    watch_id not in current_status or not current_status[watch_id].get('active', False)
                ):
                    newly_inactive_watches.append(watch_id)
            
            if newly_inactive_watches:
                print(f"Detected {len(newly_inactive_watches)} watches that became inactive")
                for watch_id in newly_inactive_watches:
                    print(f"Watch {previous_status[watch_id].get('name', watch_id)} became inactive - will reset failure counters")
            
            # Save to CSV for historical tracking
            save_to_csv(watch_data)
            
            # Update log using ServerLogFile - passing inactive watches to reset their counters
            log_file = ServerLogFile()
            result = log_file.update_fitbits_log(watch_data, reset_total_for_watches=newly_inactive_watches)
            
            # Save the current status for the next run
            save_watch_status_history(current_status)
            
            if result:
                print(f"[{datetime.datetime.now()}] Successfully updated log data")
                
                # Get statistics about watch failures
                stats = log_file.get_summary_statistics()
                if stats:
                    print("Watch Status Summary:")
                    print(f"  Total watches: {stats.get('total_watches', 0)}")
                    print(f"  Watches with sync failures: {stats.get('sync_failures', 0)}")
                    print(f"  Watches with heart rate failures: {stats.get('hr_failures', 0)}")
                    print(f"  Watches with sleep failures: {stats.get('sleep_failures', 0)}")
                    print(f"  Watches with steps failures: {stats.get('steps_failures', 0)}")
                    print(f"  Watches with battery failures: {stats.get('battery_failures', 0)}")
                    print(f"  Total watches with any failure: {stats.get('total_failures', 0)}")
            else:
                print(f"[{datetime.datetime.now()}] Failed to update log data")
                
            print(f"Data collection completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("No active watches found or error retrieving data")
        
        # Step 2: Run WhatsApp message analysis
        suspicious_nums_data = analyze_whatsapp_messages()
        
        # Step 3: Get alert configurations and fitbit data
        fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", sheet_type="fitbit_alerts_config")
        qualtrics_config_sheet = spreadsheet.get_sheet("qualtrics_alerts_config", sheet_type="qualtrics_alerts_config")
        fitbit_sheet = spreadsheet.get_sheet("fitbit", sheet_type="fitbit")  # Get fitbit sheet for student emails
        
        fitbit_config_data = fitbit_config_sheet.to_dataframe(engine="polars")
        qualtrics_config_data = qualtrics_config_sheet.to_dataframe(engine="polars")
        fitbit_data = fitbit_sheet.to_dataframe(engine="polars")
        
        # Create a unified manager email mapping for all projects
        # Priority: use fitbit config emails as primary source if available
        manager_emails = {}
        
        # First populate from fitbit config
        if not fitbit_config_data.is_empty():
            for row in fitbit_config_data.iter_rows(named=True):
                project = row.get('project', '')
                manager = row.get('manager', '')
                if project and manager:
                    manager_emails[project] = manager
        
        # Then add any missing projects from qualtrics config
        if not qualtrics_config_data.is_empty():
            for row in qualtrics_config_data.iter_rows(named=True):
                project = row.get('project', '')
                manager = row.get('manager', '')
                if project and manager and project not in manager_emails:
                    manager_emails[project] = manager
        
        print(f"Consolidated manager emails for {len(manager_emails)} projects")
        
        # Step 4: Get log data for Fitbit alerts
        log_sheet = spreadsheet.get_sheet("FitbitLog", sheet_type="log")
        log_data = log_sheet.to_dataframe(engine="polars")
        
        # Step 5: Check alerts and send emails - passing fitbit_data for student emails
        if not log_data.is_empty() and not fitbit_config_data.is_empty():
            # Update manager emails in config before sending alerts to ensure consistency
            for i in range(len(fitbit_config_data)):
                project = fitbit_config_data.row(i)[fitbit_config_data.columns.index('project')]
                if project in manager_emails:
                    fitbit_config_data = fitbit_config_data.with_columns(
                        pl.when(pl.col('project') == project)
                        .then(pl.lit(manager_emails[project]))
                        .otherwise(pl.col('manager'))
                        .alias('manager')
                    )
            
            fitbit_alerts = check_fitbit_alerts(log_data, fitbit_config_data, fitbit_data)
            
            if fitbit_alerts:
                print(f"Sent Fitbit alerts for {len(fitbit_alerts)} projects")
            else:
                print("No Fitbit alerts sent")
        
        # Step 6: Check Qualtrics alerts - suspicious numbers
        if not qualtrics_config_data.is_empty():
            # Update manager emails in qualtrics config to be consistent
            for i in range(len(qualtrics_config_data)):
                project = qualtrics_config_data.row(i)[qualtrics_config_data.columns.index('project')]
                if project in manager_emails:
                    qualtrics_config_data = qualtrics_config_data.with_columns(
                        pl.when(pl.col('project') == project)
                        .then(pl.lit(manager_emails[project]))
                        .otherwise(pl.col('manager'))
                        .alias('manager')
                    )
            
            # Get suspicious numbers from previous analysis
            suspicious_numbers_sheet = spreadsheet.get_sheet("suspicious_nums", sheet_type="suspicious_nums")
            suspicious_numbers = suspicious_numbers_sheet.to_dataframe(engine="polars")
            
            if not suspicious_numbers.is_empty():
                qualtrics_alerts = check_qualtrics_alerts(suspicious_numbers, qualtrics_config_data)
                
                if qualtrics_alerts:
                    print(f"Sent Qualtrics alerts to {len(qualtrics_alerts)} managers")
                else:
                    print("No Qualtrics alerts sent")
        
        # Step 7: Check Late Numbers alerts (using the same manager emails as other alerts)
        try:
            late_numbers_sheet = spreadsheet.get_sheet("late_nums", sheet_type="late_nums")
            late_numbers = late_numbers_sheet.to_dataframe(engine="polars")
            
            if not late_numbers.is_empty() and not qualtrics_config_data.is_empty():
                late_nums_alerts = check_late_nums_alerts(late_numbers, qualtrics_config_data)
                
                if late_nums_alerts:
                    print(f"Sent Late Numbers alerts to {len(late_nums_alerts)} managers")
                else:
                    print("No Late Numbers alerts sent")
        except Exception as late_error:
            print(f"Error processing late numbers: {late_error}")
        
        print(f"[{datetime.datetime.now()}] Hourly data collection and alerts completed")
        
    except Exception as e:
        print(f"Error during data collection: {e}")
        print(traceback.format_exc())

def main():
    """Entry point function"""
    try:
        hourly_data_collection()
        print(f"[{datetime.datetime.now()}] Data collection completed successfully!")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error during data collection process:")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
