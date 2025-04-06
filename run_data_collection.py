#!/usr/bin/env python3

import sys
from pathlib import Path
import datetime
import os
import traceback
import polars as pl
import pandas as pd  # Add explicit pandas import

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
        alert_sheet = alert_spreadsheet.get_sheet("qualtrics_nova", sheet_type="qualtrics_nova")
        
        # Get threshold from qualtrics_alert_config or use default
        hours_threshold = 48  # Default threshold
        config_sheet = alert_spreadsheet.get_sheet("qualtrics_alert_config", sheet_type="qualtrics_alert_config")
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
            late_nums_data.append({
                'nums': row['phone'],
                'sentTime': row['time'],
                'hoursLate': f"{row['hours_left']:.2f}",
                'lastUpdated': now
            })
        
        # Format suspicious numbers for the SuspiciousNums sheet
        suspicious_nums_data = []
        for row in suspicious_numbers.iter_rows(named=True):
            # Check if 'endDate' exists, otherwise use current time
            filled_time = row.get('endDate', now) if hasattr(row, 'get') else now
            suspicious_nums_data.append({
                'nums': row['phone'],
                'filledTime': filled_time,
                'lastUpdated': now
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
        
        # Create HTML version of the message
        html_part = MIMEText(message_body, "html")
        message.attach(html_part)
        
        # Connect to SMTP server and send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        
        print(f"Successfully sent email alert to {recipient_email}")
        return True
    
    except Exception as e:
        print(f"Error sending email alert: {e}")
        print(traceback.format_exc())
        return False

def check_fitbit_alerts(log_data, config_data):
    """
    Check Fitbit data against alert thresholds and send email alerts.
    
    Args:
        log_data: DataFrame containing Fitbit log data
        config_data: DataFrame containing alert configuration
        
    Returns:
        dict: Summary of alerts sent
    """
    alerts_sent = {}
    
    try:
        # Skip if either data frame is empty
        if log_data.is_empty() or config_data.is_empty():
            print("No data available for Fitbit alerts check")
            return alerts_sent
        
        # Group data by project
        project_configs = {}
        for row in config_data.iter_rows(named=True):
            project = row.get('project', '')
            if not project:
                continue
                
            # Store config for this project
            project_configs[project] = {
                'currentSyncThr': int(row.get('currentSyncThr', 0) or 0),
                'totalSyncThr': int(row.get('totalSyncThr', 0) or 0),
                'currentHrThr': int(row.get('currentHrThr', 0) or 0),
                'totalHrThr': int(row.get('totalHrThr', 0) or 0),
                'currentSleepThr': int(row.get('currentSleepThr', 0) or 0),
                'totalSleepThr': int(row.get('totalSleepThr', 0) or 0),
                'currentStepsThr': int(row.get('currentStepsThr', 0) or 0),
                'totalStepsThr': int(row.get('totalStepsThr', 0) or 0),
                'batteryThr': int(row.get('batteryThr', 0) or 0),
                'manager': row.get('manager', '')
            }
        
        # Process log data by project
        for project in project_configs:
            config = project_configs[project]
            manager_email = config['manager']
            
            if not manager_email:
                print(f"No manager email configured for project: {project}")
                continue
                
            # Filter log data for this project
            project_logs = log_data.filter(pl.col('project') == project)
            
            if project_logs.is_empty():
                print(f"No log data for project: {project}")
                continue
                
            # Collect failed watches by category
            sync_failures = project_logs.filter(pl.col('CurrentFailedSync') >= config['currentSyncThr'])
            hr_failures = project_logs.filter(pl.col('CurrentFailedHR') >= config['currentHrThr'])
            sleep_failures = project_logs.filter(pl.col('CurrentFailedSleep') >= config['currentSleepThr'])
            steps_failures = project_logs.filter(pl.col('CurrentFailedSteps') >= config['currentStepsThr'])
            
            # Check total failures as well
            total_sync_failures = project_logs.filter(pl.col('TotalFailedSync') >= config['totalSyncThr'])
            total_hr_failures = project_logs.filter(pl.col('TotalFailedHR') >= config['totalHrThr'])
            total_sleep_failures = project_logs.filter(pl.col('TotalFailedSleep') >= config['totalSleepThr'])
            total_steps_failures = project_logs.filter(pl.col('TotalFailedSteps') >= config['totalStepsThr'])
            
            # Check battery level if available
            battery_failures = project_logs.filter(
                (pl.col('lastBattaryVal').cast(pl.Float32, strict=False) <= config['batteryThr'])
            )
            
            # Only send alert if there are failures
            if (sync_failures.height > 0 or hr_failures.height > 0 or 
                sleep_failures.height > 0 or steps_failures.height > 0 or
                total_sync_failures.height > 0 or total_hr_failures.height > 0 or
                total_sleep_failures.height > 0 or total_steps_failures.height > 0 or
                battery_failures.height > 0):
                
                # Create HTML report
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
                    <h2>Fitbit Alert: Project {project}</h2>
                    <p>The following watches have exceeded failure thresholds:</p>
                """
                
                # Add sections for each type of failure
                if sync_failures.height > 0:
                    html += f"""
                    <h3>Sync Failures (Current consecutive failures ≥ {config['currentSyncThr']})</h3>
                    <table>
                        <tr>
                            <th>Watch Name</th>
                            <th>Consecutive Failures</th>
                            <th>Total Failures</th>
                            <th>Last Synced</th>
                        </tr>
                    """
                    for row in sync_failures.iter_rows(named=True):
                        html += f"""
                        <tr>
                            <td>{row['watchName']}</td>
                            <td>{row['CurrentFailedSync']}</td>
                            <td>{row['TotalFailedSync']}</td>
                            <td>{row['lastSynced']}</td>
                        </tr>
                        """
                    html += "</table>"
                
                # Add similar sections for other failure types
                # Heart rate failures
                if hr_failures.height > 0:
                    html += f"""
                    <h3>Heart Rate Failures (Current consecutive failures ≥ {config['currentHrThr']})</h3>
                    <table>
                        <tr>
                            <th>Watch Name</th>
                            <th>Consecutive Failures</th>
                            <th>Total Failures</th>
                            <th>Last HR</th>
                        </tr>
                    """
                    for row in hr_failures.iter_rows(named=True):
                        html += f"""
                        <tr>
                            <td>{row['watchName']}</td>
                            <td>{row['CurrentFailedHR']}</td>
                            <td>{row['TotalFailedHR']}</td>
                            <td>{row['lastHRVal']}</td>
                        </tr>
                        """
                    html += "</table>"
                
                # Sleep failures
                if sleep_failures.height > 0:
                    html += f"""
                    <h3>Sleep Failures (Current consecutive failures ≥ {config['currentSleepThr']})</h3>
                    <table>
                        <tr>
                            <th>Watch Name</th>
                            <th>Consecutive Failures</th>
                            <th>Total Failures</th>
                            <th>Last Sleep</th>
                        </tr>
                    """
                    for row in sleep_failures.iter_rows(named=True):
                        html += f"""
                        <tr>
                            <td>{row['watchName']}</td>
                            <td>{row['CurrentFailedSleep']}</td>
                            <td>{row['TotalFailedSleep']}</td>
                            <td>{row['lastSleepStartDateTime']}</td>
                        </tr>
                        """
                    html += "</table>"
                
                # Steps failures
                if steps_failures.height > 0:
                    html += f"""
                    <h3>Steps Failures (Current consecutive failures ≥ {config['currentStepsThr']})</h3>
                    <table>
                        <tr>
                            <th>Watch Name</th>
                            <th>Consecutive Failures</th>
                            <th>Total Failures</th>
                            <th>Last Steps</th>
                        </tr>
                    """
                    for row in steps_failures.iter_rows(named=True):
                        html += f"""
                        <tr>
                            <td>{row['watchName']}</td>
                            <td>{row['CurrentFailedSteps']}</td>
                            <td>{row['TotalFailedSteps']}</td>
                            <td>{row['lastStepsVal']}</td>
                        </tr>
                        """
                    html += "</table>"
                
                # Battery warnings
                if battery_failures.height > 0:
                    html += f"""
                    <h3>Battery Warnings (Level ≤ {config['batteryThr']}%)</h3>
                    <table>
                        <tr>
                            <th>Watch Name</th>
                            <th>Battery Level</th>
                            <th>Last Check</th>
                        </tr>
                    """
                    for row in battery_failures.iter_rows(named=True):
                        html += f"""
                        <tr>
                            <td>{row['watchName']}</td>
                            <td>{row['lastBattaryVal']}%</td>
                            <td>{row['lastBattary']}</td>
                        </tr>
                        """
                    html += "</table>"
                
                # Close HTML
                html += """
                    <p>This is an automated alert from the Fitbit Management System.</p>
                </body>
                </html>
                """
                
                # Send the email
                subject = f"Fitbit Alert: Project {project} has {sync_failures.height + hr_failures.height + sleep_failures.height + steps_failures.height + battery_failures.height} issues"
                result = send_email_alert(manager_email, subject, html)
                
                # Track results
                if result:
                    alerts_sent[project] = {
                        'manager': manager_email,
                        'sync_failures': sync_failures.height,
                        'hr_failures': hr_failures.height,
                        'sleep_failures': sleep_failures.height,
                        'steps_failures': steps_failures.height,
                        'battery_failures': battery_failures.height,
                        'total_failures': sync_failures.height + hr_failures.height + sleep_failures.height + steps_failures.height + battery_failures.height
                    }
        
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
                        <td>{row['phone']}</td>
                        <td>{row.get('endDate', 'Unknown')}</td>
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
        
        # Step 1: Get watch data
        watch_data = get_watch_details()
        
        if not watch_data.is_empty():
            # Save to CSV for historical tracking
            save_to_csv(watch_data)
            
            # Update log using ServerLogFile
            log_file = ServerLogFile()
            result = log_file.update_fitbits_log(watch_data)
            
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
                    print(f"  Total watches with any failure: {stats.get('total_failures', 0)}")
            else:
                print(f"[{datetime.datetime.now()}] Failed to update log data")
                
            print(f"Data collection completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("No active watches found or error retrieving data")
        
        # Step 2: Run WhatsApp message analysis
        suspicious_nums_data = analyze_whatsapp_messages()
        
        # Step 3: Get alert configurations
        fitbit_config_sheet = spreadsheet.get_sheet("fitbit_alerts_config", sheet_type="fitbit_alerts_config")
        qualtrics_config_sheet = spreadsheet.get_sheet("qualtrics_alert_config", sheet_type="qualtrics_alert_config")
        
        # Step 4: Get log data for Fitbit alerts
        log_sheet = spreadsheet.get_sheet("FitbitLog", sheet_type="log")
        log_data = log_sheet.to_dataframe(engine="polars")
        
        # Step 5: Check alerts and send emails
        if not log_data.is_empty():
            fitbit_config_data = fitbit_config_sheet.to_dataframe(engine="polars")
            fitbit_alerts = check_fitbit_alerts(log_data, fitbit_config_data)
            
            if fitbit_alerts:
                print(f"Sent Fitbit alerts to {len(fitbit_alerts)} managers")
            else:
                print("No Fitbit alerts sent")
        
        # Step 6: Check Qualtrics alerts
        qualtrics_config_data = qualtrics_config_sheet.to_dataframe(engine="polars")
        
        # Get suspicious numbers from previous analysis
        suspicious_numbers_sheet = spreadsheet.get_sheet("suspicious_nums", sheet_type="suspicious_nums")
        suspicious_numbers = suspicious_numbers_sheet.to_dataframe(engine="polars")
        
        if not suspicious_numbers.is_empty():
            qualtrics_alerts = check_qualtrics_alerts(suspicious_numbers, qualtrics_config_data)
            
            if qualtrics_alerts:
                print(f"Sent Qualtrics alerts to {len(qualtrics_alerts)} managers")
            else:
                print("No Qualtrics alerts sent")
        
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
