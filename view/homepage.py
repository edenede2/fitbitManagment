import streamlit as st
import datetime
from entity.User import User, UserRepository
from entity.Project import Project, ProjectRepository
from entity.Watch import Watch, WatchFactory
from entity.Sheet import Spreadsheet, GoogleSheetsAdapter
from Decorators.congrates import congrats, welcome_returning_user
from model.config import get_secrets

def display_homepage(user_email, user_role, user_project):
    """
    Display the homepage with personalized content based on user's role and project
    """
    # Get user data for personalization
    user_data = {}
    user_name = user_email.split('@')[0] if '@' in user_email else user_email
    
    try:
        # Try to get user details from spreadsheet
        spreadsheet = Spreadsheet(
            'usersPassRoles',
            get_secrets().get('spreadsheet_key'),
        )            
        GoogleSheetsAdapter.connect(spreadsheet)
        users_sheet = spreadsheet.get_sheet("user", "user")
        user_entry = next((u for u in users_sheet.data if u.get('email') == user_email), None)
        
        if user_entry:
            user_name = user_entry.get('name', user_name)
            last_login = None
            if user_entry.get('last_login'):
                try:
                    if user_entry.get('last_login') == None or user_entry.get('last_login') == '':
                        last_login = datetime.datetime.now()
                        # Update last login time in the sheet
                        user_entry['last_login'] = last_login.isoformat()
                        spreadsheet.update_sheet("user", "user", user_entry)
                        GoogleSheetsAdapter.save(spreadsheet)
                    else:
                        last_login = datetime.datetime.fromisoformat(user_entry.get('last_login'))
                except ValueError:
                    pass
                    
            # Display personalized welcome message with last login time if available
            if last_login:
                welcome_msg = welcome_returning_user(user_name, last_login)
                st.header(welcome_msg)
            else:
                welcome_msg = congrats(user_name, user_role, user_entry)
                st.header(welcome_msg)
        else:
            # Fallback to simple greeting
            welcome_msg = congrats(user_name, user_role)
            st.header(welcome_msg)
    except Exception as e:
        # Fallback in case of any error
        welcome_msg = congrats(user_name, user_role)
        st.header(welcome_msg)
    
    # System overview stats using entities
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Your Role", user_role)
        
    with col2:
        st.metric("Project", user_project)
            
    with col3:
        # Get watch count from entity layer
        try:
            spreadsheet = Spreadsheet(
                'usersPassRoles',
                get_secrets().get('spreadsheet_key')
            )

            GoogleSheetsAdapter.connect(spreadsheet)
            fitbits_sheet = spreadsheet.get_sheet("fitbit", "fitbit")


            
            if user_role == "Admin":
                # Admin sees all watches in all projects
                st.metric("System Watches", len(fitbits_sheet.data))
            elif user_role == "Manager":
                # Managers see watches in their project
                watches = [w for w in fitbits_sheet.data if w.get('project') == user_project]
                st.metric("Watches in Project", len(watches))
            elif user_role == "Student":
                # Count watches assigned to this student
                watches = [w for w in fitbits_sheet.data if w.get('user') == user_email]
                st.metric("Your Watches", len(watches))
            else:
                st.metric("System Watches", len(fitbits_sheet.data))
        except Exception as e:
            st.metric("Watches", "N/A")
    
    # Recent activity from logs
    st.subheader("Recent Activity")
    
    try:
        # Get log data from entity layer
        logs_sheet = spreadsheet.get_sheet("log", "log")
        
        if user_role == "Admin":
            # Admin sees logs from all projects
            # Show most recent 5 log entries
            recent_logs = sorted(logs_sheet.data, 
                               key=lambda x: x.get('lastCheck', ''), 
                               reverse=True)[:5]
            
            if recent_logs:
                for log in recent_logs:
                    project_name = log.get('project', 'Unknown Project')
                    st.info(f"{log.get('lastCheck', 'Unknown time')} - Project: {project_name} - Watch {log.get('watchName', 'Unknown')} - "
                           f"Battery: {log.get('lastBattaryVal', 'N/A')}, "
                           f"HR: {log.get('lastHRVal', 'N/A')}")
            else:
                st.write("No recent activity recorded")
        elif user_role == "Manager":
            # Filter logs for this project
            project_logs = [log for log in logs_sheet.data 
                           if log.get('project') == user_project]
            
            # Show most recent 5 log entries
            recent_logs = sorted(project_logs, 
                               key=lambda x: x.get('lastCheck', ''), 
                               reverse=True)[:5]
            
            if recent_logs:
                for log in recent_logs:
                    st.info(f"{log.get('lastCheck', 'Unknown time')} - Watch {log.get('watchName', 'Unknown')} - "
                           f"Battery: {log.get('lastBattaryVal', 'N/A')}, "
                           f"HR: {log.get('lastHRVal', 'N/A')}")
            else:
                st.write("No recent activity recorded")
        elif user_role == "Student":
            # Get just this student's watches
            student_watches = [w.get('name') for w in fitbits_sheet.data 
                             if w.get('user') == user_email]
            
            # Filter logs for student's watches
            student_logs = [log for log in logs_sheet.data 
                           if log.get('watchName') in student_watches]
            
            # Show most recent 5 log entries
            recent_logs = sorted(student_logs, 
                               key=lambda x: x.get('lastCheck', ''), 
                               reverse=True)[:5]
            
            if recent_logs:
                for log in recent_logs:
                    st.info(f"{log.get('lastCheck', 'Unknown time')} - Watch {log.get('watchName', 'Unknown')} - "
                           f"Battery: {log.get('lastBattaryVal', 'N/A')}, "
                           f"HR: {log.get('lastHRVal', 'N/A')}")
            else:
                st.write("No recent activity recorded for your watches")
        else:
            st.write("No activity data available for your role")
    except Exception as e:
        st.error(f"Unable to load recent activity")
        st.write("No recent activity data available")
    
    # Achievements/Milestones check
    try:
        # Just some simple examples
        if user_role == "Admin":
            # Check all watches health across projects
            spreadsheet = spreadsheet = Spreadsheet(
            'usersPassRoles',
            get_secrets().get('spreadsheet_key'),
        )
            GoogleSheetsAdapter.connect(spreadsheet)
            fitbits_sheet = spreadsheet.get_sheet("fitbit", "fitbit")
            logs_sheet = spreadsheet.get_sheet("FitbitLog", "log")
            
            all_watches = fitbits_sheet.data
            active_watches = [w for w in all_watches if str(w.get('isActive', '')).lower() != 'false']
            
            if active_watches:
                # Rest of achievement logic remains the same but applies to all projects
                # Get latest log for each watch
                active_watch_names = [w.get('name') for w in active_watches]
                
                # Get all logs for active watches
                watch_logs = {}
                for log in logs_sheet.data:
                    watch_name = log.get('watchName')
                    if watch_name in active_watch_names:
                        last_check = log.get('lastCheck', '')
                        if watch_name not in watch_logs or last_check > watch_logs[watch_name].get('lastCheck', ''):
                            watch_logs[watch_name] = log
                
                # Count watches with good battery
                good_battery_count = sum(
                    1 for log in watch_logs.values() 
                    if log.get('lastBattaryVal') and float(log.get('lastBattaryVal', 0)) > 80
                )
                
                # Show achievement if all watches have good battery
                if good_battery_count == len(active_watches) and len(active_watches) > 0:
                    st.success("🔋 Excellent! All system watches have good battery levels!")
                    
                # Count recently synced watches (last 24 hours)
                now = datetime.datetime.now()
                synced_count = 0
                
                for log in watch_logs.values():
                    if log.get('lastSynced'):
                        try:
                            last_sync = datetime.datetime.fromisoformat(log.get('lastSynced').replace('Z', ''))
                            if (now - last_sync).total_seconds() < 24 * 3600:  # 24 hours
                                synced_count += 1
                        except (ValueError, TypeError):
                            pass
                
                # Show achievement if all watches synced recently
                if synced_count == len(active_watches) and len(active_watches) > 0:
                    st.success("🌟 Fantastic job! All system watches are synced and up to date!")
                    
        elif user_role == "Manager":
            # Check watch health for this project
            spreadsheet = Spreadsheet(
            'usersPassRoles',
            get_secrets().get('spreadsheet_key'),
        )
            GoogleSheetsAdapter.connect(spreadsheet)
            fitbits_sheet = spreadsheet.get_sheet("fitbit", "fitbit")
            logs_sheet = spreadsheet.get_sheet("FitbitLog", "log")
            
            project_watches = [w for w in fitbits_sheet.data if w.get('project') == user_project]
            active_watches = [w for w in project_watches if str(w.get('isActive', '')).lower() != 'false']
            
            if active_watches:
                # Get latest log for each watch
                active_watch_names = [w.get('name') for w in active_watches]
                
                # Get all logs for active watches
                watch_logs = {}
                for log in logs_sheet.data:
                    watch_name = log.get('watchName')
                    if watch_name in active_watch_names:
                        last_check = log.get('lastCheck', '')
                        if watch_name not in watch_logs or last_check > watch_logs[watch_name].get('lastCheck', ''):
                            watch_logs[watch_name] = log
                
                # Count watches with good battery
                good_battery_count = sum(
                    1 for log in watch_logs.values() 
                    if log.get('lastBattaryVal') and float(log.get('lastBattaryVal', 0)) > 80
                )
                
                # Show achievement if all watches have good battery
                if good_battery_count == len(active_watches) and len(active_watches) > 0:
                    st.success("🔋 Excellent! All your project's watches have good battery levels!")
                    
                # Count recently synced watches (last 24 hours)
                now = datetime.datetime.now()
                synced_count = 0
                
                for log in watch_logs.values():
                    if log.get('lastSynced'):
                        try:
                            last_sync = datetime.datetime.fromisoformat(log.get('lastSynced').replace('Z', ''))
                            if (now - last_sync).total_seconds() < 24 * 3600:  # 24 hours
                                synced_count += 1
                        except (ValueError, TypeError):
                            pass
                
                # Show achievement if all watches synced recently
                if synced_count == len(active_watches) and len(active_watches) > 0:
                    st.success("🌟 Fantastic job! All your watches are synced and up to date!")
                    
        elif user_role == "Student":
            # Get student's watches
            spreadsheet = Spreadsheet(
            'usersPassRoles',
            get_secrets().get('spreadsheet_key'),
        )
            GoogleSheetsAdapter.connect(spreadsheet)
            fitbits_sheet = spreadsheet.get_sheet("fitbit", "fitbit")
            logs_sheet = spreadsheet.get_sheet("FitbitLog", "log")
            
            student_watches = [w for w in fitbits_sheet.data if w.get('user') == user_email]
            
            if student_watches:
                watch_names = [w.get('name') for w in student_watches]
                recent_syncs = []
                
                # Get latest logs for student watches
                for watch_name in watch_names:
                    logs = [log for log in logs_sheet.data if log.get('watchName') == watch_name]
                    if logs:
                        latest_log = max(logs, key=lambda x: x.get('lastCheck', ''))
                        if latest_log.get('lastSynced'):
                            recent_syncs.append(latest_log)
                
                # Show achievement if student has synced watches recently
                if len(recent_syncs) == len(student_watches) and student_watches:
                    st.success("👏 Great job keeping your watches synced!")
                
    except Exception as e:
        # Fail silently for achievements
        pass
    
    # Quick actions based on role
    st.subheader("Quick Actions")
    cols = st.columns(3)
    
    if user_role == "Admin":
        if cols[0].button("View All Watches"):
            st.session_state.navigate_to = "dashboard_all_watches"
        if cols[1].button("Add New Watch"):
            st.session_state.navigate_to = "add_watch"
        if cols[2].button("Manage Users"):
            st.session_state.navigate_to = "manage_users"
    
    elif user_role == "Manager":
        if cols[0].button("View Project Watches"):
            st.session_state.navigate_to = "dashboard_project_watches"
        if cols[1].button("Assign Watches"):
            st.session_state.navigate_to = "assign_watches"
        if cols[2].button("View Reports"):
            st.session_state.navigate_to = "reports"
    
    elif user_role == "Student":
        if cols[0].button("My Watch Status"):
            st.session_state.navigate_to = "my_watches"
        if cols[1].button("Submit Data"):
            st.session_state.navigate_to = "submit_data"
