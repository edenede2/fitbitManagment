import streamlit as st
import datetime
import random
from typing import Optional, Dict, Any

class GreetingService:
    """Service for generating personalized greetings based on context"""
    
    def __init__(self):
        self.morning_greetings = [
            "🌅 Good Morning, {}! Ready for a productive day?",
            "☕ Morning, {}! Hope you slept well.",
            "🌞 Rise and shine, {}! The day awaits.",
            "🌄 Good Morning, {}! Let's make today count."
        ]
        
        self.afternoon_greetings = [
            "🌤️ Good Afternoon, {}! How's your day going?",
            "🌞 Hello, {}! Hope your day is going well.",
            "👋 Afternoon, {}! Ready to tackle the rest of the day?",
            "🌄 Good Afternoon, {}! Keep up the great work."
        ]
        
        self.evening_greetings = [
            "🌙 Good Evening, {}! Winding down for the day?",
            "✨ Evening, {}! Hope you had a productive day.",
            "🌆 Good Evening, {}! Time to review today's progress.",
            "🎑 Hello, {}! How was your day?"
        ]
        
        self.weekend_greetings = [
            "🏖️ Happy Weekend, {}! Taking some time off?",
            "🎉 Weekend vibes, {}! Don't forget to recharge.",
            "🌴 Hello, {}! Enjoying your weekend?"
        ]
        
        self.role_specific_greetings = {
            "admin": [
                "👑 Welcome back, {}! Your system is running smoothly.",
                "🔧 Hello Administrator {}! Everything is under control."
            ],
            "manager": [
                "📊 Welcome, Manager {}! Your project data is ready for review.",
                "📈 Hello, {}! Your team's watches are collecting data."
            ],
            "student": [
                "📚 Hello, {}! Your watch data is being tracked.",
                "🔬 Welcome, {}! Ready to contribute to the research?"
            ],
            "researcher": [
                "🔬 Welcome Researcher {}! New data is available for analysis.",
                "📊 Hello, {}! The research data is ready for your review."
            ],
            "guest": [
                "👋 Welcome, Guest! Feel free to explore the system.",
                "🔍 Hello there! Interested in learning more about our system?"
            ]
        }
        
        self.special_day_greetings = {
            # Format: (month, day): "greeting"
            (1, 1): "🎆 Happy New Year, {}!",
            (12, 25): "🎄 Merry Christmas, {}!",
            (10, 31): "🎃 Happy Halloween, {}!",
            # Add more special dates as needed
        }
    
    def get_greeting(self, 
                     user_name: str = "Guest", 
                     user_role: str = "guest",
                     user_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a personalized greeting based on time, user role, and other context.
        
        Args:
            user_name (str): The name of the user
            user_role (str): The role of the user (admin, manager, student, etc.)
            user_data (dict, optional): Additional user data for personalization
            
        Returns:
            str: A personalized greeting
        """
        # Convert user_role to lowercase for case-insensitive matching
        user_role = user_role.lower() if isinstance(user_role, str) else "guest"
        
        # Get current date and time
        now = datetime.datetime.now()
        hour = now.hour
        day_of_week = now.weekday()  # 0-6 (Monday is 0)
        current_date = (now.month, now.day)
        
        # Check for special days first
        if current_date in self.special_day_greetings:
            return self.special_day_greetings[current_date].format(user_name)
        
        # Check if it's weekend
        is_weekend = day_of_week >= 5  # 5 = Saturday, 6 = Sunday
        
        # Select appropriate greeting pool based on time and day
        if is_weekend:
            greeting_pool = self.weekend_greetings
        elif 5 <= hour < 12:
            greeting_pool = self.morning_greetings
        elif 12 <= hour < 18:
            greeting_pool = self.afternoon_greetings
        else:
            greeting_pool = self.evening_greetings
        
        # Add role-specific greetings to the pool if available
        if user_role in self.role_specific_greetings:
            greeting_pool.extend(self.role_specific_greetings[user_role])
        
        # Select a random greeting from the pool
        greeting = random.choice(greeting_pool)
        
        # Format with user name
        return greeting.format(user_name)
    
    def get_returning_user_greeting(self, user_name: str, last_login: Optional[datetime.datetime] = None) -> str:
        """Generate a greeting for returning users based on their last login time"""
        if not last_login:
            return self.get_greeting(user_name)
            
        now = datetime.datetime.now()
        delta = now - last_login
        
        if delta.days == 0:
            # Same day return
            hours = delta.seconds // 3600
            if hours < 1:
                return f"👋 Welcome back, {user_name}! Nice to see you again so soon."
            else:
                return f"🔄 Hello again, {user_name}! Back for more after {hours} hours?"
        elif delta.days == 1:
            return f"👋 Welcome back, {user_name}! It's been a day since your last visit."
        elif delta.days < 7:
            return f"👋 Good to see you, {user_name}! It's been {delta.days} days since your last login."
        else:
            return f"🎉 Welcome back, {user_name}! We've missed you these past {delta.days} days."


# Create a singleton instance of the greeting service
_greeting_service = GreetingService()

def congrats(user_name: str = "Guest", user_role: str = "guest", user_data: Optional[Dict[str, Any]] = None) -> str:
    """
    Get a personalized greeting for a user.
    
    Args:
        user_name (str): The name of the user
        user_role (str): The role of the user
        user_data (dict, optional): Additional user data for personalization
        
    Returns:
        str: A personalized greeting
    """
    return _greeting_service.get_greeting(user_name, user_role, user_data)


def welcome_returning_user(user_name: str, last_login: Optional[datetime.datetime] = None) -> str:
    """
    Get a personalized greeting for a returning user.
    
    Args:
        user_name (str): The name of the user
        last_login (datetime, optional): The user's last login time
        
    Returns:
        str: A personalized greeting based on last login time
    """
    return _greeting_service.get_returning_user_greeting(user_name, last_login)