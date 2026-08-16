#!/bin/bash
# Fix system time synchronization issue

echo "================================================================"
echo "Fixing System Time Synchronization"
echo "================================================================"
echo ""

echo "Current system time:"
date
echo ""

echo "Querying actual time from NTP server..."
ntpdate -q pool.ntp.org
echo ""

echo "Syncing system clock with NTP server..."
sudo ntpdate -s pool.ntp.org

echo ""
echo "Updated system time:"
date
echo ""

echo "Syncing hardware clock with system clock..."
sudo hwclock --systohc

echo ""
echo "Checking synchronization status:"
timedatectl status
echo ""

echo "================================================================"
echo "Time synchronization complete!"
echo "================================================================"
echo ""
echo "Your system was approximately 5 minutes behind."
echo "This was causing the Google Sheets JWT authentication to fail."
echo "You should now be able to run the Fitbit data collection script."
