import streamlit as st
import datetime

def congrats():
    hour = datetime.datetime.now().hour

    if 6 <= hour < 12:
        greeting = "🌅 Good Morning, {}"
    elif 12 <= hour < 18:
        greeting = "🌄 Good Afternoon, {}"
    else:
        greeting = "🎑 Good Evening, {}"

    return greeting