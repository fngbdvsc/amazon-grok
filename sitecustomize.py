"""Loaded automatically on startup, before amazon_monitor.py reads its settings.

Forces the fast pace even if Railway still has the old GAP_MIN / MIN_SECONDS_PER_ROUND
variables. Delete this file to go back to the slow defaults in amazon_monitor.py.
"""
import os

os.environ["GAP_MIN"] = "3"
os.environ["GAP_MAX"] = "5"
os.environ["MIN_SECONDS_PER_ROUND"] = "3"
os.environ["BROWSE_FIRST"] = "0"
