"""
Put serving/ on sys.path so tests can import `main`, `db.*`, `models.*`
the same way uvicorn does inside the Docker container.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving"))
