"""
dbcm.py
Database Context Manager Module

This module provides the DBCM class, which is used throughout the project
to safely manage SQLite database connections using Python's context manager
protocol.

By: Caden Jackson

Created on: 11/16/2025
Last updated: 11/23/2025
"""

import sqlite3
import logging

# ----------------------------
# Configure logging to a file
# ----------------------------
logging.basicConfig(
    filename="weather_app.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class DBCM:
    """
    A Database Context Manager (DBCM) used to automatically handle
    SQLite connection setup, committing, and cleanup.
    """

    def __init__(self, db_file):
        """
        Initialize the context manager with the database file path.

        Args:
            db_file (str): Path to the SQLite database file.
        """
        self.db_file = db_file
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """
        Open the SQLite database connection and return a cursor object.

        Returns:
            sqlite3.Cursor: Cursor used for executing SQL commands.

        Logs:
            Any connection-related errors.
        """
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
            return self.cursor
        except sqlite3.Error as err:
            logger.error("Failed to connect to database: %s", err)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Commit changes and close the database connection.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Traceback (if any)

        Logs:
            Any SQL errors raised inside the managed block.
        """
        if exc_type:
            logger.error("SQLite Error: %s", exc_val)

        try:
            if self.conn:
                self.conn.commit()
                self.conn.close()
        except sqlite3.Error as err:
            logger.error("Error closing or committing database: %s", err)
            raise
