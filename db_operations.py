"""
db_operations.py
Database operations for the Weather Processing App.

This module contains the DBOperations class, which handles all database
initialization, insertion, and data retrieval related to weather samples.
All SQL access is done through the DBCM context manager.

Author: Caden Jackson

Created on: 11/16/2025
Last Updated: 11/23/2025
"""

import logging
from dbcm import DBCM

# Configure logging to file + console as required by rubric
logging.basicConfig(
    level=logging.INFO,
    filename="weather_app.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DBOperations:
    """
    Handles all SQLite database operations for weather data.

    This includes:
        - Initializing the database on application start
        - Saving new temperature records
        - Fetching data for verification
        - Fetching data for plotting (mean temps, min/max temps)

    """

    def __init__(self, db_file="weather_data.db"):
        """
        Initialize the DBOperations object.

        Args:
            db_file (str): The SQLite database filename (default: weather_data.db)
        """
        self.db_file = db_file
        self.initialize_db()

    # ----------------------------------------------------------------------

    def initialize_db(self):
        """
        Create the weather_data table if it does not exist.

        Returns:
            None
        """
        try:
            with DBCM(self.db_file) as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sample_date TEXT UNIQUE,
                        min_temp REAL,
                        max_temp REAL,
                        avg_temp REAL
                    )
                """)
            logger.info("Database initialized successfully.")
        except Exception as exc:
            logger.error("Database initialization error: %s", exc)

    # ----------------------------------------------------------------------

    def save_data(self, date, max_temp, min_temp, mean_temp):
        """
        Save one day's weather data into the database.
        Duplicate dates are ignored using INSERT OR IGNORE.

        Args:
            date (str): YYYY-MM-DD timestamp
            max_temp (float)
            min_temp (float)
            mean_temp (float)

        Returns:
            None
        """
        try:
            with DBCM(self.db_file) as cur:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO weather_data 
                        (sample_date, max_temp, min_temp, avg_temp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (date, max_temp, min_temp, mean_temp)
                )
            logger.info(
                "Saved record %s (Max=%s, Min=%s, Mean=%s)",
                date, max_temp, min_temp, mean_temp
            )
        except Exception as exc:
            logger.error("Failed to save data for %s: %s", date, exc)

    # ----------------------------------------------------------------------

    def fetch_data(self, limit=5, order_desc=True):
        """
        Fetch a number of records from the database.

        Args:
            limit (int): number of results
            order_desc (bool): sort descending (newest first)

        Returns:
            list[tuple]: rows of (sample_date, max_temp, min_temp, avg_temp)
        """
        try:
            order = "DESC" if order_desc else "ASC"

            with DBCM(self.db_file) as cur:
                cur.execute(
                    f"""
                    SELECT sample_date, max_temp, min_temp, avg_temp
                    FROM weather_data
                    ORDER BY sample_date {order}
                    LIMIT ?
                    """,
                    (limit,)
                )
                return cur.fetchall()

        except Exception as exc:
            logger.error("Fetch error: %s", exc)
            return []

    # ----------------------------------------------------------------------

    def count_records(self):
        """
        Return total number of weather data rows.

        Returns:
            int: number of rows in DB
        """
        try:
            with DBCM(self.db_file) as cur:
                cur.execute("SELECT COUNT(*) FROM weather_data")
                return cur.fetchone()[0]
        except Exception as exc:
            logger.error("Count records error: %s", exc)
            return 0

    # ============================
    #   PLOTTING DATA HELPERS
    # ============================

    def fetch_mean_temps_range(self, year_start, year_end):
        """
        Return (sample_date, avg_temp) rows for a range of years.

        Args:
            year_start (int)
            year_end (int)

        Returns:
            list[tuple]: (sample_date, avg_temp)
        """
        try:
            with DBCM(self.db_file) as cur:
                cur.execute(
                    """
                    SELECT sample_date, avg_temp
                    FROM weather_data
                    WHERE CAST(substr(sample_date, 1, 4) AS INT)
                          BETWEEN ? AND ?
                    ORDER BY sample_date ASC
                    """,
                    (year_start, year_end)
                )
                return cur.fetchall()
        except Exception as exc:
            logger.error("Mean temp range fetch error: %s", exc)
            return []

    # ----------------------------------------------------------------------

    def fetch_mean_temps_for_month(self, year, month):
        """
        Return all mean temperatures for a specific month/year.

        Args:
            year (int)
            month (int)

        Returns:
            list[tuple]: (sample_date, avg_temp)
        """
        try:
            pattern = f"{year}-{month:02}%"

            with DBCM(self.db_file) as cur:
                cur.execute(
                    """
                    SELECT sample_date, avg_temp
                    FROM weather_data
                    WHERE sample_date LIKE ?
                    ORDER BY sample_date ASC
                    """,
                    (pattern,)
                )
                return cur.fetchall()
        except Exception as exc:
            logger.error("Mean temp monthly fetch error: %s", exc)
            return []

    # ----------------------------------------------------------------------

    def fetch_min_max_for_month(self, year, month):
        """
        Return min/max temperatures for a month.

        Args:
            year (int)
            month (int)

        Returns:
            list[tuple]: (sample_date, max_temp, min_temp)
        """
        try:
            pattern = f"{year}-{month:02}%"

            with DBCM(self.db_file) as cur:
                cur.execute(
                    """
                    SELECT sample_date, max_temp, min_temp
                    FROM weather_data
                    WHERE sample_date LIKE ?
                    ORDER BY sample_date ASC
                    """,
                    (pattern,)
                )
                return cur.fetchall()
        except Exception as exc:
            logger.error("Min/Max monthly fetch error: %s", exc)
            return []
