import logging
from dbcm import DBCM

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBOperations:
    """Database operations for weather data."""

    def __init__(self, db_file="weather_data.db"):
        self.db_file = db_file
        self.initialize_db()

    def initialize_db(self):
        """Create the weather_data table if it doesn't exist."""
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
        logger.info("Database initialized.")

    def save_data(self, date, max_temp, min_temp, mean_temp):
        """Save a single day's weather data. Avoid duplicates."""
        try:
            with DBCM(self.db_file) as cur:
                cur.execute("""
                INSERT OR IGNORE INTO weather_data (sample_date, max_temp, min_temp, avg_temp)
                VALUES (?, ?, ?, ?)
            """, (date, max_temp, min_temp, mean_temp))
            logger.info(f"Saved {date}: Max={max_temp}, Min={min_temp}, Mean={mean_temp}")
        except Exception as e:
            logger.error(f"Failed to save {date}: {e}")


    def fetch_data(self, limit=5, order_desc=True):
        """Fetch records, ordered by date.
        Args:
            limit (int): Number of records to fetch.
            order_desc (bool): True for descending order, False for ascending.
        Returns:
            List of rows as tuples: (sample_date, max_temp, min_temp, avg_temp)
        """
        order = "DESC" if order_desc else "ASC"
        with DBCM(self.db_file) as cur:
            cur.execute(f"""
                SELECT sample_date, max_temp, min_temp, avg_temp
                FROM weather_data
                ORDER BY sample_date {order}
                LIMIT ?
            """, (limit,))
            rows = cur.fetchall()
        return rows

    def count_records(self):
        """Return the total number of records in the database."""
        with DBCM(self.db_file) as cur:
            cur.execute("SELECT COUNT(*) FROM weather_data")
            total = cur.fetchone()[0]
        return total
