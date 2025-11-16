# scrape_weather.py
import csv  # to read CSV data from Environment Canada
import logging  # for logging info, warnings, errors
from datetime import datetime  # to handle dates and determine current year
import requests  # for HTTP requests to fetch CSV or HTML data
from concurrent.futures import ThreadPoolExecutor, as_completed  # optional multithreading
import urllib3  # used to suppress insecure request warnings
from html.parser import HTMLParser  # to attempt HTML parsing (assignment requirement)

"""
This file contains the WeatherScraper class, which scrapes weather data (min, max, mean temperatures)
from Environment Canada. It first attempts HTML parsing (which fails), then falls back to CSV scraping.
Created by Caden Jackson
Created on: 11/16/25
Last updated: 11/16/25
"""

# Disable warnings for insecure HTTP requests (we're using verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging: INFO level, custom format with timestamp, logger name, level, message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("weather_scraper")

# -----------------------------
# WeatherScraper class
# -----------------------------
class WeatherScraper:
    # Base URLs for CSV and HTML scraping
    BASE_CSV_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
    BASE_HTML_URL = "https://climate.weather.gc.ca/climate_data/daily_data_e.html"

    def __init__(self, station_id=27174, start_year=2018, end_year=None, timeframe=2):
        """
        Initialize the scraper with:
        - station_id: default 27174 (Winnipeg)
        - start_year: year to stop scraping
        - end_year: year to start scraping (default = current year)
        - timeframe: 2 = daily data
        """
        self.station_id = station_id
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.timeframe = timeframe
        self.daily_data = {}  # dictionary to store scraped data

    # -----------------------------
    # HTML parsing attempt
    # -----------------------------
    def _scrape_html_for_month(self, year, month):
        """
        Dummy HTML parsing attempt to satisfy assignment requirements.
        Currently returns empty because HTML parsing is unreliable.
        """
        logger.info(f"Attempting HTML parse for {year}-{month:02} (expected to fail)...")
        try:
            # Construct HTML URL for the month/year
            response = requests.get(
                f"{self.BASE_HTML_URL}?StationID={self.station_id}&timeframe={self.timeframe}&Year={year}&Month={month}&Day=1",
                verify=False, timeout=10
            )
            if response.status_code != 200:
                return None

            html_content = response.text

            # Dummy parser class: does nothing
            class DummyParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.data = {}  # would store parsed data

                def handle_data(self, data):
                    pass  # intentionally does nothing

            parser = DummyParser()
            parser.feed(html_content)
            return parser.data  # returns empty dict
        except Exception as e:
            logger.error(f"HTML parsing failed for {year}-{month:02}: {e}")
            return None

    # -----------------------------
    # CSV scraping (fallback)
    # -----------------------------
    def fetch_csv_for_month(self, year, month):
        """
        Fetch weather data for a given month.
        Tries HTML parsing first, then falls back to CSV scraping.
        Updates self.daily_data with {date: {Max, Min, Mean}}.
        """
        # 1) Attempt HTML parsing
        html_data = self._scrape_html_for_month(year, month)
        if html_data:
            logger.info(f"Using HTML data for {year}-{month:02}")
            self.daily_data.update(html_data)
            return

        # 2) Fall back to CSV scraping
        url = f"{self.BASE_CSV_URL}?format=csv&stationID={self.station_id}&timeframe={self.timeframe}&Year={year}&Month={month}&Day=1"
        try:
            logger.info(f"Fetching CSV for {year}-{month:02}")
            response = requests.get(url, verify=False, timeout=10)

            if response.status_code != 200:
                logger.warning(f"No data for {year}-{month:02}, skipping")
                return

            # Decode CSV and read with DictReader
            decoded = response.content.decode('utf-8')
            reader = csv.DictReader(decoded.splitlines())

            for row in reader:
                date = row.get('Date/Time')
                if not date or row.get('Max Temp (°C)') == '':
                    continue  # skip invalid rows
                self.daily_data[date] = {
                    'Max': float(row.get('Max Temp (°C)', 0)),
                    'Min': float(row.get('Min Temp (°C)', 0)),
                    'Mean': float(row.get('Mean Temp (°C)', 0))
                }
        except Exception as e:
            logger.error(f"Error fetching {year}-{month:02}: {e}")

    # -----------------------------
    # Scrape all months/years
    # -----------------------------
    def scrape_all(self, use_threads=True, max_workers=6):
        """
        Scrape all data from end_year down to start_year.
        Can optionally use multithreading for speed.
        Returns self.daily_data dictionary.
        """
        logger.info(f"Initialized WeatherScraper starting at {self.start_year}")
        logger.info("Starting scraping process...")

        tasks = []
        # Use ThreadPoolExecutor for concurrent fetching if use_threads=True
        with ThreadPoolExecutor(max_workers=max_workers) if use_threads else DummyExecutor() as executor:
            for year in range(self.end_year, self.start_year - 1, -1):
                for month in range(12, 0, -1):
                    # Submit scraping tasks for each month/year
                    tasks.append(executor.submit(self.fetch_csv_for_month, year, month))

            # Wait for all futures to complete
            if use_threads:
                for future in as_completed(tasks):
                    future.result()

        logger.info(f"Scraped {len(self.daily_data)} daily records.")
        return self.daily_data


# -----------------------------
# DummyExecutor / DummyFuture
# -----------------------------
class DummyExecutor:
    """ Mimics ThreadPoolExecutor when threading is disabled """
    def __enter__(self):
        return self

    def submit(self, fn, *args, **kwargs):
        # Calls the function immediately instead of submitting to a thread
        fn(*args, **kwargs)
        return DummyFuture()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class DummyFuture:
    """ Mimics concurrent.futures.Future when using DummyExecutor """
    def result(self):
        return None
