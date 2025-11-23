"""
scrape_weather.py
-----------------
WeatherScraper module for scraping Winnipeg climate data using ONLY HTML parsing.

Features:
- Extracts Min/Max/Mean temperatures for each day of each month.
- Scrapes backward from the current year down to a given start year.
- Automatically detects when no more data is available.
- Provides dictionary output consumed by DBOperations.

Author: Caden Jackson

Created on: 11/16/2025
Last updated: 11/23/2025
"""

import logging
from datetime import datetime
from html.parser import HTMLParser

import requests
import urllib3

# Disable HTTPS warnings 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# ============================================================
# SIMPLE TABLE PARSER
# ============================================================
class SimpleTableParser(HTMLParser):
    """Parses HTML <td> rows into a list of row-lists."""

    def __init__(self):
        super().__init__()
        self.in_td = False
        self.current_cell = ""
        self.row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag == "td":
            self.in_td = True
            self.current_cell = ""

    def handle_endtag(self, tag):
        if tag == "td":
            self.in_td = False
            self.row.append(self.current_cell.strip())

        elif tag == "tr":
            if self.row:
                self.rows.append(self.row)
            self.row = []

    def handle_data(self, data):
        if self.in_td:
            self.current_cell += data


# ============================================================
# WEATHER SCRAPER
# ============================================================
class WeatherScraper:
    """
    Scrapes Winnipeg climate data from Environment Canada using HTML parsing.

    Attributes:
        station_id (int): Weather station ID.
        start_year (int): Year to scrape back to.
        end_year (int): Usually current year.
        timeframe (int): Environment Canada daily view timeframe.
        daily_data (dict): Dictionary containing date → temperature values.
    """

    BASE_HTML_URL = "https://climate.weather.gc.ca/climate_data/daily_data_e.html"

    def __init__(self, station_id=27174, start_year=2018, end_year=None, timeframe=2):
        self.station_id = station_id
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.timeframe = timeframe
        self.daily_data = {}

    # ------------------------------------------------------------
    # FETCH SINGLE MONTH
    # ------------------------------------------------------------
    def fetch_html_for_month(self, year, month):
        """
        Scrape daily max/min/mean temperatures for a specific month.

        Args:
            year (int): Year to scrape.
            month (int): Month to scrape.

        Populates:
            self.daily_data[YYYY-MM-DD] = {"Max": float, "Min": float, "Mean": float}
        """
        logger.info("Scraping HTML weather for %d-%02d", year, month)

        url = (
            f"{self.BASE_HTML_URL}?StationID={self.station_id}"
            f"&timeframe={self.timeframe}&Year={year}&Month={month}&Day=1"
        )

        try:
            response = requests.get(url, verify=False, timeout=10)
        except requests.RequestException as exc:
            logger.error("Request error while scraping %d-%02d: %s", year, month, exc)
            return

        if response.status_code != 200:
            logger.warning("Failed to load month %d-%02d, status %d",
                           year, month, response.status_code)
            return

        parser = SimpleTableParser()
        parser.feed(response.text)
        rows = parser.rows

        if not rows:
            logger.warning("No HTML table data found for %d-%02d", year, month)
            return

        day = 1
        for row in rows:
            if len(row) < 3:
                continue

            try:
                max_t = float(row[0])
                min_t = float(row[1])
                mean_t = float(row[2])
            except (ValueError, TypeError):
                # Skip non-numeric rows (summary, empty, etc.)
                continue

            date_str = f"{year}-{month:02d}-{day:02d}"
            self.daily_data[date_str] = {
                "Max": max_t,
                "Min": min_t,
                "Mean": mean_t
            }
            day += 1

    # ------------------------------------------------------------
    # SCRAPE THE ENTIRE RANGE
    # ------------------------------------------------------------
    def scrape_all(self):
        """
        Scrape all months between end_year and start_year.

        Returns:
            dict: { "YYYY-MM-DD": {"Max": float, "Min": float, "Mean": float}, ... }
        """
        logger.info("Starting full weather scraping (%d → %d)...",
                    self.end_year, self.start_year)

        for year in range(self.end_year, self.start_year - 1, -1):
            for month in range(12, 1 - 1, -1):  # 12 down to 1
                self.fetch_html_for_month(year, month)

        logger.info("Finished scraping. Total days collected: %d",
                    len(self.daily_data))
        return self.daily_data


# ============================================================
# SELF-TEST ENTRY POINT
# ============================================================
if __name__ == "__main__":
    scraper = WeatherScraper()
    data = scraper.scrape_all()

    print("\nSample records:")
    for i, (date, values) in enumerate(data.items()):
        print(date, values)
        if i >= 9:
            break
