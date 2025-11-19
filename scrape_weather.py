# scrape_weather.py
import logging
import requests
from datetime import datetime
from html.parser import HTMLParser
import urllib3

"""
Weather Scraper using ONLY HTML parsing.
Scrapes Min/Max/Mean temperature data for each day of each month.
Created by Caden Jackson
Updated: 11/16/25
"""

# Disable HTTPS warnings (verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("weather_scraper")


# --------------------------------------------------------------------
# HTML TABLE PARSER 
# --------------------------------------------------------------------
class SimpleTableParser(HTMLParser):
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


# --------------------------------------------------------------------
# MAIN SCRAPER CLASS
# --------------------------------------------------------------------
class WeatherScraper:
    BASE_HTML_URL = "https://climate.weather.gc.ca/climate_data/daily_data_e.html"

    def __init__(self, station_id=27174, start_year=2018, end_year=None, timeframe=2):
        self.station_id = station_id
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.timeframe = timeframe
        self.daily_data = {}

    # ------------------------------------------------------------
    # HTML SCRAPING (now real, working)
    # ------------------------------------------------------------
    def fetch_html_for_month(self, year, month):
        """
        Scrapes daily weather values from the HTML table.
        Populates self.daily_data[date] = {Max, Min, Mean}
        """
        logger.info(f"Scraping HTML weather for {year}-{month:02}")

        url = (
            f"{self.BASE_HTML_URL}?StationID={self.station_id}"
            f"&timeframe={self.timeframe}&Year={year}&Month={month}&Day=1"
        )

        try:
            response = requests.get(url, verify=False, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to load month {year}-{month:02}")
                return

            parser = SimpleTableParser()
            parser.feed(response.text)
            rows = parser.rows

            if not rows:
                logger.warning(f"No table data found in HTML for {year}-{month:02}")
                return

            # Extract only rows with numeric temp data (filter garbage rows)
            day = 1
            for row in rows:
                if len(row) < 3:
                    continue

                try:
                    max_t = float(row[0])
                    min_t = float(row[1])
                    mean_t = float(row[2])
                except:
                    continue  # skip summary rows

                date = f"{year}-{month:02}-{day:02}"
                self.daily_data[date] = {
                    "Max": max_t,
                    "Min": min_t,
                    "Mean": mean_t
                }
                day += 1

        except Exception as e:
            logger.error(f"HTML scraping error for {year}-{month:02}: {e}")

    # ------------------------------------------------------------
    # FULL SCRAPER
    # ------------------------------------------------------------
    def scrape_all(self):
        """
        Scrapes all months between end_year and start_year using HTML only.
        """
        logger.info(f"Starting HTML-only weather scraping…")

        for year in range(self.end_year, self.start_year - 1, -1):
            for month in range(12, 0, -1):
                self.fetch_html_for_month(year, month)

        logger.info(f"Finished. Total days scraped: {len(self.daily_data)}")
        return self.daily_data


# --------------------------------------------------------------------
# Allow running directly
# --------------------------------------------------------------------
if __name__ == "__main__":
    scraper = WeatherScraper()
    data = scraper.scrape_all()

    print("\nSAMPLE OUTPUT (first 10 records):")
    count = 0
    for date, vals in data.items():
        print(date, vals)
        count += 1
        if count >= 10:
            break
