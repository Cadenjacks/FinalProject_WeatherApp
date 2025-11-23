# weather_processor.py
"""
WeatherProcessor
Handles all user interaction (menus, prompts, validation) and manages:
- Scraping
- Database storage
- Plot selection (boxplot, lineplot, min/max subplots)
- Orchestrating DBOperations, WeatherScraper, PlotOperations

Author: Caden Jackson

Created on: 11/22/2025
Last Updated: 11/23/2025 

"""

import logging
from datetime import datetime

from scrape_weather import WeatherScraper
from db_operations import DBOperations
from plot_operations import PlotOperations

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# =======================================================
# WEATHER PROCESSOR CLASS
# =======================================================
class WeatherProcessor:
    """Main controller for user interaction, scraping, data updating, and plotting."""

    SCRAPE_START_YEAR = 2018 

    def __init__(self):
        self.db = DBOperations()
        self.plotter = PlotOperations()

    # -------------------------------------------
    # ENTRY POINT
    # -------------------------------------------
    def run(self):
        """Start program, show initial scraping menu, then show plotting menu."""

        print("\n=== Weather App ===")
        print("1) Download full weather dataset")
        print("2) Update existing dataset")
        print("3) Exit")

        choice = input("Enter choice (1/2/3): ").strip()

        if choice == "1":
            self.download_full_dataset()
        elif choice == "2":
            self.update_dataset()
        elif choice == "3":
            print("Exiting program.")
            return
        else:
            print("Invalid choice. Exiting.")
            return

        # After scraping/update → go to plot menu
        self.plot_menu()

    # -------------------------------------------
    # SCRAPING FUNCTIONS
    # -------------------------------------------
    def download_full_dataset(self):
        """Scrape all weather data from current year back to 2018."""
        print("\nDownloading dataset (2018 to present)...")

        scraper = WeatherScraper(start_year=self.SCRAPE_START_YEAR)
        data = scraper.scrape_all()

        print(f"Scraped {len(data)} days. Saving to database...")

        for date, temps in data.items():
            self.db.save_data(date, temps["Max"], temps["Min"], temps["Mean"])

        print("Full dataset download complete.")

    def update_dataset(self):
        """Scrape only missing dates from the last stored date to today."""
        print("\nUpdating existing dataset...")

        last_record = self.db.fetch_data(limit=1, order_desc=True)

        if not last_record:
            print("Database empty. Downloading full dataset instead.\n")
            self.download_full_dataset()
            return

        last_date = last_record[0][0]
        last_year = int(last_date[:4])

        print(f"Latest date stored in DB: {last_date}")

        scraper = WeatherScraper(start_year=last_year)
        new_data = scraper.scrape_all()

        print(f"Scraped {len(new_data)} new days. Saving...")

        for date, temps in new_data.items():
            self.db.save_data(date, temps["Max"], temps["Min"], temps["Mean"])

        print("Update complete.")

    # -------------------------------------------
    # PLOT MENU
    # -------------------------------------------
    def plot_menu(self):
        """Menu for selecting plot type."""
        while True:
            print("\n=== Plot Menu ===")
            print("1) Boxplot (single year)")
            print("2) Line Plot (mean temps for a month/year)")
            print("3) Min/Max Subplot (month/year)")
            print("4) Exit")
            choice = input("Choice: ").strip()

            if choice == "1":
                self.boxplot_prompt()
            elif choice == "2":
                self.lineplot_prompt()
            elif choice == "3":
                self.minmax_prompt()
            elif choice == "4":
                print("Goodbye!")
                return
            else:
                print("Invalid choice.")

    # -------------------------------------------
    # PLOT PROMPTS
    # -------------------------------------------
    def boxplot_prompt(self):
        """User selects a single year → show boxplot for all 12 months of that year."""
        print("\nBoxplot for a single year.")
        year = input("Enter year (e.g., 2024): ").strip()

        try:
            year = int(year)
        except ValueError:
            print("Invalid year.")
            return

        rows = self.db.fetch_mean_temps_range(year, year)
        if not rows:
            print("No data for that year.")
            return

        self.plotter.plot_monthly_boxplot(rows, year, year)

    def lineplot_prompt(self):
        """User selects a year+month → show mean-temp line plot."""
        print("\nLine plot of daily mean temperatures.")

        year = input("Enter year (e.g., 2024): ").strip()
        month = input("Enter month (1-12): ").strip()

        try:
            year = int(year)
            month = int(month)
            if not 1 <= month <= 12:
                raise ValueError
        except ValueError:
            print("Invalid month/year.")
            return

        rows = self.db.fetch_mean_temps_for_month(year, month)
        if not rows:
            print("No data for that month/year.")
            return

        self.plotter.plot_daily_lineplot(rows, year, month)

    def minmax_prompt(self):
        """User selects a year+month → show min/max subplot."""
        print("\nMin/Max Temperatures — Subplot View")

        year = input("Enter year (e.g., 2024): ").strip()
        month = input("Enter month (1-12): ").strip()

        try:
            year = int(year)
            month = int(month)
            if not 1 <= month <= 12:
                raise ValueError
        except ValueError:
            print("Invalid month/year.")
            return

        rows = self.db.fetch_min_max_for_month(year, month)
        if not rows:
            print("No data for that month/year.")
            return

        self.plotter.plot_min_max_subplots(rows, year, month)
