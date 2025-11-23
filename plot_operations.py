# plot_operations.py
"""
PlotOperations — plotting helpers that accept raw DB rows and
do all required processing inside this class.

All plotting code and data preparation for plotting is self-contained
in this module, as required by the assignment.

Author: Caden Jackson

Created on: 11/22/2025
Last updated: 11/23/2025
"""

import logging
import math
from datetime import datetime

import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PlotOperations:
    """Handles boxplots, lineplots, and subplots using DB data."""

    MONTH_LABELS = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ]

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    # ============================================================
    #  INTERNAL HELPERS (data processing for plotting)
    # ============================================================
    def _build_month_dict_from_rows(self, rows):
        """
        Given rows: list of (sample_date (str YYYY-MM-DD), avg_temp (float)),
        return dict {1: [...], 2: [...], ..., 12: [...]} where key is month.
        """
        month_dict = {m: [] for m in range(1, 13)}

        for date_str, avg in rows:
            if avg is None:
                continue

            date_str = date_str.strip()[:10]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.warning("Skipping invalid date row in boxplot: %s", date_str)
                continue

            month = dt.month
            month_dict[month].append(avg)

        return month_dict

    def _build_daily_lists(self, rows):
        """
        Given rows: list of (sample_date (str), avg_temp(float)) ordered by date ASC,
        return (dates_list_of_datetimes, temps_list)
        """
        dates = []
        temps = []

        for date_str, avg in rows:
            if avg is None:
                continue

            date_str = date_str.strip()[:10]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.warning("Skipping invalid date row in line plot: %s", date_str)
                continue

            dates.append(dt)
            temps.append(avg)

        return dates, temps

    def _build_min_max_lists(self, rows):
        """
        Given rows: list of (sample_date (str), max_temp(float), min_temp(float)),
        return (dates_list_of_datetimes, max_list, min_list)
        """
        dates = []
        maxs = []
        mins = []

        for date_str, max_t, min_t in rows:
            date_str = date_str.strip()[:10]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.warning("Skipping invalid date row in min/max plot: %s", date_str)
                continue

            dates.append(dt)
            maxs.append(max_t)
            mins.append(min_t)

        return dates, maxs, mins

    # ============================================================
    #  PUBLIC PLOTTING METHODS
    # ============================================================

    # ---------------------------
    # Boxplot: one box per month (Jan..Dec)
    # ---------------------------
    def plot_monthly_boxplot(self, rows, year_start: int, year_end: int):
        """
        Create a boxplot with one box per month (Jan..Dec).

        Args:
            rows: list of (sample_date (str), avg_temp (float)) across the year range.
            year_start, year_end: used in title.
        """
        self.logger.info("Plotting monthly boxplot for %s-%s", year_start, year_end)

        month_dict = self._build_month_dict_from_rows(rows)

        # Ensure keys 1..12 exist and convert empty lists -> [nan]
        groups = []
        labels = []

        for m in range(1, 13):
            vals = month_dict.get(m, [])
            if not vals:
                # placeholder so there is a box position for the month
                groups.append([math.nan])
            else:
                groups.append(vals)
            labels.append(self.MONTH_LABELS[m - 1])

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.boxplot(groups, labels=labels, showfliers=True)
        ax.set_title(f"Monthly Mean Temperature Distribution ({year_start}–{year_end})")
        ax.set_xlabel("Month")
        ax.set_ylabel("Mean Temperature (°C)")
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        plt.tight_layout()
        plt.show()

    # ---------------------------
    # Line plot: daily mean temps for a single month
    # ---------------------------
    def plot_daily_lineplot(self, rows, year: int, month: int):
        """
        Plot a line of daily mean temperatures for a given month/year.

        Args:
            rows: list of (sample_date (str), avg_temp(float)) for that month.
            year, month: for title.
        """
        self.logger.info("Plotting daily line plot for %04d-%02d", year, month)

        dates, temps = self._build_daily_lists(rows)

        if not dates or not temps:
            self.logger.warning("No data to plot for %04d-%02d", year, month)
            print("No data available for that month/year.")
            return

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(dates, temps, marker="o", linestyle="-")
        ax.set_title(f"Daily Mean Temperatures for {year}-{month:02d}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Mean Temperature (°C)")
        ax.grid(True, linestyle="--", alpha=0.5)
        fig.autofmt_xdate()  # nice date formatting
        plt.tight_layout()
        plt.show()

    # ---------------------------
    # Subplots: max + min temps in one window
    # ---------------------------
    def plot_min_max_subplots(self, rows, year: int, month: int):
        """
        Create two stacked subplots (max and min) for a month.

        Args:
            rows: list of (sample_date (str), max_temp(float), min_temp(float))
            year, month: for titles.
        """
        self.logger.info("Plotting min/max subplots for %04d-%02d", year, month)

        dates, max_temps, min_temps = self._build_min_max_lists(rows)

        if not dates:
            self.logger.warning("No dates to plot for min/max subplots for %04d-%02d", year, month)
            print("No data available for that month/year.")
            return

        fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        ax[0].plot(dates, max_temps, marker="o")
        ax[0].set_title(f"Max Temperatures for {year}-{month:02d}")
        ax[0].set_ylabel("Max Temp (°C)")
        ax[0].grid(True, linestyle="--", alpha=0.5)

        ax[1].plot(dates, min_temps, marker="o")
        ax[1].set_title(f"Min Temperatures for {year}-{month:02d}")
        ax[1].set_ylabel("Min Temp (°C)")
        ax[1].set_xlabel("Date")
        ax[1].grid(True, linestyle="--", alpha=0.5)

        fig.autofmt_xdate()
        plt.tight_layout()
        plt.show()
