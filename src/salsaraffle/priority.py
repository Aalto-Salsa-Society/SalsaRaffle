"""All functions related to high and low priority registrations."""

import logging
from typing import Final

import polars as pl

from salsaraffle.column import Col
from salsaraffle.expressions import LOW_PRIO, REJECTED
from salsaraffle.settings import OLD_ATTENDANCE_FILE, OLD_GROUPS_FILE


def get_high_priority() -> pl.Series:
    """Return a list of people who were left out last cycle."""
    if not OLD_GROUPS_FILE.exists():
        logging.warning("No groups file found in input")
        return pl.Series(dtype=pl.Utf8)

    return (
        pl.scan_csv(OLD_GROUPS_FILE)
        .with_columns(pl.col(Col.HANDLE).str.to_lowercase())
        .filter(REJECTED & ~LOW_PRIO)
        .collect()
        .get_column(Col.HANDLE)
    )


NO_SHOW: Final = "no_show"
GAVE_NOTICE: Final = "gave_notice"
ATTENDANCE_WEEKS: Final = {
    "Week 1": "week1",
    "Week 2": "week2",
    "Week 3": "week3",
    "Week 4": "week4",
}
ATTENDANCE_COLUMNS: Final = {"Handle": Col.HANDLE.value, **ATTENDANCE_WEEKS}


def get_low_priority() -> pl.Series:
    """
    Return a list of people who are considered a disruption.

    Members with a "No show" or 2 "Gave notice" are considered a disruption.
    """
    if not OLD_ATTENDANCE_FILE.exists():
        logging.warning("No attendance file found")
        return pl.Series(dtype=pl.Utf8)

    all_sheets = pl.read_excel(OLD_ATTENDANCE_FILE, sheet_id=0)
    no_show = pl.col(ATTENDANCE_WEEKS.values()).eq_missing("No show")
    gave_notice = pl.col(ATTENDANCE_WEEKS.values()).eq_missing("Gave notice")
    return (
        pl.concat(all_sheets.values())
        .lazy()
        .select(ATTENDANCE_COLUMNS)
        .rename(ATTENDANCE_COLUMNS)
        .drop_nulls(Col.HANDLE)
        .with_columns(
            pl.col(Col.HANDLE).str.to_lowercase(),
            pl.any_horizontal(no_show).alias(NO_SHOW),
            pl.sum_horizontal(gave_notice).ge(2).alias(GAVE_NOTICE),
        )
        .filter(pl.col(NO_SHOW) | pl.col(GAVE_NOTICE))
        .select(Col.HANDLE)
        .collect()
        .get_column(Col.HANDLE)
    )
