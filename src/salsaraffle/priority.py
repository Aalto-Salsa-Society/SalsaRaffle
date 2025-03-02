"""All functions related to high and low priority registrations."""

import logging
from typing import Final

import polars as pl

from salsaraffle.column import Col
from salsaraffle.results import ATTENDANCE_WEEKS
from salsaraffle.settings import (
    GROUP_INFO,
    MAX_PER_GROUP,
    OLD_ATTENDANCE_FILE,
    OLD_GROUPS_FILE,
)


def get_high_priority() -> pl.Series:
    """Return a list of people who were left out last cycle."""
    if not OLD_GROUPS_FILE.exists():
        logging.warning("No groups file found in input")
        return pl.Series(dtype=pl.Utf8)

    if not OLD_ATTENDANCE_FILE.exists():
        logging.warning("No attendance file found")
        return pl.Series(dtype=pl.Utf8)

    all_sheets = pl.read_excel(OLD_ATTENDANCE_FILE, sheet_id=0)

    # Find maximum number of participants
    max_per_group = {group: MAX_PER_GROUP for group, _, _ in GROUP_INFO}
    for name, sheet in all_sheets.items():
        group = name.rsplit(maxsplit=1)[0]
        max_per_group[group] = min(len(sheet), max_per_group[group])

    # Find people who did not get to go to class
    missed_out = []
    for name, sheet in all_sheets.items():
        group = name.rsplit(maxsplit=1)[0]
        max_participants = max_per_group[group]
        missed_out.append(sheet[max_participants:])
    missed_out_handles = (
        pl.concat(missed_out)
        .select(ATTENDANCE_COLUMNS)
        .rename(ATTENDANCE_COLUMNS)
        .drop_nulls(Col.HANDLE)
    )

    # Originally signed up handles that should have gotten a spot
    signed_up_handles = (
        pl.scan_csv(OLD_GROUPS_FILE)
        .with_columns(pl.col(Col.HANDLE).str.to_lowercase())
        .filter(~pl.col(Col.LOW_PRIO))
        .collect()
        .get_column(Col.HANDLE)
    )

    return missed_out_handles.filter(
        pl.col(Col.HANDLE).is_in(signed_up_handles)
    ).get_column(Col.HANDLE)


NO_SHOW: Final = "no_show"
GAVE_NOTICE: Final = "gave_notice"
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
        .collect()
        .get_column(Col.HANDLE)
    )
