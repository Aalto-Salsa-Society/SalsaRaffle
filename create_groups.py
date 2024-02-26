#!/usr/bin/env python
"""All code for creating the groups for the ASS dance classes."""

import enum
import itertools
import logging
import os
from typing import Callable

import polars as pl
from xlsxwriter import Workbook

# A seed for reproducible but random results
RANDOM_SEED = 455

CSV_PATH = "responses.csv"
OUTPUT_PATH = "groups.csv"
REGISTRATION_COLUMNS = {
    "Telegram handle": "handle",
    "Full name (first and last name)": "name",
    "Email address": "email",
    "First preference": "1",
    "First preference dance role": "1_role",
    "Second preference": "2",
    "Second preference dance role": "2_role",
    "first_only": "only_1",
}


class Timeslot(enum.IntEnum):
    """Represents a timeslot in which a class can happen."""

    MONDAY = enum.auto()
    TUESDAY_1 = enum.auto()
    TUESDAY_2 = enum.auto()
    THURSDAY = enum.auto()


GROUPS_TIMESLOTS_MAP = {
    "Salsa Level 1": Timeslot.THURSDAY,
    "Salsa Level 2": Timeslot.MONDAY,
    "Salsa Level 3": Timeslot.MONDAY,
    "Salsa Level 4": Timeslot.THURSDAY,
    "Bachata Level 1": Timeslot.TUESDAY_1,
    "Bachata Level 2": Timeslot.TUESDAY_2,
}
GROUPS_MAP = {
    "Salsa Level 1": "S1",
    "Salsa Level 2": "S2",
    "Salsa Level 3": "S3",
    "Salsa Level 4": "S4",
    "Bachata Level 1": "B1",
    "Bachata Level 2": "B2",
}
GROUP_LABELS = {v: k for k, v in GROUPS_MAP.items()}
GROUPS = list(
    itertools.chain(
        (group + "L" for group in GROUPS_MAP.values()),
        (group + "F" for group in GROUPS_MAP.values()),
    )
)

ATTENDANCE_COLUMNS = {
    "Handle": "handle",
    "Week 1": "week1",
    "Week 2": "week2",
    "Week 3": "week3",
    "Week 4": "week4",
}
ATTENDANCE_WEEKS = list(ATTENDANCE_COLUMNS.values())[1:]

MAX_PER_GROUP = 15


def get_high_priority() -> pl.Series:
    """Return a list of people who were left out last cycle (manually created)."""
    if "high_prio.csv" not in os.listdir():
        logging.warning("No high priority list found")
        return pl.Series(dtype=pl.Utf8)
    return (
        pl.scan_csv("high_prio.csv")
        .with_columns(pl.col("handle").str.to_lowercase())
        .collect()
        .get_column("handle")
    )


def get_low_priority() -> pl.Series:
    """
    Return a list of people who are considered a disruption.

    Members with a "No show" or 2 "Gave notice" are considered a disruption.
    """
    if not any("attendance_" in file for file in os.listdir()):
        logging.warning("No attendance files found")
        return pl.Series(dtype=pl.Utf8)

    return (
        pl.scan_csv("attendance_*.csv")
        .select(ATTENDANCE_COLUMNS)
        .rename(ATTENDANCE_COLUMNS)
        .drop_nulls("handle")
        .with_columns(
            pl.col("handle").str.to_lowercase(),
            pl.any_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("No show")).alias("no_show"),
            (
                pl.sum_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("Gave notice"))
                .ge(2)
                .alias("gave_notice")
            ),
        )
        .filter(pl.col("no_show") | pl.col("gave_notice"))
        .select("handle")
        .collect()
        .get_column("handle")
    )


def get_class_registrations() -> pl.LazyFrame:
    """
    Load the initial data from the signup responses and creates the initial dataframe.

    :return: Initial dataframe

    Columns
    -------
    handle: str
        Telegram handle
    name: str
        Full name (in lower case)
    email: str
        Email address
    high_prio: bool
        The person is high priority
    med_prio: bool
        The person is medium priority (not high, nor low priority)
    low_prio: bool
        The person is low priority (cannot be high priority)
    1: str (e.g. S1MF, S2L, B2L)
        First preference
    2: str (e.g. S1TL, S2F, B2L)
        Second preference
    only_1: bool
        The person only wants to join first preference
    S1MF: int
        Position in queue for the S1MF group (Salsa Level 1 M (Monday) Follower)
    B2L: int
        Position in queue for the B2L group (Bachata Level 2 Leader)
    (see GROUPS_MAP for the full list of groups)
    """
    return (
        pl.scan_csv(CSV_PATH)
        .select(REGISTRATION_COLUMNS)
        .rename(REGISTRATION_COLUMNS)
        .drop_nulls("1")
        .with_columns(
            # Salsa Level 1, Follower -> S1F
            pl.col("1").map_dict(GROUPS_MAP) + pl.col("1_role").str.slice(0, length=1),
            pl.col("2").map_dict(GROUPS_MAP) + pl.col("2_role").str.slice(0, length=1),
            pl.col("handle").str.to_lowercase(),
            pl.col("1").map_dict(GROUPS_TIMESLOTS_MAP).alias("timeslot_1"),
            pl.col("2").map_dict(GROUPS_TIMESLOTS_MAP).alias("timeslot_2"),
        )
        .with_columns(
            pl.col("handle").is_in(get_high_priority()).fill_null(value=False).alias("high_prio"),
            pl.col("handle").is_in(get_low_priority()).fill_null(value=False).alias("low_prio"),
            # Only allow 1 preference if the second preference is not the same class as the first
            (
                pl.col("only_1")
                .is_null()
                .or_(pl.col("timeslot_1").eq(pl.col("timeslot_2")))
                .alias("only_1")
            ),
        )
        .with_columns(
            # Remove high priority if they already have low priority
            pl.col("high_prio") & ~pl.col("low_prio"),
            (~pl.col("high_prio") & ~pl.col("low_prio")).alias("med_prio"),
        )
        .with_columns(pl.lit(value=None).alias(group) for group in GROUPS)
        .collect()
        # Sample only works on a DataFrame, not a LazyFrame
        .sample(fraction=1, shuffle=True, seed=RANDOM_SEED)
        .lazy()
    )


def assign(lf: pl.LazyFrame, assign_rule: Callable[[str], pl.Expr]) -> pl.LazyFrame:
    """Assign a spot in all groups according to the assign_rule."""
    for group in GROUPS:
        assignees = assign_rule(group) & pl.col(group).is_null()
        starting_point = (
            pl.when(pl.col(group).max().is_null()).then(0).otherwise(pl.col(group).max())
        )
        lf = lf.with_columns(
            pl.when(assignees)
            .then(assignees.cumsum() + starting_point)
            .otherwise(pl.col(group))
            .alias(group)
        )

    return lf


def print_gmail_emails(lf: pl.LazyFrame) -> None:
    """Print the emails of the people that have been accepted."""
    emails = lf.unique().collect().get_column("email").to_list()
    print(f"Accepted emails {len(emails)}:")
    print(*emails, sep=", ")


def create_group_excel_file(df: pl.DataFrame) -> None:
    """Create an excel file with all the final group divisions."""
    with Workbook("groups.xlsx") as workbook:
        for group in GROUPS_MAP.values():
            leaders = (
                df.filter(pl.col(group + "L").is_not_null())
                .sort(group + "L")
                .select(pl.col("name").alias("Leader Name"))
            )
            followers = (
                df.filter(pl.col(group + "F").is_not_null())
                .sort(group + "F")
                .select(pl.col("name").alias("Follower Name"))
            )
            pl.concat((leaders, followers), how="horizontal").write_excel(
                workbook=workbook,
                worksheet=GROUP_LABELS[group],
                autofit=True,
                header_format={"bold": True},
            )


def create_attendance_sheet(
    df: pl.DataFrame,
    workbook: Workbook,
    group: str,
    role: str,
) -> None:
    """Create one sheet in the attendance workbook."""
    role_letter = role[0].upper()
    (
        df.filter(pl.col(group + role_letter).is_not_null())
        .sort(group + role_letter)
        .select(
            pl.col("name").alias("Name"),
            pl.col("handle").alias("Handle"),
        )
        .with_columns(pl.lit(value=None).alias(keys) for keys in list(ATTENDANCE_COLUMNS)[1:])
        .write_excel(
            workbook=workbook,
            worksheet=GROUP_LABELS[group] + " " + role,
            autofit=True,
            header_format={"bold": True},
        )
    )


def main() -> None:
    """Run the main program."""
    lf = get_class_registrations()

    # A person is accepted if they got a number less than 15
    accepted = pl.any_horizontal(pl.col(GROUPS).le(MAX_PER_GROUP))
    # Required due to Kleene's logic
    rejected = pl.any_horizontal(pl.col(GROUPS).gt(MAX_PER_GROUP))

    # Assign high priority first preference
    lf = assign(lf, lambda group: pl.col("1").eq(group) & pl.col("high_prio"))
    # Assign high priority second preference that are not in first preference
    lf = assign(lf, lambda group: pl.col("2").eq(group) & pl.col("high_prio") & rejected)
    # Assign medium priority first preference
    lf = assign(lf, lambda group: pl.col("1").eq(group) & pl.col("med_prio"))
    # Assign medium priority second preference that are not in first preference
    lf = assign(lf, lambda group: pl.col("2").eq(group) & pl.col("med_prio") & rejected)
    # Assign med and high priority (not low) second preference that want to join more than 1 class
    lf = assign(lf, lambda group: pl.col("2").eq(group) & ~pl.col("low_prio") & ~pl.col("only_1"))
    # Assign all low priority first preference
    lf = assign(lf, lambda group: pl.col("1").eq(group) & pl.col("low_prio"))
    # Assign all low priority second preference that are not in first preference
    lf = assign(lf, lambda group: pl.col("2").eq(group) & pl.col("low_prio") & rejected)
    # Assign all low priority second preference that want to join more than 1 class
    lf = assign(lf, lambda group: pl.col("2").eq(group) & pl.col("low_prio") & ~pl.col("only_1"))

    # Create desired outputs
    print_gmail_emails(lf.filter(accepted))

    df = lf.drop("email").collect()
    print(str(df))
    df.write_csv(OUTPUT_PATH)
    create_group_excel_file(df)

    with Workbook("attendance.xlsx") as workbook:
        for group in GROUPS_MAP.values():
            create_attendance_sheet(df, workbook, group, "Leader")
            create_attendance_sheet(df, workbook, group, "Follower")


if __name__ == "__main__":
    main()
