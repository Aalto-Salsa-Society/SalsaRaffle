#!/usr/bin/env python
"""All code for creating the groups for the ASS dance classes."""

import enum
import itertools
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Final

import polars as pl
from xlsxwriter import Workbook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# A seed for reproducible but random results
RANDOM_SEED: Final = 455
MAX_PER_GROUP: Final = 15

LEADER_LABEL: Final = "L"
FOLLOWER_LABEL: Final = "F"
LEADER: Final = "Leader"
FOLLOWER: Final = "Follower"
ROLE_TO_LABEL: Final = {LEADER: LEADER_LABEL, FOLLOWER: FOLLOWER_LABEL}


class Col(enum.StrEnum):
    """All columns used or created."""

    HANDLE = "handle"
    NAME = "name"
    EMAIL = "email"
    P1 = "first_preference"
    P1_ROLE = "first_preference_role"
    HAS_P2 = "has_second_preference"
    P2 = "second_preference"
    P2_ROLE = "second_preference_role"
    ONLY_1 = "only_1_preference"
    TIMESLOT_1 = "timeslot_1"
    TIMESLOT_2 = "timeslot_2"
    HIGH_PRIO = "high_priority"
    MED_PRIO = "medium_priority"
    LOW_PRIO = "low_priority"

    # Membership columns
    MEMBER = "member"
    PAID = "paid"
    APPROVED = "approved"


HAS_P2_VALUE: Final = "Yes"

# Required files
INPUT_DIR: Final = Path("data") / "input"
MEMBERS_FILE: Final = INPUT_DIR / "Members.xlsx"
OLD_GROUPS_FILE: Final = INPUT_DIR / "groups.csv"
OLD_ATTENDANCE_FILE: Final = INPUT_DIR / "attendance.xlsx"
RESPONSE_FILE: Final = INPUT_DIR / "responses.xlsx"

# Created files
OUTPUT_DIR: Final = Path("data") / "output"
GROUPS_FILE: Final = OUTPUT_DIR / "groups.xlsx"
NEW_ATTENDANCE_FILE: Final = OUTPUT_DIR / "attendance.xlsx"
RAW_GROUPS_FILE: Final = OUTPUT_DIR / "groups.csv"


def get_members(condition: pl.Expr | str = Col.APPROVED) -> pl.Series:
    """Return a list of ASS members."""
    if not MEMBERS_FILE.exists():
        logging.warning("No members list found")
        return pl.Series(dtype=pl.Utf8)

    return (
        pl.read_excel(MEMBERS_FILE)
        .filter(condition)
        .with_columns(pl.col(Col.HANDLE).str.to_lowercase())
        .get_column(Col.HANDLE)
    )


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


REGISTRATION_COLUMNS: Final = {
    "Telegram handle": Col.HANDLE.value,
    "Full name (first and last name)": Col.NAME.value,
    "Email address": Col.EMAIL.value,
    "First preference": Col.P1.value,
    "First preference dance role": Col.P1_ROLE.value,
    "I have a second preference": Col.HAS_P2.value,
    "Second preference": Col.P2.value,
    "Second preference dance role": Col.P2_ROLE.value,
    "Both preferences": Col.ONLY_1.value,
}


class Timeslot(enum.IntEnum):
    """Represents a timeslot in which a class can happen."""

    MONDAY = enum.auto()
    TUESDAY_1 = enum.auto()
    TUESDAY_2 = enum.auto()
    THURSDAY = enum.auto()


GROUP_INFO: Final[dict[str, tuple[str, Timeslot]]] = {
    "Salsa Level 1": ("S1", Timeslot.THURSDAY),
    "Salsa Level 2": ("S2", Timeslot.MONDAY),
    "Salsa Level 3": ("S3", Timeslot.MONDAY),
    "Salsa Level 4": ("S4", Timeslot.THURSDAY),
    "Bachata Level 1": ("B1", Timeslot.TUESDAY_1),
    "Bachata Level 2": ("B2", Timeslot.TUESDAY_2),
}
GROUP_TO_TIMESLOT: Final = {group: timeslot for group, (_, timeslot) in GROUP_INFO.items()}
GROUP_TO_LABEL: Final = {group: label for group, (label, _) in GROUP_INFO.items()}
LABEL_TO_GROUP: Final = {v: k for k, v in GROUP_TO_LABEL.items()}
ALL_GROUPS: Final = [
    group + role
    for group, role in itertools.product(
        GROUP_TO_LABEL.values(),
        (LEADER_LABEL, FOLLOWER_LABEL),
    )
]

# Useful expressions
ACCEPTED: Final = pl.any_horizontal(pl.col(ALL_GROUPS).lt(MAX_PER_GROUP)).fill_null(value=False)
REJECTED: Final = pl.all_horizontal(pl.col(ALL_GROUPS).ge(MAX_PER_GROUP)).fill_null(value=True)
LOW_PRIO: Final = pl.col(Col.LOW_PRIO)


def get_class_registrations() -> pl.LazyFrame:
    """
    Load the data from the signup responses and creates the initial dataframe.

    :return: Initial dataframe

    Columns
    -------
    The columns are the columns found in the `Col` enum together with a column
    for each group. The groups can be found in the `ALL_GROUPS` list. Example:
    S1F: int
        Position in queue for the S1MF group (Salsa Level 1 Follower)
    B2L: int
        Position in queue for the B2L group (Bachata Level 2 Leader)
    """
    return (
        pl.read_excel(RESPONSE_FILE)
        .select(REGISTRATION_COLUMNS)
        .rename(REGISTRATION_COLUMNS)
        .unique(subset=Col.HANDLE, keep="last", maintain_order=True)
        .sample(fraction=1, shuffle=True, seed=RANDOM_SEED)
        .lazy()
        .drop_nulls(Col.P1)
        .with_columns(
            pl.col(Col.HANDLE).str.to_lowercase(),
            pl.col(Col.EMAIL).str.to_lowercase(),
            pl.col(Col.HAS_P2).eq(HAS_P2_VALUE),
            pl.col(Col.ONLY_1).is_null(),
        )
        .with_columns(
            # Exclude second preference if they do not have one
            # The second preference stays in the Google form even when they
            # change their mind and select no second preference
            pl.when(pl.col(Col.HAS_P2)).then(pl.col(Col.P2)).otherwise(pl.lit(value=None)).alias(Col.P2),
            pl.when(pl.col(Col.HAS_P2)).then(pl.col(Col.ONLY_1)).otherwise(pl.lit(value=None)).alias(Col.ONLY_1),
            pl.when(pl.col(Col.HAS_P2)).then(pl.col(Col.P2_ROLE)).otherwise(pl.lit(value=None)).alias(Col.P2_ROLE),
        )
        .with_columns(
            # Salsa Level 1, Follower -> S1F
            pl.col(Col.P1).replace(GROUP_TO_LABEL) + pl.col(Col.P1_ROLE).str.slice(0, length=1),
            pl.col(Col.P2).replace(GROUP_TO_LABEL) + pl.col(Col.P2_ROLE).str.slice(0, length=1),
            pl.col(Col.P1).replace(GROUP_TO_TIMESLOT).alias(Col.TIMESLOT_1),
            pl.col(Col.P2).replace(GROUP_TO_TIMESLOT).alias(Col.TIMESLOT_2),
        )
        .with_columns(
            pl.col(Col.HANDLE).is_in(get_high_priority()).fill_null(value=False).alias(Col.HIGH_PRIO),
            pl.col(Col.HANDLE).is_in(get_low_priority()).fill_null(value=False).alias(Col.LOW_PRIO),
            pl.col(Col.HANDLE).is_in(get_members()).fill_null(value=False).alias(Col.MEMBER),
            pl.col(Col.HANDLE).is_in(get_members(Col.PAID)).fill_null(value=False).alias(Col.PAID),
            # Only allow 1 preference if the second preference is not the same class as the first
            (pl.col(Col.ONLY_1) | pl.col(Col.TIMESLOT_1).eq(pl.col(Col.TIMESLOT_2))).alias(Col.ONLY_1),
        )
        .with_columns(
            # Remove high priority if they already have low priority
            (pl.col(Col.HIGH_PRIO) & ~pl.col(Col.LOW_PRIO)).alias(Col.HIGH_PRIO),
            (~pl.col(Col.HIGH_PRIO) & ~pl.col(Col.LOW_PRIO)).alias(Col.MED_PRIO),
        )
        .with_columns(pl.lit(value=None).cast(pl.Int64).alias(group) for group in ALL_GROUPS)
    )


def assign(lf: pl.LazyFrame, assign_rule: Callable[[str], pl.Expr]) -> pl.LazyFrame:
    """Assign a spot in all groups according to the assign_rule."""
    for group in ALL_GROUPS:
        assignees = assign_rule(group) & pl.col(group).is_null()
        # fmt: off
        starting_point = (
            pl.when(pl.col(group).max().is_null())
            .then(0)
            .otherwise(pl.col(group).max())
        )
        lf = lf.with_columns(
            pl.when(assignees)
            .then(assignees.cum_sum() + starting_point)
            .otherwise(pl.col(group))
            .alias(group)
        )
        # fmt: on

    return lf


def create_group_excel_file(df: pl.DataFrame) -> None:
    """Create an excel file with all the final group divisions."""
    with Workbook(GROUPS_FILE) as workbook:
        for group_label in GROUP_TO_LABEL.values():
            leaders = (
                df.filter(pl.col(group_label + LEADER_LABEL).is_not_null())
                .sort(group_label + LEADER_LABEL)
                .select(pl.col(Col.NAME).alias(LEADER + " Name"))
            )
            followers = (
                df.filter(pl.col(group_label + FOLLOWER_LABEL).is_not_null())
                .sort(group_label + FOLLOWER_LABEL)
                .select(pl.col(Col.NAME).alias(FOLLOWER + " Name"))
            )
            pl.concat((leaders, followers), how="horizontal").write_excel(
                workbook=workbook,
                worksheet=LABEL_TO_GROUP[group_label],
                autofit=True,
                header_format={"bold": True},
            )


def create_attendance_sheet(
    df: pl.DataFrame,
    workbook: Workbook,
    group_label: str,
    role: str,
) -> None:
    """Create one sheet in the attendance workbook."""
    (
        df.filter(pl.col(group_label + ROLE_TO_LABEL[role]).is_not_null())
        .sort(group_label + ROLE_TO_LABEL[role])
        .select(
            pl.col(Col.NAME).alias("Name"),
            pl.col(Col.HANDLE).alias("Handle"),
            pl.col(Col.MEMBER).alias("Member"),
            pl.col(Col.PAID).alias("Paid"),
        )
        .with_columns(pl.lit(value=None).alias(keys) for keys in ATTENDANCE_WEEKS)
        .write_excel(
            workbook=workbook,
            worksheet=LABEL_TO_GROUP[group_label] + " " + role,
            autofit=True,
            header_format={"bold": True},
        )
    )


def print_results(groups_lazy: pl.LazyFrame) -> None:
    """Print the results of the program."""
    groups = groups_lazy.collect()

    print("---")
    print("Number of applicants:")
    print("Total:   ", len(groups))
    print("Accepted:", len(groups.filter(ACCEPTED)))
    print("Rejected:", len(groups.filter(REJECTED)))
    print("Rejected and not low priority:")
    print(groups.filter(REJECTED & ~LOW_PRIO).select([Col.NAME, Col.HANDLE]))
    print("---")
    accepted_emails = groups.filter(ACCEPTED).get_column(Col.EMAIL).unique().to_list()
    print(f"Accepted emails {len(accepted_emails)}:")
    print(*accepted_emails, sep=", ")
    print("---")
    rejected_emails = groups.filter(REJECTED).get_column(Col.EMAIL).unique().to_list()
    print(f"Rejected emails {len(rejected_emails)}:")
    print(*rejected_emails, sep=", ")
    print("---")

    groups.write_csv(RAW_GROUPS_FILE)
    create_group_excel_file(groups)

    with Workbook(NEW_ATTENDANCE_FILE) as workbook:
        for group in GROUP_TO_LABEL.values():
            create_attendance_sheet(groups, workbook, group, LEADER)
            create_attendance_sheet(groups, workbook, group, FOLLOWER)


def main() -> None:
    """Run the main program."""
    if not INPUT_DIR.exists():
        INPUT_DIR.mkdir(parents=True)
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)

    groups = get_class_registrations()

    assignments: list[Callable[[str], pl.Expr]] = [
        # High priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.HIGH_PRIO),
        # High priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.HIGH_PRIO) & REJECTED,
        # Medium priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.MED_PRIO),
        # Medium priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.MED_PRIO) & REJECTED,
        # Med and high priority (not low) second preference that want to join more than 1 class
        lambda group: pl.col(Col.P2).eq(group) & ~pl.col(Col.LOW_PRIO) & ~pl.col(Col.ONLY_1),
        # All low priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.LOW_PRIO),
        # All low priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.LOW_PRIO) & REJECTED,
        # All low priority second preference that want to join more than 1 class
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.LOW_PRIO) & ~pl.col(Col.ONLY_1),
    ]
    for assign_rule in assignments:
        groups = assign(groups, assign_rule)

    print_results(groups)


if __name__ == "__main__":
    main()
