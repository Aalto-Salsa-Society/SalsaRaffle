"""All functions that deal with registrations."""

from typing import Final

import polars as pl

from salsaraffle.column import Col, get_all_groups
from salsaraffle.members import get_members
from salsaraffle.priority import get_high_priority, get_low_priority
from salsaraffle.settings import GROUP_INFO, RANDOM_SEED, RESPONSE_FILE

REGISTRATION_COLUMNS: Final = {
    "Telegram handle": Col.HANDLE.value,
    "Full name (first and last name)": Col.NAME.value,
    "Email address": Col.EMAIL.value,
    "First preference": Col.P1_CLASS.value,
    "First preference dance role": Col.P1_ROLE.value,
    "I have a second preference": Col.HAS_P2.value,
    "Second preference": Col.P2_CLASS.value,
    "Second preference dance role": Col.P2_ROLE.value,
    "Both preferences": Col.ONLY_1.value,
}


def read() -> pl.LazyFrame:
    """Read registrations from Excel file and set column names."""
    return (
        pl.read_excel(RESPONSE_FILE)
        .select(REGISTRATION_COLUMNS)
        .rename(REGISTRATION_COLUMNS)
        .unique(subset=Col.HANDLE, keep="last", maintain_order=True)
        .sample(fraction=1, shuffle=True, seed=RANDOM_SEED)
        .lazy()
    )


def clean(registrations: pl.LazyFrame) -> pl.LazyFrame:
    """Convert column types and clean illegal values."""
    registrations = registrations.with_columns(
        pl.col(Col.HANDLE).str.to_lowercase(),
        pl.col(Col.EMAIL).str.to_lowercase(),
        pl.col(Col.HAS_P2).eq("Yes"),
        pl.col(Col.ONLY_1).is_null(),
    )

    # Exclude second preference if they do not have one
    # The second preference stays in the Google form even when they
    # change their mind and select no second preference
    return registrations.with_columns(
        (
            pl.when(pl.col(Col.HAS_P2))
            .then(pl.col(Col.P2_CLASS))
            .otherwise(pl.lit(value=None))
            .alias(Col.P2_CLASS)
        ),
        (
            pl.when(pl.col(Col.HAS_P2))
            .then(pl.col(Col.ONLY_1))
            .otherwise(pl.lit(value=None))
            .alias(Col.ONLY_1)
        ),
        (
            pl.when(pl.col(Col.HAS_P2))
            .then(pl.col(Col.P2_ROLE))
            .otherwise(pl.lit(value=None))
            .alias(Col.P2_ROLE)
        ),
    )


def add_priority_info(registrations: pl.LazyFrame) -> pl.LazyFrame:
    """Add priority information to registration data."""
    registrations = registrations.with_columns(
        pl.col(Col.HANDLE).is_in(get_high_priority()).fill_null(value=False).alias(Col.HIGH_PRIO),
        pl.col(Col.HANDLE).is_in(get_low_priority()).fill_null(value=False).alias(Col.LOW_PRIO),
    )

    # Remove high priority if they already have low priority
    return registrations.with_columns(
        (pl.col(Col.HIGH_PRIO) & ~pl.col(Col.LOW_PRIO)).alias(Col.HIGH_PRIO),
        (~pl.col(Col.HIGH_PRIO) & ~pl.col(Col.LOW_PRIO)).alias(Col.MED_PRIO),
    )


def add_member_info(registrations: pl.LazyFrame) -> pl.LazyFrame:
    """Add member information to registration data."""
    return registrations.with_columns(
        pl.col(Col.HANDLE).is_in(get_members()).fill_null(value=False).alias(Col.MEMBER),
        pl.col(Col.HANDLE).is_in(get_members(Col.PAID)).fill_null(value=False).alias(Col.PAID),
    )


def remove_simultaneous_classes(registrations: pl.LazyFrame) -> pl.LazyFrame:
    """Remove second choice if it happens at the same time as the first choice."""
    group_to_timeslot = {group: timeslot for group, _, timeslot in GROUP_INFO}

    # Add timeslot information
    registrations = registrations.with_columns(
        pl.col(Col.P1_CLASS).replace(group_to_timeslot).alias(Col.TIMESLOT_1),
        pl.col(Col.P2_CLASS).replace(group_to_timeslot).alias(Col.TIMESLOT_2),
    )

    # Remove overlapping timeslots
    return registrations.with_columns(
        # Only allow 1 preference if the second preference is not the same class as the first
        (pl.col(Col.ONLY_1) | pl.col(Col.TIMESLOT_1).eq(pl.col(Col.TIMESLOT_2))).alias(Col.ONLY_1),
    )


def add_extra_columns(registrations: pl.LazyFrame) -> pl.LazyFrame:
    """Add extra columns such as the empty groups division and choices."""
    group_to_label = {group: label for group, label, _ in GROUP_INFO}

    # P1_CLASS + P1_ROLE -> P1
    # Salsa Level 1, Follower -> S1F
    registrations = registrations.with_columns(
        (
            pl.col(Col.P1_CLASS)
            .replace(group_to_label)
            .add(pl.col(Col.P1_ROLE).str.slice(0, length=1))
            .alias(Col.P1)
        ),
        (
            pl.col(Col.P2_CLASS)
            .replace(group_to_label)
            .add(pl.col(Col.P2_ROLE).str.slice(0, length=1))
            .alias(Col.P2)
        ),
    )

    return registrations.with_columns(
        pl.lit(value=None).cast(pl.Int64).alias(group) for group in get_all_groups()
    )


def get_class_registrations() -> pl.LazyFrame:
    """Load the data from the signup responses and creates the initial dataframe."""
    registrations = read()
    registrations = clean(registrations)
    registrations = add_priority_info(registrations)
    registrations = add_member_info(registrations)
    registrations = remove_simultaneous_classes(registrations)
    return add_extra_columns(registrations)
