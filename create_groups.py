#!/usr/bin/env python3
"""All code for creating the groups for the ASS dance classes."""

import os
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl

# A seed for reproducible but random results
RANDOM_SEED = 455
rng = np.random.default_rng(RANDOM_SEED)

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
GROUPS_MAP = {
    "Salsa Level 1": "S1",
    "Salsa Level 2 M (Monday)": "S2M",
    "Salsa Level 2 T (Tuesday)": "S2T",
    "Salsa Level 3": "S3",
    "Bachata Level 1": "B1",
    "Bachata Level 2": "B2",
}

ATTENDANCE_COLUMNS = {"Handle": "handle", "Week 1": "week1", "Week 2": "week2", "Week 3": "week3", "Week 4": "week4"}
ATTENDANCE_WEEKS = list(ATTENDANCE_COLUMNS.values())[1:]

MAX_PER_GROUP = 15


def initial_data_setup() -> pd.DataFrame:
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
    high_prio = (
        pl.scan_csv("high_prio.csv")
        .with_columns(
            (pl.col("handle").str.to_lowercase()),
            (pl.lit(value=True).alias("high_prio")),
        )
        .group_by("handle")  # Remove duplicates
        .first()
    )

    low_prio = (
        pl.scan_csv([f for f in os.listdir() if Path(f).is_file() and f.startswith("attendance_")])
        .select(ATTENDANCE_COLUMNS)
        .rename(ATTENDANCE_COLUMNS)
        .drop_nulls("handle")
        .with_columns(
            (pl.col("handle").str.to_lowercase()),
            # A "No show" or 2 "Gave notice" is considered a disruption
            (
                pl.any_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("No show"))
                .or_(pl.sum_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("Gave notice")).ge(2))
                .alias("disruption")
            ),
        )
        .filter(pl.col("disruption"))
        .select("handle")
        .with_columns(pl.lit(value=True).alias("low_prio"))
        .group_by("handle")  # Remove duplicates
        .first()
    )

    df = (
        pl.scan_csv(CSV_PATH)
        .select(REGISTRATION_COLUMNS)
        .rename(REGISTRATION_COLUMNS)
        .drop_nulls("1")
        .with_columns(
            # Salsa Level 1, Follower -> S1F
            (pl.col("1").map_dict(GROUPS_MAP) + pl.col("1_role").str.slice(0, length=1)),
            (pl.col("2").map_dict(GROUPS_MAP) + pl.col("2_role").str.slice(0, length=1)),
            (pl.col("handle").str.to_lowercase()),
            (pl.col("only_1").is_null()),
        )
        .join(low_prio, on="handle", how="left")
        .with_columns(pl.col("low_prio").fill_null(value=False))
        .join(high_prio, on="handle", how="left")
        .with_columns(pl.col("high_prio").fill_null(value=False))
        # Remove high priority if they already have low priority
        .with_columns(pl.col("high_prio").and_(pl.col("low_prio").not_()))
        .with_columns(pl.col("high_prio").not_().and_(pl.col("only_1").not_()).alias("med_prio"))
        .collect()
    )
    df = df.with_columns(pl.lit(value=None).alias(group) for group in df.get_column("1").unique())

    return df.sample(fraction=1, shuffle=True, seed=RANDOM_SEED).to_pandas()


def assign_spot(df: pd.DataFrame, assign_rule: Callable[[str], pd.Series]) -> None:
    """Assign a spot in all groups according to the assign_rule."""
    for group in df["1"].unique():
        # Find all people that need to be assigned according to the rule and that are not already assigned
        assignees = assign_rule(group) & df[group].isna()
        # Assign them a spot in the group starting from the highest number in that group
        df.loc[assignees, group] = assignees.cumsum() + (df[group].max() if df[group].any() else 0)


def accepted(df: pd.DataFrame) -> pd.Series:
    """Return a boolean series that correspond to the handles that have been accepted in any group."""
    all_groups = df["1"].unique()
    return df[all_groups].le(MAX_PER_GROUP).any(axis=1)


def print_gmail_emails(df: pd.DataFrame) -> None:
    """Print the emails of the people that have been accepted."""
    print("Accepted emails:")
    print(*df[accepted(df)]["email"].unique().tolist(), sep=", ")


def create_group_excel_file(df: pd.DataFrame) -> None:
    """Create an excel file with all the final group divisions."""
    # We need the name for each of the short version labels
    group_labels = {v: k for k, v in GROUPS_MAP.items()}

    with pd.ExcelWriter("groups.xlsx") as writer:
        for group in GROUPS_MAP.values():
            leaders = df[df[group + "L"].notna()].sort_values(group + "L").reset_index()["name"].rename("Leader Name")
            followers = (
                df[df[group + "F"].notna()].sort_values(group + "F").reset_index()["name"].rename("Follower Name")
            )

            group_division = pd.concat([leaders, followers], axis=1)
            group_division.index = pd.RangeIndex(start=1, stop=len(group_division) + 1)
            group_division.to_excel(writer, sheet_name=group_labels[group])


def main() -> None:
    """Run the main program."""
    df = initial_data_setup()

    # Assign high priority first preference
    assign_spot(df, lambda group: df["1"].eq(group) & df["high_prio"])
    # Assign high priority second preference that are not in first preference
    assign_spot(df, lambda group: df["2"].eq(group) & df["high_prio"] & ~accepted(df))
    # Assign medium priority first preference
    assign_spot(df, lambda group: df["1"].eq(group) & df["med_prio"])
    # Assign medium priority second preference that are not in first preference
    assign_spot(df, lambda group: df["2"].eq(group) & df["med_prio"] & ~accepted(df))
    # Assign medium and high priority second preference that want to join more than 1 class
    assign_spot(df, lambda group: df["2"].eq(group) & (df["med_prio"] | df["high_prio"]) & ~df["only_1"])
    # Assign all low priority first preference
    assign_spot(df, lambda group: df["1"].eq(group) & df["low_prio"])
    # Assign all low priority second preference that are not in first preference
    assign_spot(df, lambda group: df["2"].eq(group) & df["low_prio"] & ~accepted(df))
    # Assign all low priority second preference that want to join more than 1 class
    assign_spot(df, lambda group: df["2"].eq(group) & df["low_prio"] & ~df["only_1"])

    # Create desired outputs
    print_gmail_emails(df)
    df = df.drop("email", axis=1)
    print(df.to_string())
    df.to_csv(OUTPUT_PATH, index=False)
    create_group_excel_file(df)


if __name__ == "__main__":
    main()
