#!/usr/bin/env python3
"""All code for creating the groups for the ASS dance classes."""

import os
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

# A seed for reproducible but random results
rng = np.random.default_rng(455)

CSV_PATH = "responses.csv"
OUTPUT_PATH = "groups.csv"
COLUMNS_FULL = [
    "Telegram handle",
    "Full name (first and last name)",
    "Email address",
    "First preference",
    "First preference dance role",
    "Second preference",
    "Second preference dance role",
    "first_only",
]
COLUMNS = [
    "handle",
    "name",
    "email",
    "first_preference",
    "first_preference_role",
    "second_preference",
    "second_preference_role",
    "only_first_preference",
]
GROUPS_MAP = {
    "Salsa Level 1 M (Monday)": "S1M",
    "Salsa Level 1 T (Tuesday)": "S1T",
    "Salsa Level 2": "S2",
    "Salsa Level 3": "S3",
    "Bachata Level 1": "B1",
    "Bachata Level 2": "B2",
}

ATTENDANCE_COLUMNS_FULL = ["Handle", "Week 1", "Week 2", "Week 3", "Week 4"]
ATTENDANCE_COLUMNS = ["handle", "week1", "week2", "week3", "week4"]
ATTENDANCE_WEEKS = ATTENDANCE_COLUMNS[1:]

MAX_PER_GROUP = 15


def get_low_prio(handles: pd.Series, attendance_path: str = "attendance.csv") -> pd.Series:
    """Return a boolean series that correspond to the handles that are low priority."""
    attendance_df = pd.read_csv(attendance_path)[ATTENDANCE_COLUMNS_FULL]
    attendance_df.columns = ATTENDANCE_COLUMNS

    # Mark disruptions for no-shows
    attendance_df["disruption"] = attendance_df[ATTENDANCE_WEEKS].eq("No show").any(axis=1)
    # Mark disruptions for giving notice twice
    attendance_df["disruption"] |= attendance_df[ATTENDANCE_WEEKS].eq("Gave notice").sum(axis=1).ge(2)

    return handles.isin(attendance_df[attendance_df["disruption"]].handle.str.lower())


def get_high_prio(handles: pd.Series) -> pd.Series:
    """Return a boolean series that correspond to the handles that are high priority."""
    return handles.isin(pd.read_csv("high_prio.csv").handle.str.lower())


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
    df = pd.read_csv(CSV_PATH)[COLUMNS_FULL]
    df.columns = COLUMNS

    # Create first and second preference columns
    # For example:
    # Salsa Level 1 M (Monday), Follower -> S1MF
    # Salsa Level 2, Leader -> S2L
    df["1"] = df["first_preference"].map(GROUPS_MAP) + df["first_preference_role"].str.get(0)
    df["2"] = df["second_preference"].map(GROUPS_MAP) + df["second_preference_role"].str.get(0)

    # Handles are case insensitive
    df["handle"] = df["handle"].str.lower()

    df = df[df["1"].notna()]
    df["2"] = df["2"].replace({np.nan: None})
    df["only_1"] = df["only_first_preference"].isna()

    # Load attendance from attendance sheets
    attendance_files = (f for f in os.listdir() if Path(f).is_file() and f.startswith("attendance_"))
    df["low_prio"] = False
    for attendance_file in attendance_files:
        df["low_prio"] |= get_low_prio(df["handle"], attendance_file)

    # Give high priority only to those who are not low priority
    df["high_prio"] = get_high_prio(df["handle"]) & ~df["low_prio"]
    # Medium priority is everyone else
    df["med_prio"] = ~df["high_prio"] & ~df["low_prio"]

    # Create columns for each group
    groups = df["1"].unique().tolist()
    for group in groups:
        df[group] = None

    # Randomize order with a different seed
    column_order = ["handle", "name", "email", "high_prio", "med_prio", "low_prio", "1", "2", "only_1", *groups]
    return df.sample(frac=1, random_state=rng).reset_index(drop=True)[column_order]


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
