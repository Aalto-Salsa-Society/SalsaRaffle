#!/usr/bin/env python3
"""All code for creating the groups for the ASS dance classes."""

from typing import Callable

import polars as pl

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
GROUPS_MAP = {
    "Salsa Level 1": "S1",
    "Salsa Level 2 M (Monday)": "S2M",
    "Salsa Level 2 T (Tuesday)": "S2T",
    "Salsa Level 3": "S3",
    "Bachata Level 1": "B1",
    "Bachata Level 2": "B2",
}
GROUPS = [group + "L" for group in GROUPS_MAP.values()] + [group + "F" for group in GROUPS_MAP.values()]

ATTENDANCE_COLUMNS = {"Handle": "handle", "Week 1": "week1", "Week 2": "week2", "Week 3": "week3", "Week 4": "week4"}
ATTENDANCE_WEEKS = list(ATTENDANCE_COLUMNS.values())[1:]

MAX_PER_GROUP = 15


def initial_data_setup() -> pl.LazyFrame:
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
    # A list of people who were left out last cycle (manually created)
    high_prio = pl.scan_csv("high_prio.csv").with_columns(pl.col("handle").str.to_lowercase()).collect()

    # A "No show" or 2 "Gave notice" is considered a disruption
    low_prio = (
        pl.scan_csv("attendance_*.csv")
        .select(ATTENDANCE_COLUMNS)
        .rename(ATTENDANCE_COLUMNS)
        .drop_nulls("handle")
        .with_columns(
            (pl.col("handle").str.to_lowercase()),
            (pl.any_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("No show")).alias("no_show")),
            (pl.sum_horizontal(pl.col(ATTENDANCE_WEEKS).eq_missing("Gave notice")).ge(2).alias("gave_notice")),
        )
        .filter(pl.col("no_show") | pl.col("gave_notice"))
        .select("handle")
        .collect()
    )

    registrations = (
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
            (pl.col("handle").is_in(high_prio["handle"]).fill_null(value=False).alias("high_prio")),
            (pl.col("handle").is_in(low_prio["handle"]).fill_null(value=False).alias("low_prio")),
        )
        .with_columns(
            # Remove high priority if they already have low priority
            (pl.col("high_prio") & ~pl.col("low_prio")),
            (~(pl.col("high_prio") | pl.col("low_prio")).alias("med_prio")),
        )
        .with_columns(pl.lit(value=None).alias(group) for group in GROUPS)
        .collect()
    )

    return registrations.sample(fraction=1, shuffle=True, seed=RANDOM_SEED).lazy()


def assign_spot(lf: pl.LazyFrame, assign_rule: Callable[[str], pl.Expr]) -> pl.LazyFrame:
    """Assign a spot in all groups according to the assign_rule."""
    for group in GROUPS:
        assignees = assign_rule(group) & pl.col(group).is_null()
        starting_point = pl.when(pl.col(group).max().is_null()).then(0).otherwise(pl.col(group).max())
        lf = lf.with_columns(pl.when(assignees).then(assignees.cumsum() + starting_point).otherwise(pl.col(group)).alias(group))

    return lf


def print_gmail_emails(df: pl.DataFrame) -> None:
    """Print the emails of the people that have been accepted."""
    emails = df.filter(pl.any_horizontal(pl.col(GROUPS).le(MAX_PER_GROUP))).unique().collect()["email"].to_list()
    print("Accepted emails:")
    print(*emails, sep=", ")


# def create_group_excel_file(df: pl.DataFrame) -> None:
#     """Create an excel file with all the final group divisions."""
#     # We need the name for each of the short version labels
#     group_labels = {v: k for k, v in GROUPS_MAP.items()}
#
#     with pd.ExcelWriter("groups.xlsx") as writer:
#         for group in GROUPS_MAP.values():
#             leaders = df[df[group + "L"].notna()].sort_values(group + "L").reset_index()["name"].rename("Leader Name")
#             followers = df[df[group + "F"].notna()].sort_values(group + "F").reset_index()["name"].rename("Follower Name")
#
#             group_division = pd.concat([leaders, followers], axis=1)
#             group_division.index = pd.RangeIndex(start=1, stop=len(group_division) + 1)
#             group_division.to_excel(writer, sheet_name=group_labels[group])


def main() -> None:
    """Run the main program."""
    lf = initial_data_setup()

    # A person is accepted if they got a number less than 15
    accepted = pl.any_horizontal(pl.col(GROUPS).le(MAX_PER_GROUP))

    # Assign high priority first preference
    lf = assign_spot(lf, lambda group: pl.col("1").eq(group) & pl.col("high_prio"))
    # Assign high priority second preference that are not in first preference
    lf = assign_spot(lf, lambda group: pl.col("2").eq(group) & pl.col("high_prio") & ~accepted)
    # Assign medium priority first preference
    lf = assign_spot(lf, lambda group: pl.col("1").eq(group) & pl.col("med_prio"))
    # Assign medium priority second preference that are not in first preference
    lf = assign_spot(lf, lambda group: pl.col("2").eq(group) & pl.col("med_prio") & ~accepted)
    # Assign medium and high priority second preference that want to join more than 1 class
    lf = assign_spot(lf, lambda group: pl.col("2").eq(group) & pl.col("med_prio") | pl.col("high_prio") & ~pl.col("only_1"))
    # Assign all low priority first preference
    lf = assign_spot(lf, lambda group: pl.col("1").eq(group) & pl.col("low_prio"))
    # Assign all low priority second preference that are not in first preference
    lf = assign_spot(lf, lambda group: pl.col("2").eq(group) & pl.col("low_prio") & ~accepted)
    # Assign all low priority second preference that want to join more than 1 class
    lf = assign_spot(lf, lambda group: pl.col("2").eq(group) & pl.col("low_prio") & ~pl.col("only_1"))

    # Create desired outputs
    print_gmail_emails(lf)
    lf = lf.drop("email")
    lf = lf.collect()
    print(str(lf))
    lf.write_csv(OUTPUT_PATH)
    # create_group_excel_file(lf)


if __name__ == "__main__":
    main()
