"""Creates output sheets that are uploaded to the members."""

from typing import Final, Literal

import polars as pl
from xlsxwriter import Workbook

from salsaraffle.column import Col
from salsaraffle.settings import (
    GROUP_INFO,
    GROUPS_FILE,
    MAX_PER_GROUP,
    NEW_ATTENDANCE_FILE,
    RAW_GROUPS_FILE,
)


def create_group_excel_file(df: pl.DataFrame) -> None:
    """Create an excel file with all the final group divisions."""
    label_to_group = {label: group for group, label, _ in GROUP_INFO}

    with Workbook(GROUPS_FILE) as workbook:
        for _, group_label, _ in GROUP_INFO:
            leaders = (
                df.filter(pl.col(group_label + "L").is_not_null())
                .sort(group_label + "L")
                .select(pl.col(Col.NAME).alias("Leader Name"))
            )
            followers = (
                df.filter(pl.col(group_label + "F").is_not_null())
                .sort(group_label + "F")
                .select(pl.col(Col.NAME).alias("Follower Name"))
            )
            pl.concat((leaders, followers), how="horizontal").write_excel(
                workbook=workbook,
                worksheet=label_to_group[group_label],
                autofit=True,
                header_format={"bold": True},
            )


ATTENDANCE_WEEKS: Final = {
    "Week 1": "week1",
    "Week 2": "week2",
    "Week 3": "week3",
    "Week 4": "week4",
}


def create_attendance_sheet(
    df: pl.DataFrame,
    workbook: Workbook,
    group_label: str,
    role: Literal["Leader", "Follower"],
) -> None:
    """Create one sheet in the attendance workbook."""
    role_to_label = {"Leader": "L", "Follower": "F"}
    label_to_group = {label: group for group, label, _ in GROUP_INFO}

    (
        df.filter(pl.col(group_label + role_to_label[role]).is_not_null())
        .sort(group_label + role_to_label[role])
        .select(
            pl.col(Col.NAME).alias("Name"),
            pl.col(Col.HANDLE).alias("Handle"),
            pl.col(Col.MEMBER).alias("Member"),
            pl.col(Col.PAID).alias("Paid"),
        )
        .with_columns(
            pl.lit(value=None).alias(keys) for keys in ATTENDANCE_WEEKS
        )
        .write_excel(
            workbook=workbook,
            worksheet=label_to_group[group_label] + " " + role,
            autofit=True,
            header_format={"bold": True},
        )
    )


def add_accepted_status(groups: pl.DataFrame) -> pl.DataFrame:
    """Add checks whether someone is accepted or not."""
    groups = groups.with_columns(pl.lit(value=False).alias(Col.ACCEPTED))

    for _, level, _ in GROUP_INFO:
        leader_col_name = level + "L"
        follow_col_name = level + "F"

        leader_max = groups.get_column(leader_col_name).max()
        follow_max = groups.get_column(follow_col_name).max()

        if leader_max is None or follow_max is None:
            msg = f"No leaders or followers found in {level}"
            raise ValueError(msg)

        role_max = min(leader_max, follow_max, MAX_PER_GROUP)
        leader_col = pl.col(leader_col_name)
        follow_col = pl.col(follow_col_name)
        groups = groups.with_columns(
            pl.col(Col.ACCEPTED)
            | (leader_col.is_not_null() & (leader_col <= role_max))
            | (follow_col.is_not_null() & (follow_col <= role_max))
        )

    return groups


def compile_results(groups_lazy: pl.LazyFrame) -> None:
    """Generate the output files of the program."""
    groups = groups_lazy.collect()
    groups = add_accepted_status(groups)
    accepted = groups.filter(pl.col(Col.ACCEPTED))
    rejected = groups.filter(~pl.col(Col.ACCEPTED))

    print("---")
    print("Number of applicants:")
    print("Total:   ", len(groups))
    print("Rejected:", len(rejected))
    rejected_no_low_prio = rejected.filter(~pl.col(Col.LOW_PRIO))
    print("Rejected and not low priority:\n", rejected_no_low_prio)
    print("---")
    accepted_emails = accepted.get_column(Col.EMAIL).unique().to_list()
    print(f"Accepted emails {len(accepted_emails)}:")
    print(*accepted_emails, sep=", ")
    print("---")
    rejected_emails = rejected.get_column(Col.EMAIL).unique().to_list()
    print(f"Rejected emails {len(rejected_emails)}:")
    print(*rejected_emails, sep=", ")
    print("---")

    groups.write_csv(RAW_GROUPS_FILE)
    create_group_excel_file(groups)

    with Workbook(NEW_ATTENDANCE_FILE) as workbook:
        for _, label, _ in GROUP_INFO:
            create_attendance_sheet(groups, workbook, label, "Leader")
            create_attendance_sheet(groups, workbook, label, "Follower")
