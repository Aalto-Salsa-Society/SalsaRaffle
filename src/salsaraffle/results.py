"""Creates output sheets that are uploaded to the members."""

from typing import Final

import polars as pl
from xlsxwriter import Workbook

from salsaraffle.column import Col
from salsaraffle.expressions import ACCEPTED, LOW_PRIO, REJECTED
from salsaraffle.priority import ATTENDANCE_WEEKS
from salsaraffle.settings import GROUP_INFO, GROUPS_FILE, NEW_ATTENDANCE_FILE, RAW_GROUPS_FILE

LEADER_LABEL: Final = "L"
FOLLOWER_LABEL: Final = "F"
LEADER: Final = "Leader"
FOLLOWER: Final = "Follower"


def create_group_excel_file(df: pl.DataFrame) -> None:
    """Create an excel file with all the final group divisions."""
    label_to_group = {label: group for group, label, _ in GROUP_INFO}

    with Workbook(GROUPS_FILE) as workbook:
        for _, group_label, _ in GROUP_INFO:
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
                worksheet=label_to_group[group_label],
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
    role_to_label = {LEADER: LEADER_LABEL, FOLLOWER: FOLLOWER_LABEL}
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
        .with_columns(pl.lit(value=None).alias(keys) for keys in ATTENDANCE_WEEKS)
        .write_excel(
            workbook=workbook,
            worksheet=label_to_group[group_label] + " " + role,
            autofit=True,
            header_format={"bold": True},
        )
    )


def compile_results(groups_lazy: pl.LazyFrame) -> None:
    """Generate the output files of the program."""
    groups = groups_lazy.collect()

    print("---")
    print("Number of applicants:")
    print("Total:   ", len(groups))
    print("Accepted:", len(groups.filter(ACCEPTED)))
    print("Rejected:", len(groups.filter(REJECTED)))
    next_high_prio = groups.filter(REJECTED & ~LOW_PRIO).select([Col.NAME, Col.HANDLE])
    print("Rejected and not low priority:", len(next_high_prio))
    print(next_high_prio)
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
        for _, label, _ in GROUP_INFO:
            create_attendance_sheet(groups, workbook, label, LEADER)
            create_attendance_sheet(groups, workbook, label, FOLLOWER)
