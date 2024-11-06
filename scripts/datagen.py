"""Generate input to run salsaraffle with."""

import random
from pathlib import Path

import polars as pl
from faker import Faker
from xlsxwriter import Workbook

from salsaraffle.column import Col
from salsaraffle.registration import REGISTRATION_COLUMNS
from salsaraffle.settings import GROUP_INFO

OUTPUT_DIR = Path("data") / "generated_input"
MEMBERS_PATH = OUTPUT_DIR / "Members.xlsx"
RESPONSES_PATH = OUTPUT_DIR / "responses.xlsx"

NUM_MEMBERS = 300
NUM_APPROVED = 270
NUM_PAID = NUM_APPROVED - 10
NUM_REGISTRATIONS = 140

CLASSES = [info[0] for info in GROUP_INFO]
# first choice class preference distribution
CLASS_DIST = [
    0.3,
    0.15,
    0.15,
    0.1,
    0.2,
    0.1,
]

ROLES = ["Leader", "Follower"]
ROLE_DIST = [0.6, 0.4]  # follower vs leader ratio

SECOND_PREF_OPTIONS = ["Yes", "No"]
# Has second pref vs doesn't have second pref ratio
SECOND_PREF_DIST = [0.3, 0.7]


def main() -> None:
    """Generate data and write data to files."""
    responses = gen_responses()
    write_responses_file(responses)

    members = gen_members(responses)
    write_members_file(members)


def gen_responses() -> pl.DataFrame:
    """Generate fake responses."""
    fake = Faker()

    names = [fake.name() for _ in range(NUM_MEMBERS)]
    emails = [".".join(name.split()).lower() + "@email.com" for name in names]
    handles = ["@" + name.replace(" ", "").lower() for name in names]
    class_pref = random.choices(CLASSES, weights=CLASS_DIST, k=NUM_MEMBERS)
    role_pref = random.choices(ROLES, weights=ROLE_DIST, k=NUM_MEMBERS)
    has_second_pref = random.choices(
        SECOND_PREF_OPTIONS, weights=SECOND_PREF_DIST, k=NUM_MEMBERS
    )

    df = pl.DataFrame(
        {
            Col.HANDLE: handles,
            Col.NAME: names,
            Col.EMAIL: emails,
            Col.P1_CLASS: class_pref,
            Col.P1_ROLE: role_pref,
            Col.HAS_P2: has_second_pref,
        }
    )

    df = df.with_columns(
        (
            pl.when(pl.col(Col.HAS_P2) == "Yes")
            .then(pl.Series(random.choices(CLASSES, k=df.height)))
            .otherwise(pl.lit(None))
            .alias(Col.P2_CLASS)
        ),
        (
            pl.when(pl.col(Col.HAS_P2) == "Yes")
            .then(pl.Series(random.choices(ROLES, k=df.height)))
            .otherwise(pl.lit(None))
            .alias(Col.P2_ROLE)
        ),
        (
            pl.when(pl.col(Col.HAS_P2) == "Yes")
            .then(pl.Series(random.choices([True, False], k=df.height)))
            .otherwise(pl.lit(None))
            .alias(Col.ONLY_1)
        ),
    )

    inv_registration_columns = {v: k for k, v in REGISTRATION_COLUMNS.items()}
    return df.rename(inv_registration_columns)


def write_responses_file(df: pl.DataFrame) -> None:
    """Write response data to excel file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with Workbook(RESPONSES_PATH) as wb:
        df.sample(NUM_REGISTRATIONS, shuffle=True).write_excel(workbook=wb)


def gen_members(responses: pl.DataFrame) -> pl.DataFrame:
    """Generate fake members."""
    handle_map = next(
        {k: v} for k, v in REGISTRATION_COLUMNS.items() if v == Col.HANDLE
    )
    approved = [True] * NUM_APPROVED + [False] * (NUM_MEMBERS - NUM_APPROVED)
    paid = [True] * NUM_PAID + [False] * (NUM_MEMBERS - NUM_PAID)
    return (
        responses.select(handle_map)
        .rename(handle_map)
        .sample(fraction=1, shuffle=True)
        .with_columns(
            pl.Series(approved).alias(Col.APPROVED),
            pl.Series(paid).alias(Col.PAID),
        )
    )


def write_members_file(df: pl.DataFrame) -> None:
    """Write member data to excel file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with Workbook(MEMBERS_PATH) as wb:
        df.sample(NUM_MEMBERS, shuffle=True).write_excel(workbook=wb)


if __name__ == "__main__":
    main()
