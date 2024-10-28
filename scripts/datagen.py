import os
import random

import polars as pl
from faker import Faker
from xlsxwriter import Workbook

from salsaraffle.registration import REGISTRATION_COLUMNS

OUTPUT_DIR = "data/fakeinput/"
MEMBER_FILENAME = "members.xlsx"
RESPONSES_FILENAME = "responses.xlsx"

NUM_MEMBERS = 300
NUM_APPROVED = 270
NUM_PAID = 280
NUM_REG = 140


CLASSES = [
    "Salsa Level 1",
    "Salsa Level 2",
    "Salsa Level 3",
    "Salsa Level 4",
    "Bachata Level 1",
    "Bachata Level 2",
]
CLASS_DIST = [
    0.3,
    0.15,
    0.15,
    0.1,
    0.2,
    0.1,
]  # first choice class preference distribution

ROLES = ["Leader", "Follower"]
ROLE_DIST = [0.6, 0.4]  # follower vs leader ratio

SECOND_PREF_OPTIONS = ["Yes", "No"]
SECOND_PREF_DIST = [
    0.3,
    0.7,
]  # Has second pref vs doesn't have second pref ratio


def main() -> None:
    df = gen_data()
    write_members_file(df)
    write_responses_file(df)


def gen_data() -> pl.DataFrame:
    """Return a df of data needed to generate member and responses excel files."""
    fake = Faker()

    names = [
        fake.name() for x in range(NUM_MEMBERS)
    ]  # uses Faker to generate fake names
    emails = [
        name.split(" ")[0] + "." + name.split(" ")[1] + "@email.com"
        for name in names
    ]
    handles = ["@" + name.replace(" ", "").lower() for name in names]
    approved = [True for x in range(NUM_APPROVED)] + [
        False for x in range(NUM_MEMBERS - NUM_APPROVED)
    ]
    paid = [True for x in range(NUM_PAID)] + [
        False for x in range(NUM_MEMBERS - NUM_PAID)
    ]
    class_pref = random.choices(CLASSES, weights=CLASS_DIST, k=NUM_MEMBERS)
    role_pref = random.choices(ROLES, weights=ROLE_DIST, k=NUM_MEMBERS)
    has_second_pref = random.choices(
        SECOND_PREF_OPTIONS, weights=SECOND_PREF_DIST, k=NUM_MEMBERS
    )

    col_names = list(REGISTRATION_COLUMNS.keys())

    df = pl.DataFrame(
        {
            col_names[0]: handles,
            col_names[1]: names,
            col_names[2]: emails,
            col_names[3]: class_pref,
            col_names[4]: role_pref,
            col_names[5]: has_second_pref,
            "approved": approved,
            "paid": paid,
        }
    )

    return gen_second_pref_data(df, col_names)


def gen_second_pref_data(df: pl.DataFrame, col_names: list) -> pl.DataFrame:
    """Generate data for students that selected a second preference."""
    second_class, second_role, both_pref = [], [], []

    for row in df.iter_rows():  # there should be a way to replace this for loop with polars functions but idk how
        if row[5] == "Yes":
            classes = CLASSES.copy()
            classes.remove(row[3])
            second_class.append(
                str(random.choice(classes))
            )  # randomnly pick a class that isn't the first class, even distribution
            second_role.append(
                str(random.choice(ROLES))
            )  # randomnly pick a role, even distribution
            both_pref.append(
                random.choice([True, False])
            )  # randomnly pick true or false, even distribution
        else:
            second_class.append(None)
            second_role.append(None)
            both_pref.append(None)

    return df.select(
        [
            pl.all(),
            pl.lit(pl.Series(second_class)).alias(col_names[6]),
            pl.lit(pl.Series(second_role)).alias(col_names[7]),
            pl.lit(pl.Series(both_pref)).alias(col_names[8]),
        ]
    )


def write_members_file(df: pl.DataFrame) -> None:
    """Write member data to excel file."""
    if not os.path.isdir(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    with Workbook(OUTPUT_DIR + MEMBER_FILENAME) as wb:
        df.select(["Telegram handle", "approved", "paid"]).rename(
            {"Telegram handle": "handle"}
        ).write_excel(workbook=wb)


def write_responses_file(df: pl.DataFrame) -> None:
    """Write response data to excel file."""
    if not os.path.isdir(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    with Workbook(OUTPUT_DIR + RESPONSES_FILENAME) as wb:
        df.sample(NUM_REG).select(
            [pl.all().exclude(["approved", "paid"])]
        ).write_excel(workbook=wb)


if __name__ == "__main__":
    main()
