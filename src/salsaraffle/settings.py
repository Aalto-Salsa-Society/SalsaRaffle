"""Settings that could be changed between runs of the script."""

import enum
from pathlib import Path
from typing import Final


class Timeslot(enum.IntEnum):
    """
    Represents a timeslot in which a class can happen.

    Classes that happen at the same time need to be in the same timeslot. In
    that case the program will take overlapping classes into account.

    For example, Salsa level 1 from 18:00-19:30 and Salsa level 3 from
    19:00-20:30 both on Tuesdays should be on the same slot, since you cannot
    take both.
    """

    MONDAY_EARLY = enum.auto()
    MONDAY_LATE = enum.auto()
    TUESDAY_EARLY = enum.auto()
    TUESDAY_LATE = enum.auto()
    WEDNESDAY_EARLY = enum.auto()
    WEDNESDAY_LATE = enum.auto()
    THURSDAY_EARLY = enum.auto()
    THURSDAY_LATE = enum.auto()


GROUP_INFO: Final[list[tuple[str, str, Timeslot]]] = [
    ("Salsa Level 1", "S1", Timeslot.TUESDAY_EARLY),
    ("Salsa Level 2", "S2", Timeslot.WEDNESDAY_EARLY),
    ("Salsa Level 3", "S3", Timeslot.TUESDAY_EARLY),
    ("Salsa Level 4", "S4", Timeslot.THURSDAY_EARLY),
    ("Bachata Level 1", "B1", Timeslot.MONDAY_EARLY),
    ("Bachata Level 2", "B2", Timeslot.MONDAY_LATE),
]

# A seed for reproducible but random results
RANDOM_SEED: Final = 455
MAX_PER_GROUP: Final = 15

# Required files
INPUT_DIR: Final = Path("data") / "input"
OLD_GROUPS_FILE: Final = INPUT_DIR / "groups.csv"
OLD_ATTENDANCE_FILE: Final = INPUT_DIR / "attendance.xlsx"
MEMBERS_FILE: Final = INPUT_DIR / "Members.xlsx"
RESPONSE_FILE: Final = INPUT_DIR / "responses.xlsx"

# Created files
OUTPUT_DIR: Final = Path("data") / "output"
GROUPS_FILE: Final = OUTPUT_DIR / "groups.xlsx"
NEW_ATTENDANCE_FILE: Final = OUTPUT_DIR / "attendance.xlsx"
RAW_GROUPS_FILE: Final = OUTPUT_DIR / "groups.csv"
