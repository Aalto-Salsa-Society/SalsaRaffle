"""Settings that could be changed between runs of the script."""

import enum
from pathlib import Path
from typing import Final


class Timeslot(enum.IntEnum):
    """Represents a timeslot in which a class can happen."""

    MONDAY = enum.auto()
    TUESDAY_EARLY = enum.auto()
    TUESDAY_LATE = enum.auto()
    THURSDAY = enum.auto()


GROUP_INFO: Final[list[tuple[str, str, Timeslot]]] = [
    ("Salsa Level 1", "S1", Timeslot.THURSDAY),
    ("Salsa Level 2", "S2", Timeslot.MONDAY),
    ("Salsa Level 3", "S3", Timeslot.MONDAY),
    ("Salsa Level 4", "S4", Timeslot.THURSDAY),
    ("Bachata Level 1", "B1", Timeslot.TUESDAY_EARLY),
    ("Bachata Level 2", "B2", Timeslot.TUESDAY_LATE),
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
