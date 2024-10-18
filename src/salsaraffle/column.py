"""Column options of the registration form after processing."""

import enum

from salsaraffle.settings import GROUP_INFO


class Col(enum.StrEnum):
    """All columns used or created."""

    HANDLE = "handle"
    NAME = "name"
    EMAIL = "email"
    P1 = "first_preference"  # combination of class and role
    P1_CLASS = "first_preference_class"
    P1_ROLE = "first_preference_role"
    HAS_P2 = "has_second_preference"
    P2 = "second_preference"  # combination of class and role
    P2_CLASS = "second_preference_class"
    P2_ROLE = "second_preference_role"
    ONLY_1 = "only_1_preference"
    TIMESLOT_1 = "timeslot_1"
    TIMESLOT_2 = "timeslot_2"
    HIGH_PRIO = "high_priority"
    MED_PRIO = "medium_priority"
    LOW_PRIO = "low_priority"

    # Membership columns
    MEMBER = "member"
    PAID = "paid"
    APPROVED = "approved"


def get_all_groups() -> list[str]:
    """Return a list of all the groups."""
    leader_groups = [group + "L" for _, group, _ in GROUP_INFO]
    follower_groups = [group + "F" for _, group, _ in GROUP_INFO]
    return leader_groups + follower_groups
