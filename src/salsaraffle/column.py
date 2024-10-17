"""Column options of the registration form after processing."""

import enum


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
