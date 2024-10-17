"""All functions pertaining to role/group/label mappings."""

from typing import Final

from salsaraffle.settings import GROUP_INFO, Timeslot

LEADER_LABEL: Final = "L"
FOLLOWER_LABEL: Final = "F"
LEADER: Final = "Leader"
FOLLOWER: Final = "Follower"


def get_role_to_label() -> dict[str, str]:
    """Return a map from a role to a label."""
    return {LEADER: LEADER_LABEL, FOLLOWER: FOLLOWER_LABEL}


def get_group_to_timeslot() -> dict[str, Timeslot]:
    """Return a map from a group to a timeslot."""
    return {group: timeslot for group, _, timeslot in GROUP_INFO}


def get_group_to_label() -> dict[str, str]:
    """Return a map from a group to a label."""
    return {group: label for group, label, _ in GROUP_INFO}


def get_label_to_group() -> dict[str, str]:
    """Return a map from a label to a group."""
    return {v: k for k, v in get_group_to_label().items()}


def get_all_groups() -> list[str]:
    """Return a list of all the groups."""
    leader_groups = [group + LEADER_LABEL for _, group, _ in GROUP_INFO]
    follower_groups = [group + FOLLOWER_LABEL for _, group, _ in GROUP_INFO]
    return leader_groups + follower_groups
