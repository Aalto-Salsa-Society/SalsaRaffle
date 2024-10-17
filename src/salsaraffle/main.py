"""All code for creating the groups for the ASS dance classes."""

import logging
from typing import TYPE_CHECKING

import polars as pl

from salsaraffle.assign import assign
from salsaraffle.column import Col
from salsaraffle.expressions import REJECTED
from salsaraffle.registration import get_class_registrations
from salsaraffle.results import compile_results
from salsaraffle.settings import INPUT_DIR, OUTPUT_DIR

if TYPE_CHECKING:
    from collections.abc import Callable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def main() -> None:
    """Run the main program."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    groups = get_class_registrations()

    assignments: list[Callable[[str], pl.Expr]] = [
        # High priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.HIGH_PRIO),
        # High priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.HIGH_PRIO) & REJECTED,
        # Medium priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.MED_PRIO),
        # Medium priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.MED_PRIO) & REJECTED,
        # Med and high priority (not low) second preference that want to join more than 1 class
        lambda group: pl.col(Col.P2).eq(group) & ~pl.col(Col.LOW_PRIO) & ~pl.col(Col.ONLY_1),
        # All low priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.LOW_PRIO),
        # All low priority second preference that are not in first preference
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.LOW_PRIO) & REJECTED,
        # All low priority second preference that want to join more than 1 class
        lambda group: pl.col(Col.P2).eq(group) & pl.col(Col.LOW_PRIO) & ~pl.col(Col.ONLY_1),
    ]
    for assign_rule in assignments:
        groups = assign(groups, assign_rule)

    compile_results(groups)


if __name__ == "__main__":
    main()
