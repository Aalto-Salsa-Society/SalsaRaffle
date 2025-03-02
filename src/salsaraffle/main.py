"""All code for creating the groups for the ASS dance classes."""

import logging
from collections.abc import Callable
from typing import Final

import polars as pl

from salsaraffle.column import Col, get_all_groups
from salsaraffle.registration import get_class_registrations
from salsaraffle.results import compile_results
from salsaraffle.settings import INPUT_DIR, MAX_PER_GROUP, OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def assign(
    lf: pl.LazyFrame,
    assign_rule: Callable[[str], pl.Expr],
) -> pl.LazyFrame:
    """Assign a spot in all groups according to the assign_rule."""
    for group in get_all_groups():
        assignees = assign_rule(group) & pl.col(group).is_null()
        starting_point = (
            pl.when(pl.col(group).max().is_null())
            .then(0)
            .otherwise(pl.col(group).max())
        )
        lf = lf.with_columns(
            pl.when(assignees)
            .then(assignees.cum_sum() + starting_point)
            .otherwise(pl.col(group))
            .alias(group)
        )

    return lf


REJECTED: Final = pl.all_horizontal(
    pl.col(get_all_groups()).ge(MAX_PER_GROUP)
).fill_null(value=True)


def main() -> None:
    """Run the main program."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    groups = get_class_registrations()

    assignments: list[Callable[[str], pl.Expr]] = [
        # High priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.HIGH_PRIO),
        # High priority second preference that are not in first preference
        lambda group: (
            pl.col(Col.P2).eq(group) & pl.col(Col.HIGH_PRIO) & REJECTED
        ),
        # Medium priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.MED_PRIO),
        # Medium priority second preference that are not in first preference
        lambda group: (
            pl.col(Col.P2).eq(group) & pl.col(Col.MED_PRIO) & REJECTED
        ),
        # Med and high priority second preference that want to join
        # more than 1 class
        lambda group: (
            pl.col(Col.P2).eq(group)
            & (pl.col(Col.MED_PRIO) | pl.col(Col.HIGH_PRIO))
            & ~pl.col(Col.ONLY_1)
        ),
        # All low priority first preference
        lambda group: pl.col(Col.P1).eq(group) & pl.col(Col.LOW_PRIO),
        # All low priority second preference that are not in first preference
        lambda group: (
            pl.col(Col.P2).eq(group) & pl.col(Col.LOW_PRIO) & REJECTED
        ),
        # All low priority second preference that want to join more than 1 class
        lambda group: (
            pl.col(Col.P2).eq(group)
            & pl.col(Col.LOW_PRIO)
            & ~pl.col(Col.ONLY_1)
        ),
    ]
    for assign_rule in assignments:
        groups = assign(groups, assign_rule)

    compile_results(groups)


if __name__ == "__main__":
    main()
