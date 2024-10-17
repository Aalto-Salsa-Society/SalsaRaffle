"""Assign members a spot in a group."""

from collections.abc import Callable

import polars as pl

from salsaraffle.roles import get_all_groups


def assign(lf: pl.LazyFrame, assign_rule: Callable[[str], pl.Expr]) -> pl.LazyFrame:
    """Assign a spot in all groups according to the assign_rule."""
    for group in get_all_groups():
        assignees = assign_rule(group) & pl.col(group).is_null()
        starting_point = (
            pl.when(pl.col(group).max().is_null()).then(0).otherwise(pl.col(group).max())
        )
        lf = lf.with_columns(
            pl.when(assignees)
            .then(assignees.cum_sum() + starting_point)
            .otherwise(pl.col(group))
            .alias(group)
        )

    return lf
