"""Useful polars expressions."""

from typing import Final

import polars as pl

from salsaraffle.column import Col, get_all_groups
from salsaraffle.settings import MAX_PER_GROUP

ACCEPTED: Final = pl.any_horizontal(pl.col(get_all_groups()).lt(MAX_PER_GROUP)).fill_null(
    value=False
)
REJECTED: Final = pl.all_horizontal(pl.col(get_all_groups()).ge(MAX_PER_GROUP)).fill_null(
    value=True
)
LOW_PRIO: Final = pl.col(Col.LOW_PRIO)
