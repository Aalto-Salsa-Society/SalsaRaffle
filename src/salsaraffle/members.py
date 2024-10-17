import logging

import polars as pl

from salsaraffle.column import Col
from salsaraffle.settings import MEMBERS_FILE

logger = logging.getLogger(__name__)


def get_members(condition: pl.Expr | str = Col.APPROVED) -> pl.Series:
    """Return a list of ASS members."""
    if not MEMBERS_FILE.exists():
        logger.warning("No members list found")
        return pl.Series(dtype=pl.Utf8)

    return (
        pl.read_excel(MEMBERS_FILE)
        .filter(condition)
        .with_columns(pl.col(Col.HANDLE).str.to_lowercase())
        .get_column(Col.HANDLE)
    )
