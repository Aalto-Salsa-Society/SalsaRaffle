from pathlib import Path
from types import TracebackType
from typing import Self

class Workbook:
    def __init__(self: Self, filename: str | Path = ...) -> None: ...
    def __enter__(self: Self) -> Self: ...
    def __exit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
