"""Spark-named wrappers around :class:`~pydantable.expressions.Expr` and globals.

``col``/``lit``/``when`` and date helpers mirror PySpark signatures where possible;
static typing requires ``dtype=`` on ``col`` (or use ``df.col()`` on a typed frame).
"""

from __future__ import annotations

from typing import Any

from pydantable.expressions import (
    ColumnRef,
    Expr,
    Literal,
    WhenChain,
)
from pydantable.expressions import (
    coalesce as coalesce_expr,
)
from pydantable.expressions import (
    concat as concat_expr,
)
from pydantable.expressions import (
    dense_rank as dense_rank_expr,
)
from pydantable.expressions import (
    lag as lag_expr,
)
from pydantable.expressions import (
    lead as lead_expr,
)
from pydantable.expressions import (
    rank as rank_expr,
)
from pydantable.expressions import (
    row_number as row_number_expr,
)
from pydantable.expressions import (
    when as when_chain,
)
from pydantable.expressions import (
    window_max as window_max_expr,
)
from pydantable.expressions import (
    window_mean as window_mean_expr,
)
from pydantable.expressions import (
    window_min as window_min_expr,
)
from pydantable.expressions import (
    window_sum as window_sum_expr,
)
from pydantable.rust_engine import _require_rust_core as _rust_core


def lit(value: Any) -> Literal:
    """Typed literal column (Spark `lit`)."""
    return Literal(value=value)


def col(name: str, *, dtype: Any | None = None) -> ColumnRef:
    """
    Column reference by name.

    Unlike PySpark, pydantable requires a static type for ``col`` at build time.
    Pass ``dtype=`` (e.g. ``int``, ``str | None``), or use ``df.col(name)`` /
    ``df[name]`` on a typed :class:`~pydantable.pyspark.DataFrame`.
    """
    if dtype is None:
        raise TypeError(
            "functions.col() requires dtype=... in pydantable (e.g. col('age', "
            "dtype=int) or col('age', dtype=int | None)). "
            "Alternatively use df.col('age') on a typed DataFrame."
        )
    return ColumnRef(name=name, dtype=dtype)


def column(name: str, *, dtype: Any | None = None) -> ColumnRef:
    """Alias of :func:`col`."""
    return col(name, dtype=dtype)


def isnull(column: Expr) -> Expr:
    """Return a boolean column that is true where ``column`` is null."""
    return column.is_null()


def isnotnull(column: Expr) -> Expr:
    """Return a boolean column that is true where ``column`` is not null."""
    return column.is_not_null()


def coalesce(*cols: Expr) -> Expr:
    """First non-null value across columns (same compatible scalar types)."""
    for c in cols:
        if not isinstance(c, Expr):
            raise TypeError("coalesce() arguments must be Expr instances.")
    return coalesce_expr(*cols)


def when(condition: Expr, value: Expr) -> WhenChain:
    """First branch of a ``CASE WHEN`` (chain ``.when(...).otherwise(...)``)."""
    return when_chain(condition, value)


def cast(column: Expr, dtype: Any) -> Expr:
    """Cast column to a base scalar type (``int``, ``float``, ``bool``, ``str``)."""
    return column.cast(dtype)


def between(column: Expr, lower: Any, upper: Any) -> Expr:
    """True where ``lower <= column <= upper`` (null if any operand is null)."""
    return column.between(lower, upper)


def concat(*cols: Expr) -> Expr:
    """Concatenate string expressions (null if any part is null)."""
    return concat_expr(*cols)


def substring(column: Expr, pos: Any, length: Any | None = None) -> Expr:
    """Substring with 1-based ``pos`` (Spark-style); ``length`` optional."""
    return column.substr(pos, length)


def length(column: Expr) -> Expr:
    """Character length of a string column (null for null input)."""
    return column.char_length()


def isin(column: Expr, *values: Any) -> Expr:
    """True where column equals one of the given literals (null in → null out)."""
    return column.isin(*values)


# Date/datetime: thin Spark-named wrappers over Expr.dt_* (Rust lowering).
def year(column: Expr) -> Expr:
    """Calendar year (Spark ``year``); ``date`` or ``datetime`` columns."""
    return column.dt_year()


def month(column: Expr) -> Expr:
    """Calendar month 1-12 (Spark ``month``)."""
    return column.dt_month()


def day(column: Expr) -> Expr:
    """Day of month (Spark ``day`` / ``dayofmonth``)."""
    return column.dt_day()


def hour(column: Expr) -> Expr:
    """Hour 0-23 (Spark ``hour``); ``datetime`` or ``time`` column."""
    return column.dt_hour()


def minute(column: Expr) -> Expr:
    """Minute (Spark ``minute``); ``datetime`` or ``time`` column."""
    return column.dt_minute()


def second(column: Expr) -> Expr:
    """Second (Spark ``second``); ``datetime`` or ``time`` column."""
    return column.dt_second()


def nanosecond(column: Expr) -> Expr:
    """Nanosecond-of-second (Spark ``nanosecond``); ``datetime`` or ``time`` column."""
    return column.dt_nanosecond()


def to_date(column: Expr, format: str | None = None) -> Expr:
    """
    Calendar ``date`` from ``datetime``, or parse ``str`` when ``format`` is set
    (``strftime`` pattern, must match the data).
    """
    if format is None:
        return column.dt_date()
    return column.strptime(format, to_datetime=False)


def unix_timestamp(column: Expr, unit: str = "seconds") -> Expr:
    """Seconds or milliseconds since Unix epoch from ``date`` / ``datetime``."""
    return column.unix_timestamp(unit)


def row_number() -> Any:
    """Spark ``row_number``; use ``.over(Window.partitionBy(...).orderBy(...))``."""
    return row_number_expr()


def rank() -> Any:
    """Spark ``rank`` over a window."""
    return rank_expr()


def dense_rank() -> Any:
    """Spark ``dense_rank`` over a window."""
    return dense_rank_expr()


def window_sum(column: Expr) -> Any:
    """Windowed ``sum`` (not ``group_by``); finish with ``.over(Window...)``."""
    return window_sum_expr(column)


def window_avg(column: Expr) -> Any:
    """Windowed ``avg`` / mean."""
    return window_mean_expr(column)


def window_min(column: Expr) -> Any:
    """Windowed ``min``; finish with ``.over(Window...)``."""
    return window_min_expr(column)


def window_max(column: Expr) -> Any:
    """Windowed ``max``; finish with ``.over(Window...)``."""
    return window_max_expr(column)


def sum(column: Expr) -> Expr:
    """Global ``sum`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError(
            "functions.sum() expects a typed column Expr (use col(..., dtype=...))."
        )
    return Expr(rust_expr=_rust_core().expr_global_sum(column._rust_expr))


def avg(column: Expr) -> Expr:
    """Global ``avg`` / mean for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("functions.avg() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_mean(column._rust_expr))


def mean(column: Expr) -> Expr:
    """Alias of :func:`avg`."""
    return avg(column)


def max(column: Expr) -> Expr:
    """Global ``max`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("functions.max() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_max(column._rust_expr))


def min(column: Expr) -> Expr:
    """Global ``min`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("functions.min() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_min(column._rust_expr))


def count(column: Expr | None = None) -> Expr:
    """Non-null count of *column*, or row count if omitted (``count(*)``)."""
    if column is None:
        return Expr(rust_expr=_rust_core().expr_global_row_count())
    if not isinstance(column, Expr):
        raise TypeError("functions.count() expects a typed column Expr or None.")
    return Expr(rust_expr=_rust_core().expr_global_count(column._rust_expr))


def lag(column: Expr, n: int = 1) -> Any:
    """Windowed ``lag``; finish with ``.over(Window...)``."""
    return lag_expr(column, n)


def lead(column: Expr, n: int = 1) -> Any:
    """Windowed ``lead``; finish with ``.over(Window...)``."""
    return lead_expr(column, n)


__all__ = [
    "avg",
    "between",
    "cast",
    "coalesce",
    "col",
    "column",
    "concat",
    "count",
    "day",
    "dense_rank",
    "hour",
    "isin",
    "isnotnull",
    "isnull",
    "lag",
    "lead",
    "length",
    "lit",
    "max",
    "mean",
    "min",
    "minute",
    "month",
    "nanosecond",
    "rank",
    "row_number",
    "second",
    "substring",
    "sum",
    "to_date",
    "unix_timestamp",
    "when",
    "window_avg",
    "window_max",
    "window_min",
    "window_sum",
    "year",
]
