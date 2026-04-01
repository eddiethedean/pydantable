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


class _GroupedAggSpec:
    __slots__ = ("_col", "_op")

    def __init__(self, *, op: str, col: str) -> None:
        self._op = str(op)
        self._col = str(col)

    def alias(self, name: str) -> _GroupedAggSpecAliased:
        if not isinstance(name, str) or not name:
            raise TypeError("alias(name) expects a non-empty string.")
        return _GroupedAggSpecAliased(out_name=name, spec=self)


class _GroupedAggSpecAliased:
    __slots__ = ("_out_name", "_spec")

    def __init__(self, *, out_name: str, spec: _GroupedAggSpec) -> None:
        self._out_name = out_name
        self._spec = spec


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


def dayofmonth(column: Expr) -> Expr:
    """Spark ``dayofmonth``; alias of :func:`day`."""
    return column.dt_day()


def dayofweek(column: Expr) -> Expr:
    """ISO weekday (Monday = 1 … Sunday = 7); ``date`` or ``datetime``."""
    return column.dt_weekday()


def quarter(column: Expr) -> Expr:
    """Calendar quarter 1-4."""
    return column.dt_quarter()


def weekofyear(column: Expr) -> Expr:
    """ISO 8601 week number 1-53 (core :meth:`Expr.dt_week`)."""
    return column.dt_week()


def lower(column: Expr) -> Expr:
    """Lowercase string column (Spark ``lower``)."""
    return column.lower()


def upper(column: Expr) -> Expr:
    """Uppercase string column (Spark ``upper``)."""
    return column.upper()


def trim(column: Expr) -> Expr:
    """Trim leading/trailing whitespace (Spark ``trim``; core ``Expr.strip``)."""
    return column.strip()


def str_replace(
    column: Expr, pattern: str, replacement: str, *, literal: bool = True
) -> Expr:
    """Replace matches; default substring replace; ``literal=False`` is Rust regex."""
    if not isinstance(column, Expr):
        raise TypeError("functions.str_replace() expects a typed column Expr.")
    return column.str_replace(pattern, replacement, literal=literal)


def regexp_replace(column: Expr, pattern: str, replacement: str) -> Expr:
    """Regex replace (Rust ``regex`` dialect, not Python ``re``)."""
    return str_replace(column, pattern, replacement, literal=False)


def starts_with(column: Expr, prefix: str) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.starts_with() expects a typed column Expr.")
    return column.starts_with(prefix)


def ends_with(column: Expr, suffix: str) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.ends_with() expects a typed column Expr.")
    return column.ends_with(suffix)


def contains(column: Expr, substring: str) -> Expr:
    """Literal substring test (core :meth:`Expr.str_contains`)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.contains() expects a typed column Expr.")
    return column.str_contains(substring)


def str_contains_pat(column: Expr, pattern: str, *, literal: bool = False) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.str_contains_pat() expects a typed column Expr.")
    return column.str_contains_pat(pattern, literal=literal)


def split(column: Expr, delimiter: str) -> Expr:
    """Split to ``list[str]`` (core :meth:`Expr.str_split`)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.split() expects a typed column Expr.")
    return column.str_split(delimiter)


def reverse(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.reverse() expects a typed column Expr.")
    return column.str_reverse()


def lpad(column: Expr, length: int, pad: str = " ") -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.lpad() expects a typed column Expr.")
    return column.str_pad_start(length, pad)


def rpad(column: Expr, length: int, pad: str = " ") -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.rpad() expects a typed column Expr.")
    return column.str_pad_end(length, pad)


def lpad_zero(column: Expr, length: int) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.lpad_zero() expects a typed column Expr.")
    return column.str_zfill(length)


def regexp_extract(column: Expr, pattern: str, group_index: int = 1) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.regexp_extract() expects a typed column Expr.")
    return column.str_extract_regex(pattern, group_index)


def rlike(column: Expr, pattern: str) -> Expr:
    """Regex match predicate (Spark ``rlike`` / SQL ``RLIKE``)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.rlike() expects a typed column Expr.")
    return column.str_contains_pat(pattern, literal=False)


def regexp_like(column: Expr, pattern: str) -> Expr:
    """Alias of :func:`rlike`."""
    return rlike(column, pattern)


def regexp_substr(column: Expr, pattern: str, group_index: int = 0) -> Expr:
    """First regex match (group 0) or capture group (1+)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.regexp_substr() expects a typed column Expr.")
    return column.str_extract_regex(pattern, group_index)

def json_path_match(column: Expr, path: str) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.json_path_match() expects a typed column Expr.")
    return column.str_json_path_match(path)


def strip_prefix(column: Expr, prefix: str) -> Expr:
    """Remove literal ``prefix`` from string start (core ``Expr.strip_prefix``)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.strip_prefix() expects a typed column Expr.")
    return column.strip_prefix(prefix)


def strip_suffix(column: Expr, suffix: str) -> Expr:
    """Remove literal ``suffix`` from string end (core ``Expr.strip_suffix``)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.strip_suffix() expects a typed column Expr.")
    return column.strip_suffix(suffix)


def strip_chars(column: Expr, chars: str) -> Expr:
    """Strip any character in ``chars`` from both ends (core ``Expr.strip_chars``)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.strip_chars() expects a typed column Expr.")
    return column.strip_chars(chars)


def strptime(column: Expr, format: str, *, to_datetime: bool = True) -> Expr:
    """Parse strings with ``strftime``-style ``format`` (core :meth:`Expr.strptime`)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.strptime() expects a typed column Expr.")
    return column.strptime(format, to_datetime=to_datetime)


def binary_len(column: Expr) -> Expr:
    """Byte length of ``bytes`` column (core :meth:`Expr.binary_len`)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.binary_len() expects a typed column Expr.")
    return column.binary_len()


def list_len(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_len() expects a typed column Expr.")
    return column.list_len()


def list_get(column: Expr, index: Any) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_get() expects a typed column Expr.")
    return column.list_get(index)


def list_contains(column: Expr, value: Any) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_contains() expects a typed column Expr.")
    return column.list_contains(value)


def list_min(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_min() expects a typed column Expr.")
    return column.list_min()


def list_max(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_max() expects a typed column Expr.")
    return column.list_max()


def list_sum(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_sum() expects a typed column Expr.")
    return column.list_sum()


def list_mean(column: Expr) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_mean() expects a typed column Expr.")
    return column.list_mean()


def list_join(column: Expr, delimiter: str, *, ignore_nulls: bool = False) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_join() expects a typed column Expr.")
    return column.list_join(delimiter, ignore_nulls=ignore_nulls)


def list_sort(
    column: Expr,
    *,
    descending: bool = False,
    nulls_last: bool = False,
    maintain_order: bool = False,
) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.list_sort() expects a typed column Expr.")
    return column.list_sort(
        descending=descending,
        nulls_last=nulls_last,
        maintain_order=maintain_order,
    )


def array_distinct(column: Expr, *, stable: bool = False) -> Expr:
    if not isinstance(column, Expr):
        raise TypeError("functions.array_distinct() expects a typed column Expr.")
    return column.list_unique(stable=stable)


def abs(column: Expr) -> Expr:
    """Absolute value for numeric columns (Spark ``abs``)."""
    return column.abs()


def round(column: Expr, scale: int = 0) -> Expr:
    """Round to ``scale`` decimal places (Spark ``round``)."""
    return column.round(scale)


def floor(column: Expr) -> Expr:
    """Largest integer not greater than the value (Spark ``floor``)."""
    return column.floor()


def ceil(column: Expr) -> Expr:
    """Smallest integer not less than the value (Spark ``ceil``)."""
    return column.ceil()


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


def sum(column: Expr | str) -> Expr | _GroupedAggSpec:
    """Global ``sum`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if isinstance(column, str):
        return _GroupedAggSpec(op="sum", col=column)
    if not isinstance(column, Expr):
        raise TypeError(
            "functions.sum() expects a typed column Expr (use col(..., dtype=...))."
        )
    return Expr(rust_expr=_rust_core().expr_global_sum(column._rust_expr))


def avg(column: Expr | str) -> Expr | _GroupedAggSpec:
    """Global ``avg`` / mean for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if isinstance(column, str):
        return _GroupedAggSpec(op="mean", col=column)
    if not isinstance(column, Expr):
        raise TypeError("functions.avg() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_mean(column._rust_expr))


def mean(column: Expr | str) -> Expr | _GroupedAggSpec:
    """Alias of :func:`avg`."""
    return avg(column)


def max(column: Expr | str) -> Expr | _GroupedAggSpec:
    """Global ``max`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if isinstance(column, str):
        return _GroupedAggSpec(op="max", col=column)
    if not isinstance(column, Expr):
        raise TypeError("functions.max() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_max(column._rust_expr))


def min(column: Expr | str) -> Expr | _GroupedAggSpec:
    """Global ``min`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if isinstance(column, str):
        return _GroupedAggSpec(op="min", col=column)
    if not isinstance(column, Expr):
        raise TypeError("functions.min() expects a typed column Expr.")
    return Expr(rust_expr=_rust_core().expr_global_min(column._rust_expr))


def count(column: Expr | str | None = None) -> Expr | _GroupedAggSpec:
    """Non-null count of *column*, or row count if omitted (``count(*)``)."""
    if column is None:
        return Expr(rust_expr=_rust_core().expr_global_row_count())
    if isinstance(column, str):
        return _GroupedAggSpec(op="count", col=column)
    if not isinstance(column, Expr):
        raise TypeError("functions.count() expects a typed column Expr or None.")
    return Expr(rust_expr=_rust_core().expr_global_count(column._rust_expr))


def lag(column: Expr, n: int = 1) -> Any:
    """Windowed ``lag``; finish with ``.over(Window...)``."""
    return lag_expr(column, n)


def lead(column: Expr, n: int = 1) -> Any:
    """Windowed ``lead``; finish with ``.over(Window...)``."""
    return lead_expr(column, n)


def map_keys(column: Expr) -> Expr:
    """Per-row list of keys for ``dict[str, T]`` map columns."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_keys() expects a typed column Expr.")
    return column.map_keys()


def map_values(column: Expr) -> Expr:
    """Per-row list of values for ``dict[str, T]`` map columns."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_values() expects a typed column Expr.")
    return column.map_values()


def map_len(column: Expr) -> Expr:
    """Per-row map cardinality for ``dict[str, T]`` columns."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_len() expects a typed column Expr.")
    return column.map_len()


def map_get(column: Expr, key: str) -> Expr:
    """Per-row map lookup by string key (missing key -> null)."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_get() expects a typed column Expr.")
    return column.map_get(key)


def map_contains_key(column: Expr, key: str) -> Expr:
    """Per-row key membership for ``dict[str, T]`` columns."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_contains_key() expects a typed column Expr.")
    return column.map_contains_key(key)


def map_entries(column: Expr) -> Expr:
    """Per-row list of map entry structs ``{key, value}``."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_entries() expects a typed column Expr.")
    return column.map_entries()


def map_from_entries(column: Expr) -> Expr:
    """Per-row map construction from ``list[{key, value}]`` entry structs."""
    if not isinstance(column, Expr):
        raise TypeError("functions.map_from_entries() expects a typed column Expr.")
    return column.map_from_entries()


def element_at(column: Expr, key: str) -> Expr:
    """Map lookup alias with PySpark-style naming."""
    if not isinstance(column, Expr):
        raise TypeError("functions.element_at() expects a typed column Expr.")
    return column.element_at(key)


__all__ = [
    "abs",
    "array_distinct",
    "avg",
    "between",
    "binary_len",
    "cast",
    "ceil",
    "coalesce",
    "col",
    "column",
    "concat",
    "contains",
    "count",
    "day",
    "dayofmonth",
    "dayofweek",
    "dense_rank",
    "element_at",
    "ends_with",
    "floor",
    "hour",
    "isin",
    "isnotnull",
    "isnull",
    "json_path_match",
    "lag",
    "lead",
    "length",
    "list_contains",
    "list_get",
    "list_join",
    "list_len",
    "list_max",
    "list_mean",
    "list_min",
    "list_sort",
    "list_sum",
    "lit",
    "lower",
    "lpad",
    "lpad_zero",
    "map_contains_key",
    "map_entries",
    "map_from_entries",
    "map_get",
    "map_keys",
    "map_len",
    "map_values",
    "max",
    "mean",
    "min",
    "minute",
    "month",
    "nanosecond",
    "quarter",
    "rank",
    "regexp_extract",
    "regexp_like",
    "regexp_replace",
    "regexp_substr",
    "reverse",
    "rlike",
    "round",
    "row_number",
    "rpad",
    "second",
    "split",
    "starts_with",
    "str_contains_pat",
    "str_replace",
    "strip_chars",
    "strip_prefix",
    "strip_suffix",
    "strptime",
    "substring",
    "sum",
    "to_date",
    "trim",
    "unix_timestamp",
    "upper",
    "weekofyear",
    "when",
    "window_avg",
    "window_max",
    "window_min",
    "window_sum",
    "year",
]
