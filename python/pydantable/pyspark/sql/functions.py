from __future__ import annotations

from typing import Any

from pydantable.expressions import (
    ColumnRef,
    Expr,
    Literal,
    WhenChain,
    coalesce as coalesce_expr,
    concat as concat_expr,
    when as when_chain,
)


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


_AGG_HINT = (
    "functions.{name} is not implemented as a lazy column in pydantable. "
    "Use DataFrame.group_by(...).agg(output_name=('sum'|'mean'|'count', column))."
)


def sum(*_args: Any, **_kwargs: Any) -> Any:  # noqa: A001
    raise NotImplementedError(_AGG_HINT.format(name="sum"))


def avg(*_args: Any, **_kwargs: Any) -> Any:
    raise NotImplementedError(_AGG_HINT.format(name="avg"))


def mean(*_args: Any, **_kwargs: Any) -> Any:
    raise NotImplementedError(_AGG_HINT.format(name="mean"))


def max(*_args: Any, **_kwargs: Any) -> Any:  # noqa: A001
    raise NotImplementedError(_AGG_HINT.format(name="max"))


def min(*_args: Any, **_kwargs: Any) -> Any:  # noqa: A001
    raise NotImplementedError(_AGG_HINT.format(name="min"))


def count(*_args: Any, **_kwargs: Any) -> Any:
    raise NotImplementedError(_AGG_HINT.format(name="count"))


__all__ = [
    "avg",
    "between",
    "cast",
    "coalesce",
    "col",
    "column",
    "concat",
    "count",
    "isin",
    "isnull",
    "isnotnull",
    "length",
    "lit",
    "max",
    "mean",
    "min",
    "substring",
    "sum",
    "when",
]
