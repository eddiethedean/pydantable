"""Typed column expressions (:class:`Expr`) validated and lowered in Rust.

Use operators and methods on :class:`Expr` to build trees; window helpers return
pending objects finished with ``.over(WindowSpec(...))``. Globals such as
:func:`global_sum` are only valid inside :meth:`pydantable.dataframe.DataFrame.select`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Sequence, get_args, get_origin

from .rust_engine import _require_rust_core

# Public surface for stubs and docs.
__all__ = [
    "AliasedExpr",
    "BinaryOp",
    "ColumnRef",
    "CompareOp",
    "Expr",
    "Literal",
    "WhenChain",
    "coalesce",
    "concat",
    "dense_rank",
    "global_count",
    "global_max",
    "global_mean",
    "global_min",
    "global_row_count",
    "global_sum",
    "lag",
    "lead",
    "rank",
    "row_number",
    "when",
    "window_max",
    "window_mean",
    "window_min",
    "window_sum",
]

# Bound serialized AST size in :meth:`Expr.__repr__` for readable REPL output.
_MAX_EXPR_REPR_AST = 200


def _rust_expr_ast_snippet(rust_expr: Any) -> str:
    try:
        data = rust_expr.to_serializable()
        s = json.dumps(data, default=str, separators=(",", ":"))
        if len(s) > _MAX_EXPR_REPR_AST:
            return f"{s[: _MAX_EXPR_REPR_AST - 1]}…"
        return s
    except Exception:
        return "?"


if TYPE_CHECKING:
    from .window_spec import WindowSpec


@dataclass(frozen=True, slots=True)
class AliasedExpr:
    """Expression with an explicit output column name (Polars-style alias).

    Used by :meth:`pydantable.dataframe.DataFrame.select` and
    :meth:`pydantable.dataframe.DataFrame.with_columns` positional overloads.
    """

    name: str
    expr: Expr


class Expr:  # type: ignore[override]
    """Column expression: operators/methods build a Rust AST with static dtypes."""

    def __init__(self, *, rust_expr: Any):
        self._rust_expr = rust_expr

    @property
    def dtype(self) -> Any:
        return self._rust_expr.dtype

    def referenced_columns(self) -> set[str]:
        return set(self._rust_expr.referenced_columns())

    def alias(self, name: str) -> AliasedExpr:
        """Attach an output column name for use in `select` / `with_columns`."""
        if not isinstance(name, str) or not name:
            raise TypeError("alias(name) expects a non-empty string.")
        return AliasedExpr(name=str(name), expr=self)

    def __repr__(self) -> str:
        cls = type(self).__name__
        refs = sorted(self.referenced_columns())
        ref_s = f" refs={refs!r}" if refs else ""
        ast_s = _rust_expr_ast_snippet(self._rust_expr)
        return f"{cls}(dtype={self.dtype!r}{ref_s} ast={ast_s})"

    def _coerce_other(self, other: Any) -> Expr:
        if isinstance(other, Expr):
            return other
        return Literal(value=other)

    def _binary(self, op_symbol: str, other: Any) -> Expr:
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            op_symbol, self._rust_expr, other_expr._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def _binary_reflected(self, op_symbol: str, other: Any) -> Expr:
        # `other <op> self`
        left_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().binary_op(
            op_symbol, left_expr._rust_expr, self._rust_expr
        )
        return BinaryOp(rust_expr=rust_expr)

    def _compare(self, op_symbol: str, other: Any) -> Expr:
        other_expr = self._coerce_other(other)
        rust_expr = _require_rust_core().compare_op(
            op_symbol, self._rust_expr, other_expr._rust_expr
        )
        return CompareOp(rust_expr=rust_expr)

    def cast(self, dtype: Any) -> Expr:
        rust_expr = _require_rust_core().cast_expr(self._rust_expr, dtype)
        return Expr(rust_expr=rust_expr)

    def is_null(self) -> Expr:
        rust_expr = _require_rust_core().is_null_expr(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def is_not_null(self) -> Expr:
        rust_expr = _require_rust_core().is_not_null_expr(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def over(
        self,
        partition_by: str | list[str] | tuple[str, ...] | None = None,
        order_by: str | list[str] | tuple[str, ...] | None = None,
    ) -> Expr:
        if partition_by is None and order_by is None:
            return self
        raise TypeError(
            "Expr.over(partition_by=..., order_by=...) is not supported. "
            "Use window functions such as row_number().over(WindowSpec(...)) "
            "or pydantable.window_spec.Window.partitionBy(...).orderBy(...)."
        )

    # Arithmetic
    def __add__(self, other: Any) -> Expr:
        return self._binary("+", other)

    def __sub__(self, other: Any) -> Expr:
        return self._binary("-", other)

    def __mul__(self, other: Any) -> Expr:
        return self._binary("*", other)

    def __truediv__(self, other: Any) -> Expr:
        return self._binary("/", other)

    def __radd__(self, other: Any) -> Expr:
        return self._binary_reflected("+", other)

    def __rsub__(self, other: Any) -> Expr:
        return self._binary_reflected("-", other)

    def __rmul__(self, other: Any) -> Expr:
        return self._binary_reflected("*", other)

    def __rtruediv__(self, other: Any) -> Expr:
        return self._binary_reflected("/", other)

    # Comparisons
    def __eq__(self, other: Any) -> Expr:  # type: ignore[override]
        return self._compare("==", other)

    def __ne__(self, other: Any) -> Expr:  # type: ignore[override]
        return self._compare("!=", other)

    def __lt__(self, other: Any) -> Expr:
        return self._compare("<", other)

    def __le__(self, other: Any) -> Expr:
        return self._compare("<=", other)

    def __gt__(self, other: Any) -> Expr:
        return self._compare(">", other)

    def __ge__(self, other: Any) -> Expr:
        return self._compare(">=", other)

    def isin(self, *values: Any) -> Expr:
        if len(values) == 1 and isinstance(values[0], (list, tuple)):
            vals = list(values[0])
        else:
            vals = list(values)
        rust_expr = _require_rust_core().expr_in_list(self._rust_expr, vals)
        return Expr(rust_expr=rust_expr)

    def is_in(self, *values: Any) -> Expr:
        """Alias of :meth:`isin` (Polars naming parity)."""
        return self.isin(*values)

    def len(self) -> Expr:
        """String length alias (typed-safe): only valid for ``str`` columns."""
        dt = self.dtype
        origin = get_origin(dt)
        args = get_args(dt)
        if origin is None:
            base = dt
        elif origin is getattr(__import__("typing"), "Union", object()) or str(
            origin
        ).endswith("types.UnionType"):
            non_none = [a for a in args if a is not type(None)]
            base = non_none[0] if len(non_none) == 1 else dt
        else:
            base = dt
        if base is not str:
            raise TypeError("len() is only supported for string columns.")
        return self.char_length()

    def between(self, low: Any, high: Any) -> Expr:
        lo = self._coerce_other(low)
        hi = self._coerce_other(high)
        rust_expr = _require_rust_core().expr_between(
            self._rust_expr, lo._rust_expr, hi._rust_expr
        )
        return Expr(rust_expr=rust_expr)

    def substr(self, start: Any, length: Any | None = None) -> Expr:
        st = self._coerce_other(start)
        rust = _require_rust_core()
        if length is None:
            rust_expr = rust.expr_substring(self._rust_expr, st._rust_expr, None)
        else:
            ln = self._coerce_other(length)
            rust_expr = rust.expr_substring(
                self._rust_expr, st._rust_expr, ln._rust_expr
            )
        return Expr(rust_expr=rust_expr)

    def char_length(self) -> Expr:
        rust_expr = _require_rust_core().expr_string_length(self._rust_expr)
        return Expr(rust_expr=rust_expr)

    def struct_field(self, name: str) -> Expr:
        rust_expr = _require_rust_core().expr_struct_field(self._rust_expr, name)
        return Expr(rust_expr=rust_expr)

    def struct_json_encode(self) -> Expr:
        """Encode each struct cell as a JSON text value (Polars ``struct.json_encode``)."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_struct_json_encode(self._rust_expr))

    def struct_json_path_match(self, path: str) -> Expr:
        """JSONPath against struct cells (JSON-encode then ``str.json_path_match``).

        Same null/match semantics as :meth:`str_json_path_match` on strings.
        Empty ``path`` raises ``ValueError``.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_struct_json_path_match(self._rust_expr, str(path)),
        )

    def struct_rename_fields(self, names: Sequence[str]) -> Expr:
        """Rename struct subfields in order (one new name per existing field)."""
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_struct_rename_fields(
                self._rust_expr, [str(x) for x in names]
            ),
        )

    def struct_with_fields(self, **fields: Any) -> Expr:
        """Add or replace struct subfields (Polars ``struct.with_fields``).

        Each keyword must be a field name; each value must be an :class:`Expr`.
        """
        if not fields:
            raise TypeError(
                "struct_with_fields() requires at least one keyword field=Expr."
            )
        rust = _require_rust_core()
        updates: list[tuple[str, Any]] = []
        for k, v in fields.items():
            if not isinstance(v, Expr):
                raise TypeError(
                    f"struct_with_fields({k}=...) expects Expr, got {type(v).__name__}."
                )
            updates.append((str(k), v._rust_expr))
        return Expr(
            rust_expr=rust.expr_struct_with_fields(self._rust_expr, updates),
        )

    # Numeric
    def abs(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_abs(self._rust_expr))

    def round(self, decimals: int = 0) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_round(self._rust_expr, int(decimals)))

    def floor(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_floor(self._rust_expr))

    def ceil(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_ceil(self._rust_expr))

    def cumsum(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_row_accum_cum_sum(self._rust_expr))

    def cumprod(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_row_accum_cum_prod(self._rust_expr))

    def cummin(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_row_accum_cum_min(self._rust_expr))

    def cummax(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_row_accum_cum_max(self._rust_expr))

    def diff(self, periods: int = 1) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_row_accum_diff(self._rust_expr, int(periods)))

    def pct_change(self, periods: int = 1) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_row_accum_pct_change(self._rust_expr, int(periods))
        )

    def clip(self, lower: Any = None, upper: Any = None) -> Expr:
        e: Expr = self
        if lower is not None:
            lo = self._coerce_other(lower)
            e = when(self < lo, lo).otherwise(e)
        if upper is not None:
            hi = self._coerce_other(upper)
            e = when(e > hi, hi).otherwise(e)
        return e

    def replace(self, to_replace: dict[Any, Any]) -> Expr:
        items = list(to_replace.items())
        if not items:
            return self
        if len(items) > 64:
            raise ValueError("replace() supports at most 64 mappings.")
        chain = when(self == Literal(value=items[0][0]), Literal(value=items[0][1]))
        for old, new in items[1:]:
            chain = chain.when(self == Literal(value=old), Literal(value=new))
        return chain.otherwise(self)

    # Strings
    def strip(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "strip"))

    def upper(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "upper"))

    def lower(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_unary(self._rust_expr, "lower"))

    def str_replace(
        self, pattern: str, replacement: str, *, literal: bool = True
    ) -> Expr:
        """Replace matches.

        Default ``literal=True`` is substring replace.
        Use ``literal=False`` for Rust regex (syntax differs from Python ``re``;
        see docs).

        Invalid regex patterns may yield null cells at execution (Polars) rather
        than raise.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_replace(
                self._rust_expr,
                str(pattern),
                str(replacement),
                literal=bool(literal),
            )
        )

    def starts_with(self, prefix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_predicate(
                self._rust_expr, "starts_with", str(prefix)
            )
        )

    def ends_with(self, suffix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_predicate(
                self._rust_expr, "ends_with", str(suffix)
            )
        )

    def str_contains(self, substring: str) -> Expr:
        """True where the string contains ``substring`` (literal, not regex).

        The empty substring matches every non-null string (Polars substring
        ``contains`` semantics).
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_predicate(
                self._rust_expr, "contains", str(substring), literal=True
            )
        )

    def str_contains_pat(self, pattern: str, *, literal: bool = False) -> Expr:
        """Substring or Rust-regex match.

        ``literal=False`` uses the Rust ``regex`` dialect (not Python ``re``).
        Raises ``ValueError`` if ``pattern`` is empty in regex mode.
        Malformed regex may yield null per row at execution; see
        ``SUPPORTED_TYPES`` docs.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_predicate(
                self._rust_expr, "contains", str(pattern), literal=bool(literal)
            )
        )

    def matches(self, pattern: str) -> Expr:
        """Regex match predicate (Rust regex dialect)."""
        if not isinstance(pattern, str) or not pattern:
            raise TypeError("matches(pattern) expects a non-empty string.")
        return self.str_contains_pat(pattern, literal=False)

    def is_empty_str(self) -> Expr:
        """True where string cell is exactly ``\"\"``."""
        return self == ""

    def is_blank_str(self) -> Expr:
        """True where string cell is empty after stripping whitespace."""
        return self.strip().char_length() == 0

    def is_null_or_empty_str(self) -> Expr:
        return self.is_null() | self.is_empty_str()

    def is_not_null_and_not_empty_str(self) -> Expr:
        return self.is_not_null() & ~(self.is_empty_str())

    def str_split(self, delimiter: str) -> Expr:
        """Split string column into ``list[str]`` (per-row).

        Delimiter is literal (not regex). Empty ``delimiter`` follows Polars UTF-8
        split rules.
        Null string cells stay null. Edge cases are documented in
        ``SUPPORTED_TYPES``.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_string_split(self._rust_expr, str(delimiter)))

    def str_reverse(self) -> Expr:
        """Reverse each string (Polars ``str.reverse``).

        Unicode edge cases (e.g. combining marks) follow Polars, not naive
        codepoint reversal. See ``SUPPORTED_TYPES``.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_str_reverse(self._rust_expr))

    def str_pad_start(self, length: int, fill_char: str = " ") -> Expr:
        """Pad start to at least ``length`` characters (character count).

        ``fill_char`` must be exactly one non-empty character; otherwise
        ``ValueError`` at build time.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_str_pad_start(
                self._rust_expr, int(length), str(fill_char)
            )
        )

    def str_pad_end(self, length: int, fill_char: str = " ") -> Expr:
        """Pad end to at least ``length`` characters.

        Same ``fill_char`` rules as :meth:`str_pad_start`.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_str_pad_end(
                self._rust_expr, int(length), str(fill_char)
            )
        )

    def str_zfill(self, length: int) -> Expr:
        """Zero-pad strings to ``length`` (sign handled like Polars ``str.zfill``)."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_str_zfill(self._rust_expr, int(length)))

    def str_extract_regex(self, pattern: str, group_index: int = 1) -> Expr:
        """Extract a regex capture group per row (Rust ``regex`` dialect).

        ``group_index`` 0 is the full match; 1+ are capture groups. Empty
        ``pattern`` raises ``ValueError``. No match or invalid regex may yield
        null; see ``SUPPORTED_TYPES``.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_str_extract_regex(
                self._rust_expr, str(pattern), int(group_index)
            )
        )

    def str_json_path_match(self, path: str) -> Expr:
        """JSONPath against JSON text cells (Polars ``str.json_path_match``).

        Returns a **string** column (serialized match). Malformed JSON or no
        match often yields null at execution time. Empty ``path`` raises
        ``ValueError``.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_str_json_path_match(self._rust_expr, str(path)))

    def strip_prefix(self, prefix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(
                self._rust_expr, "strip_prefix", str(prefix)
            )
        )

    def strip_suffix(self, suffix: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(
                self._rust_expr, "strip_suffix", str(suffix)
            )
        )

    def strip_chars(self, chars: str) -> Expr:
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_string_unary(self._rust_expr, "strip_chars", str(chars))
        )

    # Boolean logic (typed; operands must be boolean expressions)
    def __and__(self, other: Any) -> Expr:
        right = other if isinstance(other, Expr) else self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_and(self._rust_expr, right._rust_expr))

    def __rand__(self, other: Any) -> Expr:
        left = self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_and(left._rust_expr, self._rust_expr))

    def __or__(self, other: Any) -> Expr:
        right = other if isinstance(other, Expr) else self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_or(self._rust_expr, right._rust_expr))

    def __ror__(self, other: Any) -> Expr:
        left = self._coerce_other(other)
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_or(left._rust_expr, self._rust_expr))

    def __invert__(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_logical_not(self._rust_expr))

    # Datetime / date parts (Rust validates column type)
    def dt_year(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "year"))

    def dt_month(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "month"))

    def dt_day(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "day"))

    def dt_hour(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "hour"))

    def dt_minute(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "minute"))

    def dt_second(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "second"))

    def dt_nanosecond(self) -> Expr:
        """Sub-second nanoseconds component (``datetime`` or ``time`` columns)."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "nanosecond"))

    def dt_weekday(self) -> Expr:
        """ISO weekday on ``date`` / ``datetime`` (Mon=1 ... Sun=7, same as Polars).

        Not valid on ``time`` columns (``TypeError`` at build time).
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "weekday"))

    def dt_quarter(self) -> Expr:
        """Calendar quarter 1-4 on ``date`` / ``datetime``.

        Not valid on ``time`` columns (``TypeError`` at build time).
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "quarter"))

    def dt_week(self) -> Expr:
        """ISO 8601 week number 1-53 (``date`` / ``datetime``; Polars ``dt.week``).

        Same definition as Python ``datetime.date.isocalendar().week`` /
        Polars ``dt.week()`` (weeks start Monday; week 1 contains the first
        Thursday of the year). Not valid on ``time`` columns.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "week"))

    def dt_dayofyear(self) -> Expr:
        """Day of year 1-366 on ``date`` / ``datetime`` (Spark ``dayofyear``).

        Matches Polars ``dt.ordinal_day()``. Not valid on ``time`` columns.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_temporal_part(self._rust_expr, "dayofyear"))

    def dt_date(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_datetime_to_date(self._rust_expr))

    def strptime(self, format: str, *, to_datetime: bool = False) -> Expr:
        """Parse strings to ``date`` or ``datetime`` (``strftime`` format string)."""
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_strptime(
                self._rust_expr, str(format), bool(to_datetime)
            ),
        )

    def unix_timestamp(self, unit: str = "seconds") -> Expr:
        """Unix epoch from ``date``/``datetime``; ``unit`` is ``seconds`` or ``ms``."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_unix_timestamp(self._rust_expr, str(unit)))

    def from_unix_time(self, unit: str = "seconds") -> Expr:
        """UTC-naive ``datetime`` from numeric epoch; ``unit`` is ``seconds`` or ``ms``.

        Inverse of :meth:`unix_timestamp` for typical non-null numeric input.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_from_unix_time(self._rust_expr, str(unit)))

    def binary_len(self) -> Expr:
        """Byte length of a ``bytes`` column."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_binary_length(self._rust_expr))

    def map_len(self) -> Expr:
        """Number of entries in a ``dict[str, T]`` map column."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_len(self._rust_expr))

    def map_get(self, key: str) -> Expr:
        """Value for a string key (missing key → null)."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_get(self._rust_expr, str(key)))

    def map_contains_key(self, key: str) -> Expr:
        """Whether the map contains the given string key."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_contains_key(self._rust_expr, str(key)))

    def map_keys(self) -> Expr:
        """List of keys for each map cell."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_keys(self._rust_expr))

    def map_values(self) -> Expr:
        """List of values for each map cell."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_values(self._rust_expr))

    def map_entries(self) -> Expr:
        """List of ``{key, value}`` entry structs for each map cell."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_entries(self._rust_expr))

    def map_from_entries(self) -> Expr:
        """Build ``dict[str, T]`` map cells from ``list[{key, value}]`` entries."""
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_map_from_entries(self._rust_expr))

    def element_at(self, key: str) -> Expr:
        """Alias of :meth:`map_get` for map columns."""
        return self.map_get(key)

    # List columns
    def list_len(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_len(self._rust_expr))

    def list_get(self, index: Any) -> Expr:
        rust = _require_rust_core()
        idx = index if isinstance(index, Expr) else Literal(value=index)
        return Expr(
            rust_expr=rust.expr_list_get(self._rust_expr, idx._rust_expr),
        )

    def list_contains(self, value: Any) -> Expr:
        rust = _require_rust_core()
        v = value if isinstance(value, Expr) else Literal(value=value)
        return Expr(
            rust_expr=rust.expr_list_contains(self._rust_expr, v._rust_expr),
        )

    def contains_any(self, values: Any) -> Expr:
        """Any of the provided values is contained in each list cell."""
        vals = values
        if isinstance(values, Expr):
            raise TypeError("contains_any(values) expects literal values, not Expr.")
        if not isinstance(values, (list, tuple, set)):
            vals = [values]
        expr: Expr | None = None
        for v in list(vals):
            term = self.list_contains(v)
            expr = term if expr is None else (expr | term)
        if expr is None:
            raise TypeError("contains_any(values) expects at least one value.")
        return expr

    def contains_all(self, values: Any) -> Expr:
        """All of the provided values are contained in each list cell."""
        vals = values
        if isinstance(values, Expr):
            raise TypeError("contains_all(values) expects literal values, not Expr.")
        if not isinstance(values, (list, tuple, set)):
            vals = [values]
        expr: Expr | None = None
        for v in list(vals):
            term = self.list_contains(v)
            expr = term if expr is None else (expr & term)
        if expr is None:
            raise TypeError("contains_all(values) expects at least one value.")
        return expr

    def list_is_empty(self) -> Expr:
        return self.list_len() == 0

    def list_any(self) -> Expr:
        """Any True in a boolean list."""
        return self.list_contains(True)

    def list_all(self) -> Expr:
        """All True in a boolean list."""
        return ~self.list_contains(False)

    def map_is_empty(self) -> Expr:
        return self.map_len() == 0

    def map_has_any_key(self, keys: Any) -> Expr:
        ks = keys
        if isinstance(keys, Expr):
            raise TypeError("map_has_any_key(keys) expects literal keys, not Expr.")
        if not isinstance(keys, (list, tuple, set)):
            ks = [keys]
        expr: Expr | None = None
        for k in list(ks):
            term = self.map_contains_key(str(k))
            expr = term if expr is None else (expr | term)
        if expr is None:
            raise TypeError("map_has_any_key(keys) expects at least one key.")
        return expr

    def list_min(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_min(self._rust_expr))

    def list_max(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_max(self._rust_expr))

    def list_sum(self) -> Expr:
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_sum(self._rust_expr))

    def list_mean(self) -> Expr:
        """Mean of each numeric list cell as ``float``.

        Requires ``list[int]`` or ``list[float]``. Empty lists and null list cells
        yield null.
        """
        rust = _require_rust_core()
        return Expr(rust_expr=rust.expr_list_mean(self._rust_expr))

    def list_join(self, separator: str, *, ignore_nulls: bool = False) -> Expr:
        """Join each ``list[str]`` cell (Polars ``list.join``).

        Empty lists yield empty strings. ``ignore_nulls`` skips null list
        elements when ``True``. See ``SUPPORTED_TYPES``.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_list_join(
                self._rust_expr, str(separator), ignore_nulls=bool(ignore_nulls)
            )
        )

    def list_sort(
        self,
        *,
        descending: bool = False,
        nulls_last: bool = False,
        maintain_order: bool = False,
    ) -> Expr:
        """Sort each list cell in place (``list[int]``, ``list[float]``, etc.).

        ``descending``, ``nulls_last``, and ``maintain_order`` map to Polars
        ``list.sort`` options. Element-type rules are in ``SUPPORTED_TYPES``.
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_list_sort(
                self._rust_expr,
                descending=bool(descending),
                nulls_last=bool(nulls_last),
                maintain_order=bool(maintain_order),
            )
        )

    def list_unique(self, *, stable: bool = False) -> Expr:
        """Deduplicate list elements per row.

        With ``stable=True``, first-seen order is preserved (Polars
        ``unique_stable``).
        """
        rust = _require_rust_core()
        return Expr(
            rust_expr=rust.expr_list_unique(self._rust_expr, stable=bool(stable))
        )


class WhenChain:
    """Chained ``when`` / ``otherwise`` (Spark-style)."""

    def __repr__(self) -> str:
        n = len(self._branches)
        return f"WhenChain({n} branch{'es' if n != 1 else ''})"

    def __init__(self, condition: Expr, value: Expr):
        if not isinstance(condition, Expr) or not isinstance(value, Expr):
            raise TypeError("when() expects Expr arguments.")
        self._branches: list[tuple[Expr, Expr]] = [(condition, value)]

    def when(self, condition: Expr, value: Expr) -> WhenChain:
        if not isinstance(condition, Expr) or not isinstance(value, Expr):
            raise TypeError("when().when(...) expects Expr arguments.")
        self._branches.append((condition, value))
        return self

    def otherwise(self, value: Expr) -> Expr:
        if not isinstance(value, Expr):
            raise TypeError("otherwise() expects an Expr.")
        rust = _require_rust_core()
        conds = [c._rust_expr for c, _ in self._branches]
        thens = [v._rust_expr for _, v in self._branches]
        return Expr(rust_expr=rust.expr_case_when(conds, thens, value._rust_expr))


def when(condition: Expr, value: Expr) -> WhenChain:
    """First branch of a ``CASE WHEN`` (chain ``.when(...).otherwise(...)``)."""
    return WhenChain(condition, value)


class ColumnRef(Expr):  # type: ignore[override]
    """Named column with an explicit Python annotation used for Rust typing."""

    def __init__(self, *, name: str, dtype: Any):
        self._column_name = name
        rust_expr = _require_rust_core().make_column_ref(
            name=name, dtype_annotation=dtype
        )
        super().__init__(rust_expr=rust_expr)

    def __repr__(self) -> str:
        return f"ColumnRef({self._column_name!r}, dtype={self.dtype!r})"


class Literal(Expr):  # type: ignore[override]
    """Scalar constant repeated on every row (dtype inferred from the value)."""

    def __init__(self, *, value: Any, dtype: Any = None):
        # `dtype` is accepted for backwards compatibility with the old skeleton.
        # Rust derives the actual dtype from the provided scalar value.
        _ = dtype
        rust_expr = _require_rust_core().make_literal(value=value)
        super().__init__(rust_expr=rust_expr)


class BinaryOp(Expr):  # type: ignore[override]
    """Arithmetic expression node (internal)."""

    def __init__(self, *, rust_expr: Any):
        super().__init__(rust_expr=rust_expr)


class CompareOp(Expr):  # type: ignore[override]
    """Comparison expression node (internal)."""

    def __init__(self, *, rust_expr: Any):
        super().__init__(rust_expr=rust_expr)


def coalesce(*exprs: Expr) -> Expr:
    """SQL ``coalesce``: first non-null among compatible typed expressions."""
    if not exprs:
        raise TypeError("coalesce() requires at least one expression.")
    rust = _require_rust_core()
    return Expr(
        rust_expr=rust.coalesce_exprs([e._rust_expr for e in exprs]),
    )


def concat(*exprs: Expr) -> Expr:
    """Concatenate string expressions."""
    if len(exprs) < 2:
        raise TypeError("concat() requires at least two expressions.")
    for e in exprs:
        if not isinstance(e, Expr):
            raise TypeError("concat() arguments must be Expr instances.")
    rust = _require_rust_core()
    return Expr(rust_expr=rust.expr_string_concat([e._rust_expr for e in exprs]))


class _WindowFnPending:
    """Spark-style ``fn().over(WindowSpec(...))`` for one window expression."""

    def __init__(self, kind: str):
        self._kind = kind

    def __repr__(self) -> str:
        return f"_WindowFnPending({self._kind!r})"

    def over(self, window: WindowSpec) -> Expr:
        rust = _require_rust_core()
        part = list(window.partition_by)
        order = list(window.order_by)
        frame_kind = window.frame_kind
        frame_start = window.frame_start
        frame_end = window.frame_end
        if self._kind == "row_number":
            return Expr(
                rust_expr=rust.expr_window_row_number(
                    part, order, frame_kind, frame_start, frame_end
                )
            )
        if self._kind == "rank":
            return Expr(
                rust_expr=rust.expr_window_rank(
                    False, part, order, frame_kind, frame_start, frame_end
                )
            )
        if self._kind == "dense_rank":
            return Expr(
                rust_expr=rust.expr_window_rank(
                    True, part, order, frame_kind, frame_start, frame_end
                )
            )
        if self._kind == "percent_rank":
            return Expr(
                rust_expr=rust.expr_window_percent_rank(
                    part, order, frame_kind, frame_start, frame_end
                )
            )
        if self._kind == "cume_dist":
            return Expr(
                rust_expr=rust.expr_window_cume_dist(
                    part, order, frame_kind, frame_start, frame_end
                )
            )
        raise AssertionError(self._kind)


class _WindowValuePending:
    """Deferred window value function; complete with ``.over(WindowSpec(...))``."""

    def __init__(self, inner: Expr, kind: str, n: int | None = None):
        self._inner = inner
        self._kind = kind
        self._n = n

    def __repr__(self) -> str:
        return f"_WindowValuePending({self._kind!r}, n={self._n!r})"

    def over(self, window: WindowSpec) -> Expr:
        rust = _require_rust_core()
        part = list(window.partition_by)
        order = list(window.order_by)
        frame_kind = window.frame_kind
        frame_start = window.frame_start
        frame_end = window.frame_end
        if self._kind == "first_value":
            return Expr(
                rust_expr=rust.expr_window_first_value(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "last_value":
            return Expr(
                rust_expr=rust.expr_window_last_value(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "nth_value":
            assert self._n is not None
            return Expr(
                rust_expr=rust.expr_window_nth_value(
                    self._inner._rust_expr,
                    int(self._n),
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        raise AssertionError(self._kind)


class _WindowNtilePending:
    """Deferred ntile(N); complete with ``.over(WindowSpec(...))``."""

    def __init__(self, n: int):
        self._n = n

    def __repr__(self) -> str:
        return f"_WindowNtilePending({self._n!r})"

    def over(self, window: WindowSpec) -> Expr:
        rust = _require_rust_core()
        part = list(window.partition_by)
        order = list(window.order_by)
        return Expr(
            rust_expr=rust.expr_window_ntile(
                int(self._n),
                part,
                order,
                window.frame_kind,
                window.frame_start,
                window.frame_end,
            )
        )


class _WindowAggPending:
    """Deferred window aggregate; complete with ``.over(WindowSpec(...))``."""

    def __init__(self, inner: Expr, kind: str):
        self._inner = inner
        self._kind = kind

    def __repr__(self) -> str:
        return f"_WindowAggPending({self._kind!r}, {self._inner!r})"

    def over(self, window: WindowSpec) -> Expr:
        rust = _require_rust_core()
        part = list(window.partition_by)
        order = list(window.order_by)
        frame_kind = window.frame_kind
        frame_start = window.frame_start
        frame_end = window.frame_end
        if self._kind == "sum":
            return Expr(
                rust_expr=rust.expr_window_sum(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "mean":
            return Expr(
                rust_expr=rust.expr_window_mean(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "min":
            return Expr(
                rust_expr=rust.expr_window_min(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "max":
            return Expr(
                rust_expr=rust.expr_window_max(
                    self._inner._rust_expr,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        raise AssertionError(self._kind)


class _WindowShiftPending:
    """``lag`` / ``lead`` with ``.over(WindowSpec(...))``."""

    def __init__(self, inner: Expr, kind: str, n: int):
        self._inner = inner
        self._kind = kind
        self._n = int(n)

    def __repr__(self) -> str:
        return f"_WindowShiftPending({self._kind!r}, n={self._n}, {self._inner!r})"

    def over(self, window: WindowSpec) -> Expr:
        rust = _require_rust_core()
        part = list(window.partition_by)
        order = list(window.order_by)
        frame_kind = window.frame_kind
        frame_start = window.frame_start
        frame_end = window.frame_end
        if self._kind == "lag":
            return Expr(
                rust_expr=rust.expr_window_lag(
                    self._inner._rust_expr,
                    self._n,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        if self._kind == "lead":
            return Expr(
                rust_expr=rust.expr_window_lead(
                    self._inner._rust_expr,
                    self._n,
                    part,
                    order,
                    frame_kind,
                    frame_start,
                    frame_end,
                )
            )
        raise AssertionError(self._kind)


def row_number() -> _WindowFnPending:
    """Spark ``row_number``; finish with ``.over(WindowSpec(...))``."""
    return _WindowFnPending("row_number")


def rank() -> _WindowFnPending:
    """Spark ``rank`` (ties share rank; gaps after ties)."""
    return _WindowFnPending("rank")


def dense_rank() -> _WindowFnPending:
    """Spark ``dense_rank`` (ties share rank; no gaps)."""
    return _WindowFnPending("dense_rank")


def percent_rank() -> _WindowFnPending:
    """Spark ``percent_rank`` over a window."""
    return _WindowFnPending("percent_rank")


def cume_dist() -> _WindowFnPending:
    """Spark ``cume_dist`` over a window."""
    return _WindowFnPending("cume_dist")


def window_sum(column: Expr) -> _WindowAggPending:
    """``sum`` over a window (not ``group_by`` aggregation)."""
    if not isinstance(column, Expr):
        raise TypeError("window_sum() expects an Expr.")
    return _WindowAggPending(column, "sum")


def window_mean(column: Expr) -> _WindowAggPending:
    """``avg`` / mean over a window."""
    if not isinstance(column, Expr):
        raise TypeError("window_mean() expects an Expr.")
    return _WindowAggPending(column, "mean")


def window_min(column: Expr) -> _WindowAggPending:
    """``min`` over a window."""
    if not isinstance(column, Expr):
        raise TypeError("window_min() expects an Expr.")
    return _WindowAggPending(column, "min")


def window_max(column: Expr) -> _WindowAggPending:
    """``max`` over a window."""
    if not isinstance(column, Expr):
        raise TypeError("window_max() expects an Expr.")
    return _WindowAggPending(column, "max")


def first_value(column: Expr) -> _WindowValuePending:
    """Spark ``first_value`` over a window."""
    return _WindowValuePending(column, "first_value")


def last_value(column: Expr) -> _WindowValuePending:
    """Spark ``last_value`` over a window."""
    return _WindowValuePending(column, "last_value")


def nth_value(column: Expr, n: int) -> _WindowValuePending:
    """Spark ``nth_value`` over a window (1-based)."""
    return _WindowValuePending(column, "nth_value", n=int(n))


def ntile(n: int) -> _WindowNtilePending:
    """Spark ``ntile`` over a window."""
    return _WindowNtilePending(n=int(n))


def lag(column: Expr, n: int = 1) -> _WindowShiftPending:
    """Previous row value within partition/order (``shift(n)``)."""
    if not isinstance(column, Expr):
        raise TypeError("lag() expects an Expr.")
    return _WindowShiftPending(column, "lag", n)


def lead(column: Expr, n: int = 1) -> _WindowShiftPending:
    """Next row value within partition/order (``shift(-n)``)."""
    if not isinstance(column, Expr):
        raise TypeError("lead() expects an Expr.")
    return _WindowShiftPending(column, "lead", n)


def global_sum(column: Expr) -> Expr:
    """Whole-frame ``sum`` for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("global_sum() expects an Expr.")
    return Expr(rust_expr=_require_rust_core().expr_global_sum(column._rust_expr))


def global_mean(column: Expr) -> Expr:
    """Whole-frame mean for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("global_mean() expects an Expr.")
    return Expr(rust_expr=_require_rust_core().expr_global_mean(column._rust_expr))


def global_count(column: Expr) -> Expr:
    """Whole-frame non-null ``count`` for ``DataFrame.select``."""
    if not isinstance(column, Expr):
        raise TypeError("global_count() expects an Expr.")
    return Expr(rust_expr=_require_rust_core().expr_global_count(column._rust_expr))


def global_min(column: Expr) -> Expr:
    """Whole-frame minimum for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("global_min() expects an Expr.")
    return Expr(rust_expr=_require_rust_core().expr_global_min(column._rust_expr))


def global_max(column: Expr) -> Expr:
    """Whole-frame maximum for :meth:`~pydantable.dataframe.DataFrame.select`."""
    if not isinstance(column, Expr):
        raise TypeError("global_max() expects an Expr.")
    return Expr(rust_expr=_require_rust_core().expr_global_max(column._rust_expr))


def global_row_count() -> Expr:
    """Row count for the current frame (Spark ``count(*)``); global ``select`` only."""
    return Expr(rust_expr=_require_rust_core().expr_global_row_count())
