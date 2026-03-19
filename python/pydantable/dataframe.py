from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Mapping, Optional, Sequence, Set, Type, TypeVar, Union, cast, get_args, get_origin

from pydantic import BaseModel

from .expressions import ColumnRef, Expr, Literal
from .schema import make_derived_schema_type, schema_field_types, validate_columns_strict


def _load_rust_core() -> Any:
    try:
        from . import _core as rust_core  # type: ignore

        return rust_core
    except ImportError:
        return None


_RUST_CORE = _load_rust_core()


def _require_rust_core() -> Any:
    if _RUST_CORE is None:
        raise NotImplementedError("Rust extension is required for DataFrame execution.")
    return _RUST_CORE

SchemaT = TypeVar("SchemaT", bound=BaseModel)

_NoneType = type(None)

_SUPPORTED_BASE_TYPES = (int, float, bool, str)


def _is_bool_or_nullable_bool(dtype: Any) -> bool:
    """
    Return True if `dtype` is either `bool` or `Optional[bool]` / `Union[bool, None]`.
    """
    if dtype is bool:
        return True
    origin = get_origin(dtype)
    if origin is Union:
        args = tuple(get_args(dtype))
        if len(args) == 2 and _NoneType in args and bool in args:
            return True
    return False


def _base_type_from_nullable(dtype: Any) -> Any:
    """
    Extract the base scalar type from either `T` or `Optional[T]` / `Union[T, None]`.
    """
    if dtype in _SUPPORTED_BASE_TYPES:
        return dtype
    origin = get_origin(dtype)
    if origin is Union:
        args = tuple(get_args(dtype))
        if len(args) == 2 and _NoneType in args:
            base = args[0] if args[1] is _NoneType else args[1]
            if base in _SUPPORTED_BASE_TYPES:
                return base
    raise TypeError(f"Unsupported (non-nullable or nullable) dtype: {dtype!r}")


@dataclass(frozen=True)
class SelectStep:
    columns: List[str]


@dataclass(frozen=True)
class FilterStep:
    condition: Expr


@dataclass(frozen=True)
class WithColumnsStep:
    columns: Dict[str, Expr]


class DataFrame(Generic[SchemaT]):
    """
    Strongly-typed DataFrame.

    This skeleton focuses on:
    - Schema enforcement at DataFrame construction time.
    - Typed expression AST building.
    - Schema propagation through `select`, `filter`, `with_columns`.
    - Pure-Python `collect()` execution for now.
    """

    _schema_type: Type[BaseModel] | None = None

    def __class_getitem__(cls, schema_type: Any) -> Type["DataFrame[Any]"]:
        if not isinstance(schema_type, type) or not issubclass(schema_type, BaseModel):
            raise TypeError("DataFrame[Schema] expects a Pydantic BaseModel type.")

        name = f"{cls.__name__}[{schema_type.__name__}]"
        # Important: avoid referencing `DataFrame[Any]` in a runtime `cast(...)`
        # because it triggers `__class_getitem__` again.
        return type(name, (cls,), {"_schema_type": schema_type})

    def __init__(self, data: Mapping[str, Sequence[Any]], *, strict: bool = True):
        if self._schema_type is None:
            raise TypeError("Use DataFrame[SchemaType](data) to construct a typed DataFrame.")

        if not strict:
            raise NotImplementedError("Non-strict mode is not implemented in 0.4.0.")

        root_data = validate_columns_strict(data, self._schema_type)
        self._root_data: Dict[str, list[Any]] = root_data
        self._root_schema_type: Type[BaseModel] = self._schema_type
        self._current_schema_type: Type[BaseModel] = self._schema_type
        self._current_field_types = schema_field_types(self._current_schema_type)
        # Rust owns expression typing, logical planning, and execution.
        self._rust_plan = _require_rust_core().make_plan(self.schema_fields())

    @classmethod
    def _from_plan(
        cls,
        *,
        root_data: Dict[str, list[Any]],
        root_schema_type: Type[BaseModel],
        current_schema_type: Type[BaseModel],
        rust_plan: Any,
    ) -> "DataFrame[Any]":
        obj = cls.__new__(cls)
        obj._root_data = root_data
        obj._root_schema_type = root_schema_type
        obj._current_schema_type = current_schema_type
        obj._current_field_types = schema_field_types(current_schema_type)
        obj._rust_plan = rust_plan
        obj._schema_type = None
        return cast("DataFrame[Any]", obj)

    @property
    def schema_type(self) -> Type[BaseModel]:
        return self._current_schema_type

    def schema_fields(self) -> Dict[str, Any]:
        return dict(self._current_field_types)

    def col(self, name: str) -> ColumnRef:
        if name not in self._current_field_types:
            raise KeyError(f"Unknown column {name!r} for current schema.")
        return ColumnRef(name=name, dtype=self._current_field_types[name])

    def __getattr__(self, item: str) -> Any:
        # Called only when attribute resolution fails; treat schema fields as columns.
        if item in self._current_field_types:
            return self.col(item)
        raise AttributeError(item)

    def with_columns(self, **new_columns: Union[Expr, Any]) -> "DataFrame[Any]":
        rust = _require_rust_core()
        rust_columns: Dict[str, Any] = {}

        for name, value in new_columns.items():
            if isinstance(value, Expr):
                rust_columns[name] = value._rust_expr
            else:
                rust_columns[name] = rust.make_literal(value=value)

        rust_plan = rust.plan_with_columns(self._rust_plan, rust_columns)
        derived_fields = rust_plan.schema_fields()
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )

        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def select(self, *cols: Union[str, ColumnRef]) -> "DataFrame[Any]":
        rust = _require_rust_core()
        if not cols:
            raise ValueError("select() requires at least one column.")

        selected: List[str] = []
        for col in cols:
            if isinstance(col, str):
                selected.append(col)
            elif isinstance(col, Expr):
                referenced = col.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "select() accepts column names or a ColumnRef expression."
                    )
                selected.append(next(iter(referenced)))
            else:
                raise TypeError("select() accepts column names or ColumnRef objects.")

        rust_plan = rust.plan_select(self._rust_plan, selected)
        derived_fields = rust_plan.schema_fields()
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def filter(self, condition: Expr) -> "DataFrame[Any]":
        rust = _require_rust_core()

        if not isinstance(condition, Expr):
            raise TypeError("filter(condition) expects an Expr.")

        rust_plan = rust.plan_filter(self._rust_plan, condition._rust_expr)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def collect(self, *, engine: str = "rust") -> Dict[str, list[Any]]:
        """
        Materialize this typed logical DataFrame into Python column data.

        Execution is owned by Rust.
        """
        if engine != "rust":
            raise NotImplementedError(
                "The Rust-first build executes collect() in Rust only."
            )

        rust = _require_rust_core()
        result = rust.execute_plan(self._rust_plan, self._root_data)
        return result

    def to_dict(self) -> Dict[str, list[Any]]:
        return self.collect(engine="rust")

