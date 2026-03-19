from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterable, List, Mapping, Sequence, Set, Type, TypeVar, Union, cast

from pydantic import BaseModel

from .backend import execute_plan_rust
from .expressions import ColumnRef, Expr, Literal
from .schema import make_derived_schema_type, schema_field_types, validate_columns_strict

SchemaT = TypeVar("SchemaT", bound=BaseModel)


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
        self._plan: List[object] = []
        self._current_schema_type: Type[BaseModel] = self._schema_type
        self._current_field_types = schema_field_types(self._current_schema_type)

    @classmethod
    def _from_plan(
        cls,
        *,
        root_data: Dict[str, list[Any]],
        root_schema_type: Type[BaseModel],
        current_schema_type: Type[BaseModel],
        plan: List[object],
    ) -> "DataFrame[Any]":
        obj = cls.__new__(cls)
        obj._root_data = root_data
        obj._root_schema_type = root_schema_type
        obj._plan = plan
        obj._current_schema_type = current_schema_type
        obj._current_field_types = schema_field_types(current_schema_type)
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
        inferred: Dict[str, Any] = dict(self._current_field_types)
        exprs: Dict[str, Expr] = {}

        for name, value in new_columns.items():
            expr = value if isinstance(value, Expr) else Literal(value=value, dtype=type(value))
            referenced = expr.referenced_columns()
            missing = sorted(referenced - set(self._current_field_types.keys()))
            if missing:
                raise ValueError(
                    f"Expression for {name!r} references unknown columns: {missing}"
                )

            inferred[name] = getattr(expr, "dtype", object)
            exprs[name] = expr

        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, inferred
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            plan=self._plan + [WithColumnsStep(columns=exprs)],
        )

    def select(self, *cols: Union[str, ColumnRef]) -> "DataFrame[Any]":
        selected: List[str] = []
        for col in cols:
            if isinstance(col, str):
                selected.append(col)
            elif isinstance(col, ColumnRef):
                selected.append(col.name)
            else:
                raise TypeError("select() accepts column names or ColumnRef objects.")

        missing = sorted(set(selected) - set(self._current_field_types.keys()))
        if missing:
            raise KeyError(f"Unknown columns in select(): {missing}")

        inferred = {name: self._current_field_types[name] for name in selected}
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, inferred
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            plan=self._plan + [SelectStep(columns=selected)],
        )

    def filter(self, condition: Expr) -> "DataFrame[Any]":
        if not isinstance(condition, Expr):
            raise TypeError("filter(condition) expects an Expr.")

        referenced = condition.referenced_columns()
        missing = sorted(referenced - set(self._current_field_types.keys()))
        if missing:
            raise ValueError(
                f"Filter expression references unknown columns: {missing}"
            )

        # Skeleton does not enforce `bool` dtype at runtime strictly.
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            plan=self._plan + [FilterStep(condition=condition)],
        )

    def collect(self, *, engine: str = "python") -> Dict[str, list[Any]]:
        """
        Materialize this typed logical DataFrame into Python column data.
        """

        if engine == "python":
            return self._collect_python()
        if engine == "rust":
            return cast(Dict[str, list[Any]], execute_plan_rust(self._plan, self._root_data))
        raise ValueError(f"Unknown engine {engine!r}. Expected 'python' or 'rust'.")

    def _collect_python(self) -> Dict[str, list[Any]]:
        data: Dict[str, list[Any]] = dict(self._root_data)

        for step in self._plan:
            if isinstance(step, SelectStep):
                data = {name: data[name] for name in step.columns}
            elif isinstance(step, FilterStep):
                mask = step.condition.eval(data)
                if not all(isinstance(v, bool) for v in mask):
                    raise TypeError("Filter condition must evaluate to booleans.")
                data = {
                    name: [v for v, m in zip(vals, mask) if m]
                    for name, vals in data.items()
                }
            elif isinstance(step, WithColumnsStep):
                for name, expr in step.columns.items():
                    data[name] = expr.eval(data)
            else:
                raise RuntimeError(f"Unknown plan step type: {type(step)!r}")

        return data

    def to_dict(self) -> Dict[str, list[Any]]:
        return self.collect(engine="python")

