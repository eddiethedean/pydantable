# Typed DataFrame System — Rules & Design Specification

## Purpose

This document defines the non-negotiable rules, constraints, and architectural patterns required to build a Python DataFrame system that is:

- Statistically type-safe
- Compatible with static type checkers (Pyright, ty)
- Capable of evolving schema through transformations
- Predictable and composable

This is not a wrapper around pandas/Polars.  
This is a typed transformation system with a DataFrame execution backend.

---

## 1. Core Design Principle

All schema changes must be knowable at type-check time.

---

## 2. Hard Constraints (DO NOT VIOLATE)

### 2.1 No Runtime-Dependent Schema Changes
Forbidden:
- df[df.columns[0]]
- df.rename({some_variable: "new_name"})

Required:
- Schema-changing operations must have an explicit, statically-declared output schema.
- Column identity must be represented by typed column tokens (e.g. ``ColumnRef``), not strings.
- With **`DataFrameModel`**, the output type may be a **subclass** of the input model when the new schema **extends** the old one (merged field annotations along the MRO). You still pass that subclass to **`*_as(AfterModel, ...)`** so the evolution stays explicit to type checkers and at runtime.

Example (explicit output schema + typed column tokens):

```python
from pydantable import DataFrame, Schema

class Before(Schema):
    id: int
    name: str
    age: int

class After(Schema):
    id: int
    name: str

df = DataFrame[Before]({"id": [1], "name": ["a"], "age": [10]})
out = df.select_as(After, df.col.id, df.col.name)
```

### 2.2 No Stringly-Typed Column Access
Forbidden:
- df["id"]

Required:
- df.col.id

Notes:
- ``ColumnRef`` values must always be bound to a schema (e.g. via ``df.col.<field>``).
- Avoid helper APIs like ``col(\"id\")`` in strict mode; they invite stringly access and
  weaken schema coupling.

### 2.3 No In-Place Mutation
Forbidden:
- df["age"] = df["age"] + 1

Required:
- df2 = df.with_column(...)

### 2.4 No Arbitrary Python Execution Inside Transformations
Forbidden:
- df.apply(lambda row: ...)

Required:
- Typed expression system

### 2.5 No Unbounded Dynamic Column Sets
Forbidden:
- looping over df.columns to mutate schema

---

## 3. Core Abstractions

### 3.1 Typed DataFrame
TypedDataFrame[SchemaT]

### 3.2 Schema Representation
- Pydantic models (preferred)
- TypedDict (optional)

Requirements:
- Static field names
- Static field types

### 3.3 Column Access Layer
- df.col.id (typed)

---

## 4. Transformation Rules

All transformations must be type-level functions.

### Supported Operations
- select
- rename
- with_column
- drop
- cast
- join
- groupby + agg

All must return a new TypedDataFrame with a new schema.

---

## 5. Expression System

### Requirements
- Expressions must be typed (Expr[int], Expr[str])
- No raw Python operators
- Must be composable and deterministic

---

## 6. Schema Evolution

- Must be deterministic
- Must produce a new schema every time

Recommended:
- Code generation for schemas

---

## 7. Static Typing Requirements

- Prefer typed column tokens over raw strings for column identity
- Avoid raw ``str`` usage for schema-changing operations
- Preserve generics
- Minimize ``Any`` leakage (allowed at I/O and engine boundaries)

---

## 8. API Design Constraints

- Small, strict API
- No hidden schema mutations
- No untyped escape hatch mode in strict 2.0: all transformations must remain typed and
  schema-evolving operations must be explicit (``*_as`` APIs).

---

## 9. Backend Independence

Typing system must be independent of execution engine (Polars, Pandas, SQL, etc.)

---

## 10. Developer Experience Strategy

- Prioritize correctness over flexibility
- Accept reduced ergonomics for safety

---

## 11. Testing Requirements

- Validate typing with Pyright/ty
- Test schema transformations
- Cover edge cases

---

## 12. Summary

Avoid:
- Dynamic columns
- Mutation
- Untyped expressions

Require:
- Typed schema
- Typed transformations
- Deterministic outputs

---

## Final Mental Model

A typed relational algebra engine, not a traditional DataFrame library.
