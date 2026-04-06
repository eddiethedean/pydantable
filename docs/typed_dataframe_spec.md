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
- df.select("id", "name")  # Literal-only

### 2.2 No Stringly-Typed Column Access
Forbidden:
- df["id"]

Required:
- df.col.id
- col("id")  # Literal

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

- Use Literal types for column names
- Avoid raw str usage
- Preserve generics
- No Any leakage

---

## 8. API Design Constraints

- Small, strict API
- No hidden schema mutations
- Provide explicit escape hatch (untyped mode)

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
