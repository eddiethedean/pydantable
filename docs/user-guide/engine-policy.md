# Engine policy (v2)

PydanTable v2 supports **multi-engine workflows** (SQL / Mongo / Spark / native) with two goals:

- **Execution stays in the most logical place** (prefer the source-matched engine).
- **Users keep full control and visibility** over where work runs and when handoff occurs.

## The `execution_policy` knob

Terminal methods like `collect()` and `to_dict()` accept `execution_policy=`:

- **`"fallback_to_native"` (default)**: best-effort execution. If the current engine cannot execute the plan, pydantable will **fall back** to the native Rust/Polars engine (with a warning).
- **`"error_on_fallback"`**: strict mode. If the current engine cannot execute the plan, pydantable raises an actionable error telling you how to force an explicit handoff.
- **`"pushdown"`**: strict “stay here” mode (synonym-ish for strictness). Use this in tests or production guardrails to ensure computation does not move engines implicitly.

Example:

```python
from pydantable import DataFrame, ExecutionPolicy, Schema

class Row(Schema):
    id: int
    amount: float

df = DataFrame[Row]({"id": [1, 2], "amount": [10.0, 25.0]}).filter(lambda r: r.amount > 10)

out = df.to_dict(execution_policy="pushdown")  # or "error_on_fallback"
```

## Explicit handoff (always supported)

Engine boundaries are explicit and observable:

- `df.to_native(...)`
- `df.to_engine(target_engine, ...)`
- convenience helpers such as `df.to_sql_engine(...)`

## Visibility APIs

Two helpers let you see what pydantable thinks will happen:

- `df.engine_report()` / `model.engine_report()`
- `df.explain_execution()` / `model.explain_execution()`

They return small dicts designed to be stable across versions and easy to log in services.

