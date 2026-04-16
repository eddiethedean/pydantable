# RFC: Finer-grained engine protocols (interface segregation)

## Status

**Draft / not implemented.** This document records a possible future direction if a real second backend needs a **smaller** surface than the full **`ExecutionEngine`** protocol in **`pydantable-protocol`**.

## Context

Today, **`PlanExecutor`**, **`SinkWriter`**, and **`ExecutionEngine`** (composition of the two plus plan builders) live in **`pydantable-protocol`**. Implementations that only need execution can still satisfy **`ExecutionEngine`** by raising **`UnsupportedEngineOperationError`** for sinks or unsupported plan ops—see [CUSTOM_ENGINE_PACKAGE](../../integrations/engines/custom-engine-package.md).

## Problem

A backend that **never** writes Parquet/CSV/IPC/NDJSON must nonetheless implement or delegate sink methods if it wants to type-check as **`ExecutionEngine`**, or it documents “always raises” and accepts the wide protocol.

## Proposal (breaking, cross-package)

1. **Optional runtime protocols** — e.g. split **`AsyncPlanExecutor`** only if async execution is optional and callers use **`EngineCapabilities`** + **`isinstance`** checks before **`async_execute_plan`**.
2. **Versioned protocol module** — e.g. **`pydantable_protocol.protocols_v2`** with slimmer types; **`pydantable`** continues to re-export v1 until a major release.
3. **Narrow imports for authors** — third-party packages import only **`PlanExecutor`** / **`SinkWriter`** when defining adapters; **`ExecutionEngine`** remains the “full” façade for **`DataFrame`**.

## Preconditions

Do **not** undertake this without:

- At least one **concrete** non-native engine that benefits (smaller stubs, clearer errors, or static typing wins).
- Coordinated releases of **`pydantable-protocol`**, **`pydantable-native`**, and **`pydantable`**, plus a migration note in the changelog.

## Related

- [ADR-engines](../../project/adrs/engines.md)
- [CUSTOM_ENGINE_PACKAGE](../../integrations/engines/custom-engine-package.md)
