# Pydantable Async Execution Design

## Overview
This document defines the async execution model for Pydantable, a Rust-backed DataFrame engine with Python bindings.

## Core Principle
Async is used for orchestration, not computation. Computation is executed in Rust using parallel execution.

## API Design

### collect()
Blocking execution.

### acollect()
Async execution returning an awaitable.

### submit()
Fire-and-forget execution returning a handle.

## Execution Flow

1. Python builds logical plan
2. Plan passed to Rust engine
3. Rust executes using Rayon thread pool
4. Python receives Future-like handle
5. acollect awaits completion

## Rust-Python Bridge

- Use PyO3 for bindings
- Use pyo3-asyncio for async bridge
- Rust returns Future mapped to Python awaitable

## Example

```python
result = await df.acollect()
```

## Future Extensions

- Streaming via astream()
- Backpressure support
- Distributed execution
