"""Protocols and capability descriptors for execution engines.

Re-exports structural protocols from :mod:`pydantable_protocol` (no ``pydantable``
dependency for third-party engines). :func:`native_engine_capabilities` delegates to
``pydantable-native`` when that distribution is installed.
"""

from __future__ import annotations

from pydantable_protocol.protocols import (
    EngineCapabilities,
    ExecutionEngine,
    PlanExecutor,
    SinkWriter,
    stub_engine_capabilities,
)

__all__ = [
    "EngineCapabilities",
    "ExecutionEngine",
    "PlanExecutor",
    "SinkWriter",
    "native_engine_capabilities",
    "stub_engine_capabilities",
]


def native_engine_capabilities() -> EngineCapabilities:
    """Capabilities for the current native extension load, if any."""
    try:
        from pydantable_native.capabilities import (  # type: ignore[import-not-found]
            native_engine_capabilities as _native_engine_capabilities,
        )
    except (ImportError, ModuleNotFoundError):
        return EngineCapabilities(
            backend="native",
            extension_loaded=False,
            has_execute_plan=False,
            has_async_execute_plan=False,
            has_async_collect_plan_batches=False,
            has_sink_parquet=False,
            has_sink_csv=False,
            has_sink_ipc=False,
            has_sink_ndjson=False,
            has_collect_plan_batches=False,
            has_execute_join=False,
            has_execute_groupby_agg=False,
        )
    return _native_engine_capabilities()
