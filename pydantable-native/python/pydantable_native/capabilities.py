"""Native extension capability flags (no pydantable dependency)."""

from __future__ import annotations

from pydantable_protocol.protocols import EngineCapabilities

from ._binding import rust_core_loaded


def native_engine_capabilities() -> EngineCapabilities:
    """Capabilities for the current native extension load, if any."""
    rc = rust_core_loaded()
    if rc is None:
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
    return EngineCapabilities(
        backend="native",
        extension_loaded=True,
        has_execute_plan=hasattr(rc, "execute_plan"),
        has_async_execute_plan=hasattr(rc, "async_execute_plan"),
        has_async_collect_plan_batches=hasattr(rc, "async_collect_plan_batches"),
        has_sink_parquet=hasattr(rc, "sink_parquet"),
        has_sink_csv=hasattr(rc, "sink_csv"),
        has_sink_ipc=hasattr(rc, "sink_ipc"),
        has_sink_ndjson=hasattr(rc, "sink_ndjson"),
        has_collect_plan_batches=hasattr(rc, "collect_plan_batches"),
        has_execute_join=hasattr(rc, "execute_join"),
        has_execute_groupby_agg=hasattr(rc, "execute_groupby_agg"),
    )
