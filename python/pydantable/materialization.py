"""Taxonomy for how a lazy :class:`~pydantable.DataFrame` plan is materialized.

PydanTable exposes **four** scheduling / consumption patterns for the same Rust
logical plan. They differ in **how** you invoke the engine and **what** Python
API you use—not in query semantics (see **INTERFACE_CONTRACT** in the docs).

Use :class:`PlanMaterialization` to label modes in logs, tests, or adapters.

Full narrative: **MATERIALIZATION** doc page.
"""

from __future__ import annotations

from enum import Enum


class PlanMaterialization(str, Enum):
    """How terminal materialization of a lazy plan is scheduled and consumed.

    Values are **lowercase strings** suitable for stable logging and JSON.

    **Blocking**

        Synchronous APIs on the **current thread**:
        :meth:`~pydantable.DataFrame.collect`,
        :meth:`~pydantable.DataFrame.to_dict`,
        :meth:`~pydantable.DataFrame.to_polars`,
        :meth:`~pydantable.DataFrame.to_arrow`,
        :meth:`~pydantable.DataFrame.collect_batches`, etc.

    **Async**

        ``await`` coroutines so the event loop can run other tasks while the
        engine is awaited (or work is offloaded to a thread pool when the wheel
        does not expose native async execution): :meth:`~pydantable.DataFrame.acollect`,
        :meth:`~pydantable.DataFrame.ato_dict`,
        :meth:`~pydantable.DataFrame.ato_polars`,
        :meth:`~pydantable.DataFrame.ato_arrow` (and :class:`~pydantable.DataFrameModel`
        mirrors such as ``arows`` / ``ato_dicts``).

    **Deferred**

        Start materialization in the background and await the result later:
        :meth:`~pydantable.DataFrame.submit` returns a
        :class:`~pydantable.dataframe.ExecutionHandle`; ``await handle.result()``
        matches :meth:`~pydantable.DataFrame.collect` for the same arguments.

    **Chunked**

        After **one** full engine collect, yield **column dict** chunks (same slicing
        contract as :meth:`~pydantable.DataFrame.collect_batches`): **sync**
        :meth:`~pydantable.DataFrame.stream` or **async**
        :meth:`~pydantable.DataFrame.astream`. Not out-of-core Polars streaming;
        see **EXECUTION** (Rust engine) in the docs.
    """

    BLOCKING = "blocking"
    ASYNC = "async"
    DEFERRED = "deferred"
    CHUNKED = "chunked"


__all__ = ["PlanMaterialization", "plan_materialization_summary"]

# Stable one-line labels for each mode (docs, tests, tooling).
_PLAN_MATERIALIZATION_SUMMARY: dict[PlanMaterialization, str] = {
    PlanMaterialization.BLOCKING: (
        "Synchronous collect / to_* / collect_batches on the current thread."
    ),
    PlanMaterialization.ASYNC: (
        "Async acollect / ato_* / arows; awaitable engine or thread pool."
    ),
    PlanMaterialization.DEFERRED: (
        "submit() → ExecutionHandle; await result() like collect()."
    ),
    PlanMaterialization.CHUNKED: (
        "stream() / astream(); dict[str, list] batches after one collect."
    ),
}


def plan_materialization_summary(kind: PlanMaterialization) -> str:
    """Return a short English description of the given materialization mode."""

    return _PLAN_MATERIALIZATION_SUMMARY[kind]
