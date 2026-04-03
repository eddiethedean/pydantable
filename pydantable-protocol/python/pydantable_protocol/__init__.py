"""Zero-dependency protocols for pydantable-compatible execution engines."""

from __future__ import annotations

from pydantable_protocol.exceptions import (
    MissingRustExtensionError,
    UnsupportedEngineOperationError,
)
from pydantable_protocol.protocols import (
    EngineCapabilities,
    ExecutionEngine,
    PlanExecutor,
    SinkWriter,
    stub_engine_capabilities,
)

__version__ = "1.14.1"

__all__ = [
    "EngineCapabilities",
    "ExecutionEngine",
    "MissingRustExtensionError",
    "PlanExecutor",
    "SinkWriter",
    "UnsupportedEngineOperationError",
    "__version__",
    "stub_engine_capabilities",
]
