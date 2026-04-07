from __future__ import annotations


class MissingPlanFrameError(ImportError):
    pass


def require_planframe() -> None:
    try:
        import planframe as _  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise MissingPlanFrameError(
            "PlanFrame is required for this feature. "
            "Install pydantable (it depends on planframe)."
        ) from e
