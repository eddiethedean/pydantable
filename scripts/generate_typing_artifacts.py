from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from pathlib import Path


def _write_if_changed(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing == content:
        return
    path.write_text(content, encoding="utf-8")


def _differs(path: Path, content: str) -> bool:
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    return existing != content


def _normalize_stub_content(repo_root: Path, path: Path, content: str) -> str:
    """
    Lint-fix and format generated stub text the same way for `--check` and for writes.

    Uses the file's path relative to the repo so Ruff applies the same `pyproject`
    rules as `ruff check` / `ruff format` on committed paths (tempfile-based runs
    can diverge).
    """
    rel = str(path.relative_to(repo_root))
    try:
        check_proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "ruff",
                "check",
                "--fix-only",
                "--stdin-filename",
                rel,
                "-",
            ],
            cwd=repo_root,
            input=content,
            text=True,
            capture_output=True,
            check=False,
        )
        # fix-only: exit 0 even if only the formatter can clean up (e.g. line length).
        checked = check_proc.stdout if check_proc.returncode == 0 else content
        fmt_proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "ruff",
                "format",
                "--stdin-filename",
                rel,
                "-",
            ],
            cwd=repo_root,
            input=checked,
            text=True,
            capture_output=True,
            check=False,
        )
        return fmt_proc.stdout if fmt_proc.returncode == 0 else checked
    except OSError:
        return content


def _render_init_stub(init_py: Path) -> str:
    """
    Generate `__init__.pyi` from runtime `__init__.py`.

    This is intentionally conservative: we mirror *only* imports/exports that define
    the public API (`__all__`) to avoid drift and reduce pyright/Pylance noise.
    """
    src = init_py.read_text(encoding="utf-8")
    mod = ast.parse(src, filename=str(init_py))

    all_names: list[str] | None = None
    version_value: str | None = None
    import_nodes: list[ast.stmt] = []

    for node in mod.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(alias.name == "annotations" for alias in node.names)
            ):
                continue
            import_nodes.append(node)
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "__all__":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, list) and all(isinstance(x, str) for x in value):
                    all_names = list(value)
            if isinstance(target, ast.Name) and target.id == "__version__":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, str):
                    version_value = value

    if all_names is None:
        raise SystemExit(f"Could not statically parse __all__ from {init_py}")

    imports_src = "\n".join(ast.unparse(n) for n in import_nodes).strip()
    # Emit `__all__` deterministically without relying on synthesized AST locations.
    all_src = f"__all__ = {all_names!r}"

    lines: list[str] = ["from __future__ import annotations", ""]
    if imports_src:
        lines.append(imports_src)
        lines.append("")
    if version_value is not None:
        lines.append(f"__version__ = {version_value!r}")
        lines.append("")
    # Keep explicit __all__ for editor/typing tooling parity.
    lines.append(all_src)
    lines.append("")
    return "\n".join(lines)


def _stubify_function(fn: ast.AST) -> ast.AST:
    fn2 = ast.fix_missing_locations(ast.parse(ast.unparse(fn)).body[0])
    assert isinstance(fn2, (ast.FunctionDef, ast.AsyncFunctionDef))
    # Keep only decorators that affect the signature shape in stubs.
    keep: list[ast.expr] = []
    for d in fn2.decorator_list:
        name: str | None = None
        if isinstance(d, ast.Name):
            name = d.id
        elif isinstance(d, ast.Attribute):
            name = d.attr
        if name in {"property", "classmethod", "staticmethod", "overload"}:
            keep.append(d)
    fn2.decorator_list = keep

    # Avoid mypy override errors for operator overloads in stubs.
    if fn2.name in {"__eq__", "__ne__"}:
        fn2.returns = ast.Name(id="Any", ctx=ast.Load())
    # Remove docstring if present.
    if (
        fn2.body
        and isinstance(fn2.body[0], ast.Expr)
        and isinstance(fn2.body[0].value, ast.Constant)
        and isinstance(fn2.body[0].value.value, str)
    ):
        fn2.body = fn2.body[1:]
    fn2.body = [ast.Expr(value=ast.Constant(value=Ellipsis))]
    return fn2


def _stubify_class(cls: ast.ClassDef) -> ast.ClassDef:
    # Clone via roundtrip to detach locations, then rebuild body.
    cls2 = ast.fix_missing_locations(ast.parse(ast.unparse(cls)).body[0])
    assert isinstance(cls2, ast.ClassDef)
    new_body: list[ast.stmt] = []
    if cls2.name == "Expr":
        new_body.append(
            ast.AnnAssign(
                target=ast.Name(id="_rust_expr", ctx=ast.Store()),
                annotation=ast.Name(id="Any", ctx=ast.Load()),
                value=None,
                simple=1,
            )
        )
    for node in cls2.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            new_body.append(_stubify_function(node))  # type: ignore[arg-type]
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            # Keep simple attribute declarations.
            new_body.append(node)
        elif isinstance(node, ast.Pass):
            continue
        elif (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            # Drop docstring.
            continue
        elif isinstance(node, ast.ClassDef):
            # Nested helpers (e.g. pandas façade `_ILoc` / `_Rolling`); stubs must
            # include them so return types like `-> _ModelRolling` resolve.
            new_body.append(_stubify_class(node))
        else:
            # Drop complex statements; stubs focus on signatures.
            continue
    if not new_body:
        new_body = [ast.Expr(value=ast.Constant(value=Ellipsis))]
    cls2.body = new_body
    return cls2


def _parse_all_names(mod: ast.Module) -> list[str] | None:
    for node in mod.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            t = node.targets[0]
            if isinstance(t, ast.Name) and t.id == "__all__":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, list) and all(isinstance(x, str) for x in value):
                    return list(value)
    return None


def _render_module_stub(
    module_py: Path,
    *,
    include_all_public_defs: bool = False,
    include_private_defs: bool = False,
) -> str:
    src = module_py.read_text(encoding="utf-8")
    mod = ast.parse(src, filename=str(module_py))
    all_names = _parse_all_names(mod)
    if all_names is None:
        raise SystemExit(f"Could not statically parse __all__ from {module_py}")

    import_nodes: list[ast.stmt] = []
    defs: dict[str, ast.stmt] = {}
    ordered: list[str] = []

    for node in mod.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(alias.name == "annotations" for alias in node.names)
            ):
                continue
            import_nodes.append(node)
        elif (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "TYPE_CHECKING"
        ):
            for inner in node.body:
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    import_nodes.append(inner)
        elif isinstance(node, ast.ClassDef):
            if node.name in all_names or (
                include_all_public_defs
                and (include_private_defs or not node.name.startswith("_"))
            ):
                defs[node.name] = _stubify_class(node)
                if node.name not in ordered:
                    ordered.append(node.name)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in all_names or (
                include_all_public_defs
                and (include_private_defs or not node.name.startswith("_"))
            ):
                defs[node.name] = _stubify_function(node)
                if node.name not in ordered:
                    ordered.append(node.name)
        elif isinstance(node, ast.Assign):
            # export constant aliases (e.g. TypeAlias-like variables) when requested
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                name = node.targets[0].id
                if name in all_names or (
                    include_all_public_defs
                    and include_private_defs
                    and name.startswith("_")
                ):
                    defs[name] = node
                    if name not in ordered:
                        ordered.append(name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name = node.target.id
            if name in all_names or (
                include_all_public_defs
                and include_private_defs
                and name.startswith("_")
            ):
                defs[name] = node
                if name not in ordered:
                    ordered.append(name)

    imports_src = "\n".join(ast.unparse(n) for n in import_nodes).strip()
    lines: list[str] = ["from __future__ import annotations", ""]
    if imports_src:
        lines.append(imports_src)
        lines.append("")
    if include_all_public_defs:
        for name in ordered:
            node = defs.get(name)
            if node is None:
                continue
            lines.append(ast.unparse(node).rstrip())
            lines.append("")
    else:
        # Emit defs in a stable order: exported names in __all__ order.
        for name in all_names:
            node = defs.get(name)
            if node is None:
                # It's still useful for re-export-only modules to have __all__ entries.
                continue
            lines.append(ast.unparse(node).rstrip())
            lines.append("")
    lines.append(f"__all__ = {all_names!r}")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """
    Keep committed typing artifacts in sync.

    Typing artifacts are committed and generated deterministically to prevent drift.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated artifacts would change (do not write).",
    )
    args = parser.parse_args(argv)

    repo = Path(__file__).resolve().parents[1]
    pkg = repo / "python" / "pydantable"
    stub_pkg = repo / "typings" / "pydantable"

    init_stub = _render_init_stub(pkg / "__init__.py")
    dataframe_init_stub = _render_init_stub(pkg / "dataframe" / "__init__.py")
    schema_init_stub = _render_init_stub(pkg / "schema" / "__init__.py")
    io_init_stub = _render_module_stub(
        pkg / "io" / "__init__.py",
        include_all_public_defs=True,
        include_private_defs=True,
    )
    pyspark_init_stub = _render_init_stub(pkg / "pyspark" / "__init__.py")
    pyspark_sql_init_stub = _render_init_stub(pkg / "pyspark" / "sql" / "__init__.py")

    expressions_stub = _render_module_stub(
        pkg / "expressions.py",
        include_all_public_defs=True,
        include_private_defs=True,
    )
    display_stub = _render_module_stub(pkg / "display.py")
    observe_stub = _render_module_stub(pkg / "observe.py")
    window_spec_stub = _render_module_stub(
        pkg / "window_spec.py",
        include_all_public_defs=True,
        include_private_defs=True,
    )
    pandas_stub = _render_module_stub(pkg / "pandas.py", include_all_public_defs=True)
    pyspark_sql_functions_stub = _render_module_stub(
        pkg / "pyspark" / "sql" / "functions.py"
    )
    pyspark_sql_window_stub = _render_module_stub(pkg / "pyspark" / "sql" / "window.py")
    pyspark_sql_column_stub = _render_module_stub(pkg / "pyspark" / "sql" / "column.py")

    committed_dataframe_model = (pkg / "dataframe_model.pyi").read_text(
        encoding="utf-8"
    )
    committed_awaitable_dataframe_model = (
        pkg / "awaitable_dataframe_model.pyi"
    ).read_text(encoding="utf-8")

    targets: list[tuple[Path, str]] = [
        (pkg / "__init__.pyi", init_stub),
        (pkg / "dataframe_model.pyi", committed_dataframe_model),
        (
            pkg / "awaitable_dataframe_model.pyi",
            committed_awaitable_dataframe_model,
        ),
        (pkg / "dataframe" / "__init__.pyi", dataframe_init_stub),
        (pkg / "schema" / "__init__.pyi", schema_init_stub),
        (pkg / "io" / "__init__.pyi", io_init_stub),
        (pkg / "pyspark" / "__init__.pyi", pyspark_init_stub),
        (pkg / "pyspark" / "sql" / "__init__.pyi", pyspark_sql_init_stub),
        (pkg / "expressions.pyi", expressions_stub),
        (pkg / "display.pyi", display_stub),
        (pkg / "observe.pyi", observe_stub),
        (pkg / "window_spec.pyi", window_spec_stub),
        (pkg / "pandas.pyi", pandas_stub),
        (pkg / "pyspark" / "sql" / "functions.pyi", pyspark_sql_functions_stub),
        (pkg / "pyspark" / "sql" / "window.pyi", pyspark_sql_window_stub),
        (pkg / "pyspark" / "sql" / "column.pyi", pyspark_sql_column_stub),
        (stub_pkg / "__init__.pyi", init_stub),
        (stub_pkg / "dataframe_model.pyi", committed_dataframe_model),
        (
            stub_pkg / "awaitable_dataframe_model.pyi",
            committed_awaitable_dataframe_model,
        ),
        (stub_pkg / "dataframe" / "__init__.pyi", dataframe_init_stub),
        (stub_pkg / "schema" / "__init__.pyi", schema_init_stub),
        (stub_pkg / "io" / "__init__.pyi", io_init_stub),
        (stub_pkg / "pyspark" / "__init__.pyi", pyspark_init_stub),
        (stub_pkg / "pyspark" / "sql" / "__init__.pyi", pyspark_sql_init_stub),
        (stub_pkg / "expressions.pyi", expressions_stub),
        (stub_pkg / "display.pyi", display_stub),
        (stub_pkg / "observe.pyi", observe_stub),
        (stub_pkg / "window_spec.pyi", window_spec_stub),
        (stub_pkg / "pandas.pyi", pandas_stub),
        (stub_pkg / "pyspark" / "sql" / "functions.pyi", pyspark_sql_functions_stub),
        (stub_pkg / "pyspark" / "sql" / "window.pyi", pyspark_sql_window_stub),
        (stub_pkg / "pyspark" / "sql" / "column.pyi", pyspark_sql_column_stub),
    ]

    formatted_targets = [(p, _normalize_stub_content(repo, p, c)) for (p, c) in targets]

    if args.check:
        changed = [
            str(p.relative_to(repo)) for (p, c) in formatted_targets if _differs(p, c)
        ]
        if changed:
            print("Typing artifacts are out of date. Re-run:")
            print("  python scripts/generate_typing_artifacts.py")
            print("Changed:")
            for p in changed:
                print(f"  - {p}")
            return 1
    else:
        for p, c in formatted_targets:
            if str(p).startswith(str(pkg)):
                _write_if_changed(p, c)
    (pkg / "py.typed").parent.mkdir(parents=True, exist_ok=True)
    (pkg / "py.typed").touch(exist_ok=True)

    if not args.check:
        for p, c in formatted_targets:
            if str(p).startswith(str(stub_pkg)):
                _write_if_changed(p, c)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
