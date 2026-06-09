# Installation

Install **pydantable** from PyPI for normal use. The published wheel pulls in
**pydantable-protocol** (engine protocols) and **pydantable-native** (Rust + Polars
extension) automatically.

## Requirements

| Requirement | Details |
|-------------|---------|
| **Python** | **3.10–3.13** (`requires-python >=3.10` in package metadata; CI exercises all four) |
| **Native extension** | Shipped inside **`pydantable-native`** wheels on supported platforms |
| **Core runtime deps** | **Pydantic 2.x**, **typing-extensions** — no Polars package required for basic use |

## PyPI install

```bash
pip install pydantable
```

Pin a release when you need reproducible deploys:

```bash
pip install "pydantable==1.19.2"
```

That resolves matching **`pydantable-protocol`** and **`pydantable-native`** versions from PyPI.

## Verify your install

Run this after `pip install` (or in CI smoke tests):

```python
import pydantable

pydantable.DataFrameModel  # import check
from pydantable.engine import native_engine_capabilities

caps = native_engine_capabilities()
assert caps.extension_loaded, (
    "Native extension missing — see Troubleshooting / build from source below"
)
print("pydantable", pydantable.__version__, "native OK")
```

If verification fails with **`MissingRustExtensionError`**, see [Troubleshooting](troubleshooting.md).

## Optional extras

Install only what your app needs:

| Extra | Install | Use when |
|-------|---------|----------|
| **`polars`** | `pip install "pydantable[polars]"` | **`to_polars()`** / **`ato_polars()`** |
| **`arrow`** | `pip install "pydantable[arrow]"` | **`to_arrow()`** / Arrow constructors |
| **`io`** | `pip install "pydantable[io]"` | Full file I/O convenience (arrow + polars) |
| **`sql`** | `pip install "pydantable[sql]"` | SQLModel I/O, lazy **`SqlDataFrame`** — add a DB-API driver for your URL |
| **`mongo`** | `pip install "pydantable[mongo]"` | PyMongo + Beanie + lazy **`MongoDataFrame`** |
| **`spark`** | `pip install "pydantable[spark]"` | **`SparkDataFrame`** (PySpark 3.4–3.x, Java required) |
| **`fastapi`** | `pip install "pydantable[fastapi]"` | **`pydantable.fastapi`** helpers |
| **`pandas`** | `pip install "pydantable[pandas]"` | **`pydantable.pandas`** façade |
| **`cloud`** | `pip install "pydantable[cloud]"` | **`s3://`** and fsspec object-store reads |
| **`excel`** | `pip install "pydantable[excel]"` | Excel via **`iter_excel`** |
| **`kafka`** | `pip install "pydantable[kafka]"` | Kafka JSON iterators |
| **`bq`** | `pip install "pydantable[bq]"` | BigQuery iterator |
| **`snowflake`** | `pip install "pydantable[snowflake]"` | Snowflake iterator |

Terminology for I/O entrypoints: [Glossary](../concepts/glossary.md) and [I/O decision tree](../io/decision-tree.md).

## Platform / wheel matrix

Wheels are built in the [release workflow](https://github.com/eddiethedean/pydantable/blob/main/.github/workflows/release.yml) for common CPython targets:

| Platform | Typical wheel tag | Notes |
|----------|-------------------|-------|
| **Linux x86_64** | `manylinux_2_17_x86_64`, `manylinux_2_28_x86_64` | Primary CI and PyPI publish path |
| **Linux aarch64** | `manylinux_*_aarch64` | Built in release matrix |
| **macOS** | `macosx_*_x86_64`, `macosx_*_arm64` | Apple Silicon and Intel |
| **Windows** | `win_amd64` | Spark tests skipped on Windows CI (JVM constraints) |
| **musl Linux** | `musllinux_*` | Alpine-style targets |

If no wheel exists for your platform/Python combo, **`pip`** may try to build from source (slow; requires **Rust** + **maturin**). Prefer a supported matrix cell or build locally (below).

## Source checkout (contributors)

From a git clone you must build the native extension once:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip setuptools wheel
.venv/bin/python -m pip install -e ./pydantable-protocol
.venv/bin/python -m pip install -e ".[dev]"
(cd pydantable-native && maturin develop --manifest-path ../pydantable-core/Cargo.toml)
```

Or use the Makefile shortcut:

```bash
make dev-setup
```

Full contributor setup: [Developer guide](../project/developer.md).

## Three-package layout (evaluators)

| Package | Role |
|---------|------|
| **`pydantable`** | Public Python API (DataFrame, I/O, integrations) |
| **`pydantable-native`** | Compiled Rust/Polars engine (`pydantable_native._core`) |
| **`pydantable-protocol`** | Zero-dep **`ExecutionEngine`** protocols for third-party backends |

End users normally install only **`pydantable`**. Custom engine authors may depend on **`pydantable-protocol`** alone — see [Custom engine package](../integrations/engines/custom-engine-package.md).

## Next steps

- [Five-minute tour](quickstart.md) — first typed pipeline
- [Troubleshooting](troubleshooting.md) — **`MissingRustExtensionError`**, shape/order surprises
- [Migration](migration.md) — upgrading from older 1.x or deprecated names
