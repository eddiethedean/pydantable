# Security policy

## Supported versions

Security fixes are applied to the **current stable 1.x** release line. Check [PyPI](https://pypi.org/project/pydantable/) and [Changelog](https://pydantable.readthedocs.io/en/latest/project/changelog/) for the latest version.

| Version | Supported |
|---------|-----------|
| **1.x** (latest) | Yes |
| **0.x** | No — upgrade to 1.x |
| **2.x** (future) | TBD at release |

## Reporting a vulnerability

**Do not** open a public GitHub issue for security-sensitive reports.

Email the maintainers via the contact address on the [GitHub repository](https://github.com/eddiethedean/pydantable) profile, or use GitHub **Private vulnerability reporting** if enabled on the repo.

Include:

- Affected version(s)
- Steps to reproduce
- Impact assessment (data exposure, RCE, denial of service, etc.)

We aim to acknowledge reports within a reasonable timeframe and coordinate disclosure before a fix release.

## Supply chain

- **License:** MIT (see package metadata)
- **Dependencies:** Core runtime is Pydantic + typing-extensions; the native wheel bundles Polars/Rust code via **`pydantable-native`**
- **SBOMs:** Not generated in CI by default; contributors may produce CycloneDX artifacts locally — see [Developer guide](docs/project/developer.md)

## Application security

Service authors should read [Security considerations](https://pydantable.readthedocs.io/en/latest/project/security.md) for **`trusted_mode`**, path allowlisting, and SQL connection handling.
