from __future__ import annotations

import io
import logging
import os
import shutil
import tarfile
import urllib.request
from pathlib import Path

_log = logging.getLogger(__name__)

# Default: GitHub archive of main (README + docs/ tree).
_DEFAULT_TARBALL = (
    "https://github.com/eddiethedean/pydantable/archive/refs/heads/main.tar.gz"
)

# Under service root: bundled/pydantable/{README.md,docs/}
_BUNDLE_REL = Path("bundled") / "pydantable"


def bundled_pydantable_root(repo_root: Path) -> Path:
    return (repo_root / _BUNDLE_REL).resolve()


def upstream_tarball_url() -> str:
    return os.getenv("RAG_UPSTREAM_TARBALL_URL", _DEFAULT_TARBALL).strip()


def fetch_enabled() -> bool:
    """Opt-in: production should use a CI-built DB (see build_index_ci.py)."""
    v = os.getenv("RAG_FETCH_UPSTREAM_DOCS")
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "on"}


def fetch_upstream_docs(*, repo_root: Path, url: str | None = None) -> bool:
    """
    Download the upstream tarball and place ``README.md`` and ``docs/`` under
    ``<repo_root>/bundled/pydantable/``.

    Returns True if files were written, False on error (logged).
    """
    url = url or upstream_tarball_url()
    dest = bundled_pydantable_root(repo_root)
    tmp = repo_root / ".upstream_fetch_tmp"

    try:
        _log.info("pydantable-rag: fetching upstream docs from %s", url)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "pydantable-rag-upstream-fetch/1"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()

        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tf:
            members = tf.getmembers()
            if not members:
                _log.warning("pydantable-rag: empty tarball")
                return False

            prefix = ""
            for m in members:
                if m.isfile() and m.name.endswith("/README.md"):
                    prefix = m.name[: -len("README.md")]
                    break
            if not prefix:
                prefix = f"{members[0].name.split('/')[0]}/"

            readme = f"{prefix}README.md"
            docs_prefix = f"{prefix}docs/"

            if tmp.exists():
                shutil.rmtree(tmp)
            tmp.mkdir(parents=True)

            readme_ok = False
            docs_n = 0
            for m in members:
                name = m.name
                if name == readme and m.isfile():
                    f = tf.extractfile(m)
                    if f is None:
                        continue
                    (tmp / "README.md").write_bytes(f.read())
                    readme_ok = True
                elif name.startswith(docs_prefix) and name != docs_prefix.rstrip("/"):
                    if m.isdir():
                        continue
                    rel = name[len(docs_prefix) :]
                    if not rel:
                        continue
                    f = tf.extractfile(m)
                    if f is None:
                        continue
                    out_path = tmp / "docs" / rel
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_bytes(f.read())
                    docs_n += 1

            if docs_n == 0:
                _log.warning(
                    "pydantable-rag: no docs/ tree in archive (prefix=%r)", prefix
                )
                shutil.rmtree(tmp, ignore_errors=True)
                return False
            if not readme_ok:
                _log.info("pydantable-rag: archive had no README.md; docs only")

            if dest.exists():
                shutil.rmtree(dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(tmp), str(dest))
            _log.info("pydantable-rag: upstream docs installed at %s", dest)
            return True
    except Exception:
        _log.exception("pydantable-rag: upstream doc fetch failed")
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        return False


def ensure_upstream_bundle(repo_root: Path) -> None:
    """If enabled and bundle missing, download upstream README + docs/."""
    if not fetch_enabled():
        return
    docs_dir = bundled_pydantable_root(repo_root) / "docs"
    if docs_dir.is_dir():
        return
    fetch_upstream_docs(repo_root=repo_root)
