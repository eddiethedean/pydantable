"""Mongo helpers for [Beanie](https://github.com/BeanieODM/beanie) ``Document`` models.

This is the **recommended** integration surface for MongoDB: define **Beanie**
models, then use :func:`sync_pymongo_collection` and
:meth:`pydantable.mongo_entei.EnteiDataFrame.from_beanie`.

`Beanie <https://beanie-odm.dev/>`_ drives collections through PyMongo's **async**
API (``AsyncMongoClient``, ``AsyncDatabase``, ``AsyncCollection``). Pydantable's
lazy :class:`~pydantable.mongo_entei.EnteiDataFrame` and eager helpers in
:mod:`pydantable.io.mongo` use **sync** ``pymongo.collection.Collection``
(``find()``, ``insert_many``, …).

Use :func:`sync_pymongo_collection` with a **synchronous**
:class:`pymongo.database.Database` that points at the **same database name** as your
Beanie app (typically ``MongoClient(uri)[name]`` alongside ``AsyncMongoClient(uri)``
for Beanie). This module does **not** import Beanie at runtime; any document class
with a ``get_collection_name()`` classmethod (Beanie ``Document``) works.

Install **beanie** with the **mongo** extra: ``pip install "pydantable[mongo]"``.
At runtime :func:`sync_pymongo_collection` only needs **pymongo** (tests may use
**mongomock**); it does not import Beanie.
"""

from __future__ import annotations

import importlib
from typing import Any


def _is_sync_database(database: Any) -> bool:
    """True for real PyMongo ``Database`` and for ``mongomock`` test doubles."""
    try:
        pymongo = importlib.import_module("pymongo")
        database_mod = getattr(pymongo, "database", None)
        Database = getattr(database_mod, "Database", None)
        if Database is not None and isinstance(database, Database):
            return True
    except ImportError:
        pass
    mod = type(database).__module__
    return mod.startswith("mongomock") and hasattr(database, "__getitem__")


def sync_pymongo_collection(document_cls: type[Any], database: Any) -> Any:
    """Resolve a **sync** PyMongo ``Collection`` for a Beanie document model.

    Parameters
    ----------
    document_cls:
        A class implementing ``get_collection_name() -> str``, e.g. a Beanie
        :class:`beanie.odm.documents.Document` subclass after ``init_beanie``.
    database:
        A **sync** :class:`pymongo.database.Database` (``MongoClient(...).db_name``).

    Returns
    -------
    pymongo.collection.Collection
        ``database[get_collection_name()]``.

    Raises
    ------
    TypeError
        If ``database`` is not a sync ``Database`` or ``document_cls`` has no
        ``get_collection_name``.
    """
    if not _is_sync_database(database):
        raise TypeError(
            "database must be a sync pymongo.database.Database "
            "(e.g. MongoClient(uri).mydb), not pymongo AsyncDatabase / Beanie async DB."
        )
    get_name = getattr(document_cls, "get_collection_name", None)
    if get_name is None:
        raise TypeError(
            "document_cls must define get_collection_name() (Beanie Document)."
        )
    col_name = get_name()
    if not isinstance(col_name, str) or not col_name:
        raise TypeError("get_collection_name() must return a non-empty str.")
    return database[col_name]


__all__ = ["sync_pymongo_collection"]
