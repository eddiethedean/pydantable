from typing import List, MutableMapping, Optional

from sqlite_utils import Database
from sqlite_utils.db import Table

from tinytable.functional.rows import iterrows, row_dicts_to_data


def get_table_names(path: str) -> List[str]:
    return Database(path).table_names()


def read_sqlite_table(path: str, table_name: str) -> dict:
    db = Database(path)
    return row_dicts_to_data(list(db[table_name].rows))


def data_to_sqlite_table(
    data: MutableMapping,
    path: str,
    table_name: str,
    primary_key: Optional[str] = None,
    replace_table: bool = False,
    append_records = False
) -> None:
    """
    Create Sqlite Table and insert data.
    Error if table_name already exists.
    
    Set primary_key to a column name to create primary key.
    
    Set replace_table = True to drop table then
    create new table based on data.
    
    Set append_records = True to insert records
    into existing table.
    """
    db = Database(path)
    records = [d for _, d in iterrows(data)]
    table = db.table(table_name)
    if table.exists() and not replace_table and not append_records:
        raise ValueError(f'Table {table_name} already exists.')
    if isinstance(table, Table):
        if replace_table and table.exists():
            table.drop()
        table.insert_all(records, pk=primary_key)  # type: ignore

