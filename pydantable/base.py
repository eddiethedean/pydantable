from __future__ import annotations
import typing as _t
import pydantic as _pydantic
import tinytable as _tt
import tinytable.csv
import tinytim.rows

from pydantable import errors


def validate_row(
    row: dict,
    model: _t.Type[_pydantic.BaseModel]
) -> dict:
    return dict(model.model_validate(dict(row)))


def validate_table(
    table: _tt.Table,
    model: _t.Type[_pydantic.BaseModel]
) -> _tt.Table:
    out_table: _tt.Table = table.copy()
    validation_errors: list[dict] = []
    errored = False
    for i, row in table.iterrows():
        try:
            validated_row: dict = validate_row(dict(row), model)
            if not errored:
                out_table[i] = validated_row
        except _pydantic.ValidationError as e:
            validation_errors.extend(e.errors())
            errored = True
    if validation_errors:
        grouped_errors: list[dict] = errors.group_errors(validation_errors)
        raise errors.ValidationErrors(grouped_errors)
    return out_table
    

class BaseTableModel(_pydantic.BaseModel):
    # TODO: Add __init__ for reading dict
    
    @classmethod
    def read_csv(cls, path: str) -> _tt.Table:
        tbl: _tt.Table = _tt.read_csv(path)
        return validate_table(tbl, cls)
    
    @classmethod
    def read_dict(cls, d: dict[str, _t.Sequence]) -> _tt.Table:
        tbl = _tt.Table(d)
        return validate_table(tbl, cls)
    
    @classmethod
    def read_excel(cls, path: str, sheet_name: str | None = None) -> _tt.Table:
        tbl: _tt.Table = _tt.read_excel(path, sheet_name)
        return validate_table(tbl, cls)
    
    @classmethod
    def read_sqlite(cls, path: str, table_name: str) -> _tt.Table:
        tbl: _tt.Table = _tt.read_sqlite(path, table_name)
        return validate_table(tbl, cls)
        
    @classmethod
    def read_csv_chunks(cls, path: str, chunksize: int) -> _t.Generator[_tt.Table, None, None]:
        for d_chunk in tinytable.csv.chunk_csv_file(path, chunksize):
            tbl = _tt.Table(d_chunk)
            yield validate_table(tbl, cls)