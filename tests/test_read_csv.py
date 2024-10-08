import pydantic
import pytest
from tinytable.table import Table
from pydantable import base
from pydantable.errors import ValidationErrors


class PeopleTable(base.BaseTableModel):
    id: int
    name: str
    age: int


class Ages(pydantic.BaseModel):
    id: int
    age: float


def test_read_csv() -> None:
    tbl: Table = PeopleTable.read_csv('tests/data/people.csv')
    assert tbl.data == {
        'id': [1, 2, 3],
        'name': ['Odos', 'Kayla', 'Dexter'],
        'age': [38, 31, 2]
    }


def test_validation() -> None:
    tbl = Table({'id': ['1', '2'], 'age': ['Six', '99']})
    with pytest.raises(ValidationErrors):
        base.validate_table(tbl, Ages)
