from __future__ import annotations
import copy
from typing import Dict, List, Mapping, Optional, Sequence, Union, Iterator
from typing import Any, Callable, Generator
from enum import Enum

from tabulate import tabulate
from hasattrs import has_mapping_attrs

from tinytable.column import Column
from tinytable.row import Row
from tinytable.group import Group
from tinytable.filter import Filter
from tinytable.iloc import Iloc
import tinytable.column as column
import tinytable.row as row
import tinytable.csv as csv
import tinytable.excel as excel
import tinytable.sqlite as sqlite
import tinytable.functional.edit as edit
import tinytable.functional.features as features
import tinytable.functional.utils as utils
import tinytable.functional.filter as filter
import tinytable.functional.rows as rows
import tinytable.functional.columns as columns
import tinytable.functional.copy as data_copy
import tinytable.functional.group as group
import tinytable.functional.join as join
import tinytable.functional.na as na

from tinytable.types import DataDict, data_dict, DataMapping
ColumnNames = Sequence[str]


class JoinStrategy(str, Enum):
    left = 'left'
    right = 'right'
    inner = 'inner'
    full = 'full'


class Table:
    """Data table organized into {column_name: list[values]}
    
       A pure Python version of Pandas DataFrame.
    """
    def __init__(
        self,
        data: Union[DataMapping, Sequence[Sequence]] = {},
        labels: Optional[Sequence] = None,
        columns: Optional[ColumnNames] = None
    ) -> None:
        """
        data passed can be Mapping[str, Iterable] ({column_name: column_values}) or
        Sequence[Sequence] (sequence of row values) with column names passed in columns parameter.
        """
        data = self._transform_data_input(data, columns)
        self.data: DataDict = data
        self._validate()
        self.labels: Union[None, list] = labels if labels is None else list(labels)

    def _transform_data_input(
        self,
        data: Union[DataMapping, Sequence[Sequence]],
        columns: Optional[ColumnNames] = None
    ) -> Dict[str, list]:
        out: Dict[str, list]
        if columns is not None and not has_mapping_attrs(data):
            # data is a sequence of sequences and column names passed
            return rows.row_values_to_data(list(data), columns)
        return data_dict(data)

    @classmethod
    def from_records(
        cls,
        data: Sequence[Sequence],
        columns: Sequence[str],
        labels: Optional[Sequence] = None
    ) -> Table:
        """Convert sequence of row values to Table"""
        return Table(rows.row_values_to_data(data, columns), labels=labels)

    @classmethod
    def from_dict(cls, data: DataDict, columns=None) -> Table:
        """Costruct Table from dict of column values"""
        return Table(data, columns)
        
    def __len__(self) -> int:
        return features.row_count(self.data)
        
    def __repr__(self) -> str:
        index = True if self.labels is None else self.labels
        return tabulate(self, headers=self.columns, tablefmt='grid', showindex=index)
    
    def __iter__(self) -> Iterator[str]:
        return iter(self.data)
    
    def __getitem__(self, key: Union[str, int]) -> Union[column.Column, Row, Table]:
        """
           Use str key for Column selection. Setting Column items changes parent Table.
           Use int key for Row selection. Setting Row items changes parent Table.

           Selecting subset of Table returns new Table,
           changes do not change original Table.
           Use int:int:int for index slice of Table rows.
           Use list of bool values to filter to Table of True rows.
           Use Filter to filter to Table of True rows.
        """
        # tbl['id'] -> Column
        if isinstance(key, str):
            return self.column(str(key))
        # tbl[1] -> Row
        if isinstance(key, int):
            index: int = self._convert_index(key)
            self._validate_index(index)
            return self.row(index)
        # tbl[1:4] -> Table
        if isinstance(key, slice):
            validate_int_slice(key)
            return self.filter_by_indexes(list(utils.slice_to_range(key, len(self))))
        if isinstance(key, list):
            if utils.all_bool(key):
                # tbl[[True, False, True, True]] -> Table
                return self.filter(key)
            # tbl[['id', 'name']] -> Table
            validate_list_key(key)
            return self.only_columns(key)
        if isinstance(key, Filter):
            # tbl[tbl['age'] >= 18] -> Table
            return self.filter(key)
        if isinstance(key, tuple):
            # tble[1, 2] or tbl[(1, 2)] -> labeled Row
            if self.labels is None:
                raise ValueError('Table must have labels to use tuple as key.')
            else:
                if key not in self.labels:
                    raise KeyError('tuple key is not in Table labels.')
                labels = self.labels
                if isinstance(labels, list):
                    index = labels.index(key)
                return self[index]
        raise TypeError('key must be str for column selection, int for row selection or slice for subset of Table rows.')
    
    def __setitem__(self, key: Union[str, int], values: list) -> None:
        if type(key) == str:
            column_name: str = str(key)
            self.edit_column(column_name, values)
        elif type(key) == int:
            index: int = int(key)
            self.edit_row(index, values)

    def __delitem__(self, key: Union[str, int]) -> None:
        if type(key) == str:
            column_name: str = str(key)
            self.drop_column(column_name)
        elif type(key) == int:
            index: int = int(key)
            self.drop_row(index)

    @property
    def shape(self) -> tuple[int, int]:
        return features.shape(self.data)

    @property
    def size(self) -> int:
        return features.size(self.data)

    @property
    def columns(self) -> tuple[str]:
        """Column names."""
        return features.column_names(self.data)

    @columns.setter
    def columns(self, values: List[str]) -> None:
        """Set the value of the column names."""
        self.replace_column_names(values)

    @property
    def index(self) -> Column:
        return Column(features.index(self.data), None, self, self.labels)

    @property
    def values(self) -> tuple[tuple]:
        return features.values(self.data)

    @property
    def iloc(self) -> Iloc:
        """Purely integer-location based indexing for selection by position."""
        return Iloc(self)

    def filter(self, f: Filter) -> Table:
        indexes = filter.indexes_from_filter(f)
        return self.filter_by_indexes(indexes)

    def only_columns(self, column_names: List[str]) -> Table:
        """Return new Table with only column_names Columns."""
        d = filter.only_columns(self.data, column_names)
        return Table(data_dict(d), self.labels)

    def _convert_index(self, index: int) -> int:
        if index < 0:
            return len(self) + index
        return index

    def _validate_index(self, index: int) -> None:
        if len(self) == 0:
            raise IndexError('row index out of range (empty Table)')
        upper_range = len(self) - 1
        if index > len(self) - 1 or index < 0:
            raise IndexError(f'row index {index} out of range (0-{upper_range})')
        
    def _validate(self) -> bool:
        count = None
        for key in self.data:
            col_count = len(self.data[key])
            if count is None:
                count = col_count
            if count != col_count:
                raise ValueError('All columns must be of the same length')
            count = col_count
        return True

    def _get_label(self, index: int) -> Union[None, List]:
        return None if self.labels is None else self.labels[index]
   
    def row(self, index: int) -> Row:
        label = self._get_label(index)
        return Row(rows.row_dict(self.data, index), index, self, label)

    def column(self, column_name: str) -> Column:
        return Column(features.column_values(self.data, column_name), column_name, self, self.labels)

    def drop_column(self, column_name: str, inplace=True) -> Union[None, Table]:
        if inplace:
            edit.drop_column_inplace(self.data, column_name)
        else:
            return Table(edit.drop_column(self.data, column_name), self.labels)

    def drop_row(self, index: int, inplace=True) -> Union[None, Table]:
        if inplace:
            edit.drop_row_inplace(self.data, index)
            edit.drop_label_inplace(self.labels, index)
        else:
            return Table(edit.drop_row(self.data, index), edit.drop_label(self.labels, index))

    def keys(self) -> tuple[str]:
        return self.columns
    
    def itercolumns(self) -> Generator[Column, None, None]:
        return column.itercolumns(self.data, self, self.labels)
            
    def iterrows(self) -> Generator[tuple[int, Row], None, None]:
        return row.iterrows(self.data, self, self.labels)

    def iteritems(self) -> Generator[tuple[str, column.Column], None, None]:
        return column.iteritems(self.data, self)

    def itertuples(self) -> Generator[tuple, None, None]:
        return rows.itertuples(self.data)
    
    def edit_row(self, index: int, values: Union[Mapping, Sequence], inplace=True) -> Union[None, Table]:
        if inplace:
            if isinstance(values, Mapping):
                edit.edit_row_items_inplace(self.data, index, values)
            elif isinstance(values, Sequence):
                edit.edit_row_values_inplace(self.data, index, values)
        else:
            if isinstance(values, Mapping):
                data = edit.edit_row_items(self.data, index, values)
            elif isinstance(values, Sequence):
                data = edit.edit_row_values(self.data, index, values)
            return Table(data, copy.copy(self.labels))
            
    def edit_column(self, column_name: str, values: Sequence, inplace=True) ->Union[None, Table]:
        if inplace:
            edit.edit_column_inplace(self.data, column_name, values)
            return 
        else:
            return Table(edit.edit_column(self.data, column_name, values))

    def edit_value(self, column_name: str, index: int, value: Any, inplace=True) -> Union[None, Table]:
        if inplace:
            edit.edit_value_inplace(self.data, column_name, index, value)
        else:
            return Table(edit.edit_value(self.data, column_name, index, value), copy.copy(self.labels))

    def copy(self, deep=False) -> Table:
        if deep:
             return Table(data_copy.deepcopy_table(self.data), copy.deepcopy(self.labels))
        return Table(data_dict(data_copy.copy_table(self.data)), copy.copy(self.labels))

    def cast_column_as(self, column_name: str, data_type: Callable) -> None:
        self.data[column_name] = [data_type(value) for value in self.data[column_name]]

    def replace_column_names(self, new_keys: Sequence[str]) -> None:
        if len(new_keys) != len(self.data.keys()):
            raise ValueError('new_keys must be same len as dict keys.')
        for new_key, old_key in zip(new_keys, self.data.keys()):
            if new_key != old_key:
                self.data[new_key] = list(self.data[old_key])
                del self.data[old_key]

    def to_csv(self, path: str) -> None:
        """Save Table as csv at path."""
        csv.data_to_csv_file(self.data, path)

    def to_excel(
        self,
        path: str,
        sheet_name: Optional[str] = None,
        replace_workbook: bool = False,
        replace_worksheet: bool = True
    ) -> None:
        """Save Table in Excel Workbook."""
        excel.data_to_excel_file(
            self.data,
            path,
            sheet_name,
            replace_workbook,
            replace_worksheet)

    def to_sqlite(
        self,
        path: str,
        table_name: str,
        primary_key: Optional[str] = None,
        replace_table: bool = False,
        append_records = False
    ) -> None:
        """Save Table in sqlite database."""
        sqlite.data_to_sqlite_table(
            self.data,
            path,
            table_name,
            primary_key,
            replace_table,
            append_records)

    def label_head(self, n: int = 5) -> Union[None, List]:
        return None if self.labels is None else self.labels[:5]

    def label_tail(self, n: int = 5) -> Union[None, List]:
        return None if self.labels is None else self.labels[5:]

    def head(self, n: int = 5) -> Table:
        return Table(data_dict(features.head(self.data, n)), self.label_head(n))

    def tail(self, n: int = 5) -> Table:
        return Table(data_dict(features.tail(self.data, n)), self.label_tail(n))

    def nunique(self) -> dict[str, int]:
        """Count number of distinct values in each column.
           Return dict with number of distinct values.
        """
        return utils.nunique(self.data)

    def filter_by_indexes(self, indexes: Sequence[int]) -> Table:
        """return only rows in indexes"""
        labels = None if self.labels is None else filter.filter_list_by_indexes(self.labels, indexes)
        data = data_dict(filter.filter_by_indexes(self.data, indexes))
        if len(data) == 0:
            self.labels = None
        return Table(data, labels=labels)

    def filter_by_indexes_inplace(self, indexes: Sequence[int]) -> None:
        """return only rows in indexes"""
        labels = None if self.labels is None else filter.filter_list_by_indexes(self.labels, indexes)
        data = data_dict(filter.filter_by_indexes(self.data, indexes))
        if len(data) == 0:
            self.labels = None
        self.labels = labels
        self.data = data
    
    def filter_by_columns(self, columns: Sequence[str]) -> Table:
        data = data_dict(filter.filter_by_columns(self.data, columns))
        return Table(data, labels=self.labels)

    def filter_by_columns_inplace(self, columns: Sequence[str]) -> None:
        data = data_dict(filter.filter_by_columns(self.data, columns))
        self.data = data
        if len(self.data) == 0:
            self.labels = None

    def sample(self, n, random_state=None) -> Table:
        """return random sample of rows"""
        indexes = filter.sample_indexes(self.data, n, random_state)
        labels = None if self.labels is None else filter.filter_list_by_indexes(self.labels, indexes)
        return Table(data_dict(filter.filter_by_indexes(self.data, indexes)), labels=labels)

    def groupby(self, by: Union[str, Sequence]) -> Group:
        return Group([(value, Table(data)) for value, data in group.groupby(self.data, by)], by)

    def inner_join(self, other: DataMapping, left_on, right_on=None) -> Table:
        data = join.inner_join(self.data, other, left_on, right_on)
        return Table(data)

    def left_join(self, other: DataMapping, left_on, right_on=None) -> Table:
        data = join.left_join(self.data, other, left_on, right_on)
        return Table(data)

    def right_join(self, other: DataMapping, left_on, right_on=None) -> Table:
        data = join.right_join(self.data, other, left_on, right_on)
        return Table(data)

    def full_join(self, other: DataMapping, left_on, right_on=None) -> Table:
        data = join.full_join(self.data, other, left_on, right_on)
        return Table(data)

    def join(
        self,
        other: DataMapping,
        left_on,
        right_on=None,
        how: JoinStrategy = JoinStrategy.left
    ) -> Table:
        if how == JoinStrategy.left:
            return self.left_join(other, left_on, right_on)
        if how == JoinStrategy.right:
            return self.right_join(other, left_on, right_on)
        if how == JoinStrategy.inner:
            return self.inner_join(other, left_on, right_on)
        if how == JoinStrategy.full:
            return self.full_join(other, left_on, right_on)
        raise ValueError('how must be "left", "right", "inner", or "outer"')

    def sum(self) -> dict:
        return group.sum_data(self.data)

    def count(self) -> dict:
        return group.count_data(self.data)

    def mean(self) -> dict:
        return group.mean_data(self.data)

    def min(self) -> dict:
        return group.min_data(self.data)

    def max(self) -> dict:
        return group.max_data(self.data)

    def std(self) -> dict:
        return group.stdev_data(self.data)

    def mode(self) -> dict:
        return group.mode_data(self.data)

    def pstd(self) -> dict:
        return group.pstdev_data(self.data)

    def fillna(
        self,
        value: Optional[Any] = None,
        method: Optional[str] = None,
        axis: Optional[Union[int, str]] = 0,
        inplace: bool = False,
        limit: Optional[int] = None,
        na_value: Optional[Any] = None
    ) -> Union[Table, None]:
        """
        Fill missing values using the specified method.

        Parameters
        ----------
        value : Any
            value to use to fill missing values
        method : {'backfill', 'bfill', 'pad', 'ffill', None}
            method to use for filling holes in new Table.
            pad/ffill: propagate last valid observation
            forward to next valid
            backfill/bfill: use next valid observation to fill gap.
        axis : int | str, default 0
            {0 or 'rows', 1 or 'columns'}
        inplace : bool
            Change Table data inplace (True)
            Return new Table (False)
        limit : int, optional
            Max number of values to fill, fill all if None
        na_value : Any, default None
            The missing value to fill. use np.nan for pandas DataFrame

        Returns
        -------
        Table | None
            Table with missing values filled or None if inplace=True
        """
        data = na.fillna(self.data, value, method, axis, inplace, limit, na_value)
        if data is not None:
            return Table(data_dict(data), self.labels)
                
    def isna(self, na_value=None) -> Table:
        data = na.isna(self.data, na_value)
        return Table(data_dict(data), self.labels)

    def notna(self, na_value=None) -> Table:
        data = na.notna(self.data, na_value)
        return Table(data_dict(data), self.labels)

    isnull = isna
    notnull = notna

    def dropna(
        self,
        axis: Union[int, str] = 0,
        how: str = 'any',
        thresh: Optional[int] = None,
        subset: Optional[Sequence[str]] = None,
        inplace: bool = False,
        na_value: Optional[Any] = None
    ) -> Union[Table, None]:
        """
        Remove missing values.

        Parameters
        ----------
        axis : int | str
            {0 or 'rows', 1 or 'columns'}
        how : str
            {'any', 'all'}
        thresh : int, optional
            Require that many not missing values. Cannot be combined with how.
        subset : Sequence[str]
            column names to consider when checking for row values
        inplace : bool, default False
            Whether to modify the original Table rather than returning new Table.

        Returns
        -------
        Table | None
            Table with missing values removed or None if inplace=True
        """
        # Get the remaining indexes/column names after dropping na rows/columns
        if thresh is not None:
            remaining = na.dropna_thresh(self.data, thresh, axis, subset, na_value, remaining=True)
        elif how == 'any':
            remaining = na.dropna_any(self.data, axis, subset, na_value, remaining=True)
        elif how == 'all':
            remaining = na.dropna_all(self.data, axis, subset, na_value, remaining=True)
        else:
            raise ValueError('how must be "any" or "all" if thresh is not None')
        # Filter by indexes or columns
        if axis in [1, 'columns']:
            remaining_columns: List[str] = [str(name) for name in remaining]
            if inplace:
                self.filter_by_columns_inplace(remaining_columns)
            else:
                return self.filter_by_columns(remaining_columns)
        elif axis in [0, 'rows']:
            remaining_indexes = [int(index) for index in remaining]
            if inplace:
                self.filter_by_indexes_inplace(remaining_indexes)
            else:
                return self.filter_by_indexes(remaining_indexes)
        else:
            raise ValueError('axis but be 0, 1, "columns", or "rows"')


def read_csv(path: str, names: Optional[Sequence[str]] = None):
    return Table(csv.read_csv(path, names=names))


def read_excel(path: str, sheet_name: Optional[str] = None) -> Table:
    return Table(excel.read_excel_file(path, sheet_name))


def read_sqlite(path: str, table_name: str) -> Table:
    return Table(sqlite.read_sqlite_table(path, table_name))


def validate_int_slice(s: slice) -> None:
    if s.start is not None:
        if type(s.start) is not int:
            raise ValueError('slice start must be None or int')
    if s.stop is not None:
        if type(s.stop) is not int:
            raise ValueError('slice stop must be None or int')
    if s.step is not None:
        if type(s.step) is not int:
            raise ValueError('slice step must be None or int')


def validate_list_key(l: List) -> None:
    if not all(isinstance(item, str) for item in l):
        raise ValueError('All list items bust be str to use as key.')





