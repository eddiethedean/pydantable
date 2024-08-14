import csv
from urllib import request
from os.path import exists
from typing import Dict, Generator, List, Optional, Sequence, Union

from tinytable.functional.features import column_names
from tinytable.functional.rows import itertuples, row_dicts_to_data
from tinytable.functional.utils import combine_names_rows
from tinytable.types import DataDict, DataMapping, data_dict


def convert_str(value: str) -> Union[float, int, bool, str]:
    """Takes a str value and tries to convert it to float, int, or bool
       Returns converted value if successful, or str value if fails to convert.
    """
    value = str(value)
    if value.count('.') == 1:
        try:
            return float(value)
        except ValueError:
            pass
    if value.isnumeric():
        try:
            return int(value)
        except ValueError:
            pass
    if value in {'True', 'False'}:
        return bool(value)
    return value


def chunk_csv_file(
    path: str,
    chunksize=5,
    newline='',
    encoding='utf-8-sig'
) -> Generator[dict, None, None]:
    """
    Read chunks of table object from given CSV file.
    """
    column_names = []
    rows = []
    first = True
    chunk_end = chunksize
    with open(path, 'r', newline=newline, encoding=encoding) as f:
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        for i, row in enumerate(csv.reader(f, dialect)):
            if first:
                column_names = row
                first = False
            else:
                rows.append([convert_str(v) for v in row])
            if i == chunk_end:
                yield combine_names_rows(column_names, rows)
                rows = []
                chunk_end += chunksize
        else:
            if len(rows) > 0:
                yield combine_names_rows(column_names, rows)


def convert_values(d: DataMapping) -> DataDict:
    """Try to convert each column values to int or float"""
    d = data_dict(d)
    convert_values_inplace(d)
    return d


def convert_values_inplace(d: DataDict) -> None:
    for col_name, values in d.items():
        for i, value in enumerate(values):
            d[col_name][i] = convert_str(value)


def convert_all(values: list, to_type: type) -> bool:
    new_values = []
    for value in values:
        old_value = value
        try:
            old_value = to_type(value)
        except ValueError:
            return False
        new_values.append(old_value)
    for i in range(len(values)):
        values[i] = new_values[i]
    return True
        
def convert_all_to_float(values: list) -> bool:
    return convert_all(values, float)
    
    
def convert_all_to_int(values: list) -> bool:
    new_values = []
    for value in values:
        old_value = value
        
        try:
            if '.' in str(old_value):
                raise ValueError
            old_value = int(value)
        except ValueError:
            return False
        new_values.append(old_value)
    for i in range(len(values)):
        values[i] = new_values[i]
    return True


def convert_columns_inplace(d: DataDict) -> None:
    """Try to convert entire column to float then int
       If all successfully convert, convert entire column
       otherwise leave column as is.
    """
    for col in d:
        to_int_success = convert_all_to_int(d[col])
        if not to_int_success:
            convert_all_to_float(d[col])


def read_csv_file(
    path: str,
    names: Optional[Sequence[str]] = None,
    newline: str = '',
    encoding: str = 'utf-8-sig',
    convert_numbers: bool = True,
    convert_columns: bool = False
) -> Dict[str, List]:
    with open(path, 'r', newline=newline, encoding=encoding) as f:
        reader = csv.DictReader(f, fieldnames=names) if names else csv.DictReader(f)
        d = row_dicts_to_data([row for row in reader])
    if convert_numbers: convert_values_inplace(d)  # type: ignore
    if convert_columns: convert_columns_inplace(d)  # type: ignore
    return d


def data_to_csv_file(
    data: DataDict,
    path: str,
    newline='',
    encoding='utf-8-sig'
) -> None:
    """Write data to csv file at path."""
    names = column_names(data)
    rows = itertuples(data)
    with open(path, 'w', encoding=encoding, newline=newline) as f:
        writer = csv.writer(f)
        writer.writerow(names)
        writer.writerows(rows)


def read_csv_url(
    url: str,
    names: Optional[Sequence[str]] = None,
    encoding='utf-8-sig',
    convert_numbers: bool = True,
    convert_columns: bool = False
) -> Dict[str, List]:
    response = request.urlopen(url)
    lines = [l.decode(encoding) for l in response.readlines()]
    reader = csv.DictReader(lines, fieldnames=names) if names else csv.DictReader(lines)
    d = row_dicts_to_data([row for row in reader])
    if convert_numbers: convert_values_inplace(d)  # type: ignore
    if convert_columns: convert_columns_inplace(d)  # type: ignore
    return d


def read_csv(
    path: str,
    names: Optional[Sequence[str]] = None,
    newline: str = '',
    encoding: str = 'utf-8-sig',
    convert_numbers: bool = True,
    convert_columns: bool = False
) -> Dict[str, List]:
    # check if path is valid file path
    if exists(path):
        return read_csv_file(path,
                             names,
                             newline,
                             encoding,
                             convert_numbers,
                             convert_columns)
    return read_csv_url(path,
                        names,
                        encoding,
                        convert_numbers,
                        convert_columns)