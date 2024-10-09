from collections.abc import Mapping
import typing as _t
import csv

from pydantable.generators.dicts.base import chain, iter
from pydantable.results import dicts


class CSVDictWriter(iter.BaseIter, chain.ChainBase):
    def __init__(
        self,
        data: _t.Iterator[dicts.MappingResult],
        csv_file: _t.IO,
        add_headers: bool = True
    ) -> None:
        super().__init__(data)
        self.file: _t.IO = csv_file
        self.writer: csv.DictWriter | None = None
        self.add_headers: bool = add_headers

    def __next__(self) -> dicts.MappingResult:
        result: dicts.MappingResult = super().__next__()
        if isinstance(result, Mapping):
            if self.writer is None:
                    self._start_writer(result)
            return self.write(result)
        
        if isinstance(result, Exception):
            return result

    def _start_writer(self, row: Mapping) -> None:
        self.writer = csv.DictWriter(self.file, row.keys())
        if self.add_headers:
            self.writer.writeheader()
            
    def write(self, row: Mapping) -> dicts.MappingResult:
        if self.writer is None:
            raise ValueError('writer is not initialized')
        try:
            self.writer.writerow(row)
        except Exception as e:
            return e
        return row