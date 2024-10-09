from abc import ABC, abstractmethod
from collections.abc import Mapping
import typing as _t
import csv

from pydantable.generators.dicts.base import chain, iter
from pydantable.results import dicts as dict_results


class CSVDictWriter(iter.BaseIter, chain.ChainBase):
    def __init__(
        self,
        data: _t.Iterator[dict_results.MappingResult],
        csv_file: _t.IO,
        add_headers: bool = True,
        mode: iter.ReturnMode = iter.ReturnMode.PASSIVE
    ) -> None:
        super().__init__(data, mode)
        self.file: _t.IO = csv_file
        self.writer: csv.DictWriter | None = None
        self.add_headers: bool = add_headers

    def __next__(self) -> dict_results.MappingResult:
        result: dict_results.MappingResult = super().__next__()
        if isinstance(result, Mapping):
            if self.writer is None:
                    self._start_writer(result)
            return self.write(result)
        
        if isinstance(result, Exception):
            return self.filter_error(result)

    def _start_writer(self, row: Mapping) -> None:
        self.writer = csv.DictWriter(self.file, row.keys())
        if self.add_headers:
            self.writer.writeheader()
            
    def write(self, row: Mapping) -> dict_results.MappingResult:
        if self.writer is None:
            raise ValueError('writer is not initialized')
        try:
            self.writer.writerow(row)
        except Exception as e:
            return self.filter_error(e)
        return row


class CSVWriterMixin(ABC):
    @abstractmethod
    def __iter__(self) -> _t.Self:
        ...

    @abstractmethod
    def __next__(self) -> dict_results.MappingResult:
        ...

    def csv_writer(
        self,
        csv_file: _t.IO,
        mode: iter.ReturnMode = iter.ReturnMode.PASSIVE
    ) -> CSVDictWriter:
        return CSVDictWriter(self, csv_file, mode=mode)