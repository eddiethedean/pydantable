from typing import Collection, List, Union

import tinytable as tt
import tinytable.functional.group as group


class Group:
    """Returned by Column and Table groupby method.
       Acts like a list of tuple(key, Table)
       Can apply aggregation function to calculate new Table.
    """
    def __init__(self, groups: List[tuple], by: Union[str, Collection]):
        self.groups = groups
        self.by = [by] if isinstance(by, str) else by

    def __iter__(self):
        return iter(self.groups)

    def __repr__(self):
        return repr(self.groups)

    def __getitem__(self, i: int):
        return self.groups[i]
        
    def sum(self):
        labels, rows = group.sum_groups(self.groups)
        return tt.Table(rows, labels)

    def count(self):
        labels, rows = group.count_groups(self.groups)
        return tt.Table(rows, labels)

    def mean(self):
        labels, rows = group.mean_groups(self.groups)
        return tt.Table(rows, labels)

    def min(self):
        labels, rows = group.min_groups(self.groups)
        return tt.Table(rows, labels)

    def max(self):
        labels, rows = group.max_groups(self.groups)
        return tt.Table(rows, labels)

    def mode(self):
        labels, rows = group.mode_groups(self.groups)
        return tt.Table(rows, labels)

    def std(self):
        labels, rows = group.stdev_groups(self.groups)
        return tt.Table(rows, labels)

    def pstd(self):
        labels, rows = group.pstdev_groups(self.groups)
        return tt.Table(rows, labels)

    def nunique(self):
        labels, rows = group.nunique_groups(self.groups)
        return tt.Table(rows, labels)



