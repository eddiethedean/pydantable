"""0.18.0 regression: join/group_by integration (Hypothesis + Rust tests complement)."""

from __future__ import annotations

from collections import Counter

from pydantable import DataFrame, Schema


class KV(Schema):
    k: int
    v: int


class L(Schema):
    id: int
    x: int


class R(Schema):
    id: int
    y: int


def test_group_by_agg_empty_frame() -> None:
    """Grouped aggregation on zero rows returns empty output columns."""
    df = DataFrame[KV]({"k": [], "v": []})
    out = df.group_by("k").agg(s=("sum", "v")).collect(as_lists=True)
    assert out == {"k": [], "s": []}


def test_group_by_multiple_aggs_match_reference() -> None:
    """Several aggregations in one ``agg()`` match manual per-key stats."""
    data = {"k": [1, 1, 2, 2, 2], "v": [10, 20, 1, 2, 3]}
    df = DataFrame[KV](data)
    out = (
        df.group_by("k")
        .agg(
            s=("sum", "v"),
            c=("count", "v"),
            mu=("mean", "v"),
            n=("n_unique", "v"),
        )
        .collect(as_lists=True)
    )
    by_k: dict[int, list[int]] = {}
    for k, v in zip(data["k"], data["v"], strict=True):
        by_k.setdefault(k, []).append(v)
    exp: dict[str, list] = {"k": [], "s": [], "c": [], "mu": [], "n": []}
    for k in sorted(by_k):
        vs = by_k[k]
        exp["k"].append(k)
        exp["s"].append(sum(vs))
        exp["c"].append(len(vs))
        exp["mu"].append(sum(vs) / len(vs))
        exp["n"].append(len(set(vs)))
    got = {
        "k": list(out["k"]),
        "s": list(out["s"]),
        "c": list(out["c"]),
        "mu": list(out["mu"]),
        "n": list(out["n"]),
    }
    assert got == exp


def test_left_join_preserves_left_row_count_with_duplicate_keys() -> None:
    """``how='left'`` emits one output row per left row (0.18.0 join smoke)."""
    left = DataFrame[L]({"id": [1, 1, 2, 3], "x": [10, 20, 30, 40]})
    right = DataFrame[R]({"id": [1, 2], "y": [100, 200]})
    j = left.join(right, on="id", how="left")
    out = j.collect(as_lists=True)
    assert len(out["id"]) == 4


def test_semi_join_row_subset_matches_keys_in_right() -> None:
    """Semi join keeps left rows whose key exists on the right at least once."""
    left = DataFrame[L]({"id": [1, 1, 2, 3], "x": [10, 20, 30, 40]})
    right = DataFrame[R]({"id": [1], "y": [99]})
    s = left.join(right, on="id", how="semi")
    out = s.collect(as_lists=True)
    assert out["id"] == [1, 1]
    assert out["x"] == [10, 20]


def test_anti_join_excludes_matching_keys() -> None:
    left = DataFrame[L]({"id": [1, 2, 3], "x": [10, 20, 30]})
    right = DataFrame[R]({"id": [2], "y": [0]})
    a = left.join(right, on="id", how="anti")
    out = a.collect(as_lists=True)
    assert out == {"id": [1, 3], "x": [10, 30]}


def test_group_by_count_matches_group_size() -> None:
    """``count`` on a non-null column equals number of rows per key."""
    data = {"k": [0, 0, 0, 1, 2, 2], "v": [1, 2, 3, 4, 5, 6]}
    df = DataFrame[KV](data)
    out = df.group_by("k").agg(c=("count", "v")).collect(as_lists=True)
    sizes = Counter(data["k"])
    got = dict(zip(out["k"], out["c"], strict=True))
    assert got == dict(sizes)
