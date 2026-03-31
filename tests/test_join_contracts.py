import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrame
from pydantable.schema import Schema


class LeftSchema(Schema):
    id: int
    age: int | None
    score: int


class RightSchema(Schema):
    id: int
    age: int | None
    country: str
    score: int


def test_join_rejects_empty_on() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )

    with pytest.raises(ValueError, match=r"requires on=\.\.\. or both left_on"):
        left.join(right, on=[], how="inner")


def test_join_unknown_left_key() -> None:
    class LeftNoId(Schema):
        x: int
        age: int | None

    class RightHasId(Schema):
        id: int
        age: int | None

    left = DataFrame[LeftNoId]({"x": [1, 2], "age": [None, None]})
    right = DataFrame[RightHasId]({"id": [1, 2], "age": [None, None]})

    with pytest.raises(KeyError, match=r"join\(\) unknown left join key"):
        left.join(right, on="id", how="inner")


def test_join_unknown_right_key() -> None:
    class LeftHasId(Schema):
        id: int
        age: int | None

    class RightNoId(Schema):
        y: int
        age: int | None

    left = DataFrame[LeftHasId]({"id": [1, 2], "age": [None, None]})
    right = DataFrame[RightNoId]({"y": [1, 2], "age": [None, None]})

    with pytest.raises(KeyError, match=r"join\(\) unknown right join key"):
        left.join(right, on="id", how="inner")


def test_join_rejects_unsupported_how() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )

    with pytest.raises(ValueError, match=r"Unsupported join how"):
        left.join(right, on="id", how="median")


def test_join_supports_right_semi_anti_cross() -> None:
    left = DataFrame[LeftSchema]({"id": [1, 2], "age": [10, None], "score": [10, 20]})
    right = DataFrame[RightSchema](
        {"id": [2, 3], "age": [None, 30], "country": ["US", "CA"], "score": [200, 300]}
    )

    out_right = left.join(right, on="id", how="right", suffix="_r").collect(
        as_lists=True
    )
    assert set(out_right.keys()) == {
        "id",
        "age",
        "score",
        "age_r",
        "country",
        "score_r",
    }

    out_semi = left.join(right, on="id", how="semi").collect(as_lists=True)
    assert out_semi == {"id": [2], "age": [None], "score": [20]}

    out_anti = left.join(right, on="id", how="anti").collect(as_lists=True)
    assert out_anti == {"id": [1], "age": [10], "score": [10]}

    out_cross = left.join(right, how="cross", suffix="_r").collect(as_lists=True)
    assert len(out_cross["id"]) == 4


def test_cross_join_rejects_on_keys() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )
    with pytest.raises(ValueError, match="cross join does not accept"):
        left.join(right, on="id", how="cross")


def test_join_supports_expression_keys() -> None:
    left = DataFrame[LeftSchema]({"id": [1, 2], "age": [10, 20], "score": [10, 20]})
    right = DataFrame[RightSchema](
        {"id": [2, 1], "age": [20, 10], "country": ["US", "CA"], "score": [200, 100]}
    )
    out = left.join(right, left_on=left.id, right_on=right.id, how="inner").collect(
        as_lists=True
    )
    assert_table_eq_sorted(
        out,
        {
            "id": [1, 2],
            "age": [10, 20],
            "score": [10, 20],
            "age_right": [10, 20],
            "country": ["CA", "US"],
            "score_right": [100, 200],
        },
        keys=["id"],
    )


def test_join_collision_suffixes_all_non_key_overlaps() -> None:
    class Left(Schema):
        id: int
        score: int
        age: int | None

    class Right(Schema):
        id: int
        score: int
        age: int | None

    left = DataFrame[Left]({"id": [2, 1], "score": [20, 10], "age": [200, 100]})
    right = DataFrame[Right]({"id": [2, 1], "score": [200, 100], "age": [2000, 1000]})

    joined = left.join(right, on="id", how="inner", suffix="_r")
    out = joined.collect(as_lists=True)
    assert_table_eq_sorted(
        out,
        {
            "id": [1, 2],
            "score": [10, 20],
            "score_r": [100, 200],
            "age": [100, 200],
            "age_r": [1000, 2000],
        },
        keys=["id"],
    )


def test_join_validate_one_to_one_rejects_duplicates() -> None:
    left = DataFrame[LeftSchema]({"id": [1, 1], "age": [10, 20], "score": [10, 20]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [10], "country": ["US"], "score": [100]}
    )
    with pytest.raises(ValueError, match="one_to_one"):
        left.join(right, on="id", how="inner", validate="one_to_one").to_dict()


def test_join_validate_not_supported_on_scan_roots(tmp_path) -> None:
    left_csv = tmp_path / "left.csv"
    right_csv = tmp_path / "right.csv"
    left_csv.write_text("id,age,score\n1,10,10\n", encoding="utf-8")
    right_csv.write_text("id,age,country,score\n1,10,US,100\n", encoding="utf-8")

    left = DataFrame[LeftSchema].read_csv(str(left_csv))
    right = DataFrame[RightSchema].read_csv(str(right_csv))
    out = left.join(right, on="id", how="inner", validate="one_to_one").to_dict()
    assert set(out.keys()) >= {"id", "age", "score", "country", "score_right"}


def test_join_validate_scan_roots_multi_key_and_side_specific(tmp_path) -> None:
    class L(Schema):
        k1: int
        k2: int
        v: int

    class R(Schema):
        k1: int
        k2: int
        v2: int

    lp = tmp_path / "l.csv"
    rp = tmp_path / "r.csv"
    # Left duplicates on (k1,k2) but right is unique.
    lp.write_text("k1,k2,v\n1,1,10\n1,1,11\n2,2,20\n", encoding="utf-8")
    rp.write_text("k1,k2,v2\n1,1,100\n2,2,200\n", encoding="utf-8")

    left = DataFrame[L].read_csv(str(lp))
    right = DataFrame[R].read_csv(str(rp))

    with pytest.raises(ValueError, match="one_to_many"):
        left.join(right, on=["k1", "k2"], how="inner", validate="one_to_many").to_dict()

    # many_to_one should pass because right keys are unique.
    out = left.join(right, on=["k1", "k2"], how="inner", validate="many_to_one").to_dict()
    assert set(out.keys()) >= {"k1", "k2", "v", "v2"}


def test_join_coalesce_true_left_on_right_on_left_join_drops_right_key() -> None:
    class L(Schema):
        lid: int
        v: int

    class R(Schema):
        rid: int
        w: int

    left = DataFrame[L]({"lid": [1, 2], "v": [10, 20]})
    right = DataFrame[R]({"rid": [1], "w": [100]})

    out = left.join(
        right, left_on="lid", right_on="rid", how="left", coalesce=True
    ).collect(as_lists=True)
    assert set(out.keys()) == {"lid", "v", "w"}
    assert out["lid"] == [1, 2]


def test_join_coalesce_true_left_on_right_on_right_join_prefers_right_key_including_right_only_rows() -> None:
    class L(Schema):
        lid: int
        v: int

    class R(Schema):
        rid: int
        w: int

    left = DataFrame[L]({"lid": [1], "v": [10]})
    right = DataFrame[R]({"rid": [1, 2], "w": [100, 200]})

    out = left.join(
        right, left_on="lid", right_on="rid", how="right", coalesce=True
    ).collect(as_lists=True)
    assert set(out.keys()) == {"rid", "v", "w"}
    assert out["rid"] == [1, 2]


def test_join_coalesce_true_scan_roots_right_join(tmp_path) -> None:
    class L(Schema):
        lid: int
        v: int

    class R(Schema):
        rid: int
        w: int

    lp = tmp_path / "l.csv"
    rp = tmp_path / "r.csv"
    lp.write_text("lid,v\n1,10\n", encoding="utf-8")
    rp.write_text("rid,w\n1,100\n2,200\n", encoding="utf-8")

    left = DataFrame[L].read_csv(str(lp))
    right = DataFrame[R].read_csv(str(rp))
    out = left.join(
        right, left_on="lid", right_on="rid", how="right", coalesce=True
    ).to_dict()
    assert set(out.keys()) == {"rid", "v", "w"}
    assert out["rid"] == [1, 2]


def test_join_coalesce_true_multi_key_right_join_drops_left_keys(tmp_path) -> None:
    class L(Schema):
        a: int
        b: int
        v: int

    class R(Schema):
        x: int
        y: int
        w: int

    left = DataFrame[L]({"a": [1], "b": [1], "v": [10]})
    right = DataFrame[R]({"x": [1, 2], "y": [1, 2], "w": [100, 200]})

    out = left.join(
        right,
        left_on=["a", "b"],
        right_on=["x", "y"],
        how="right",
        coalesce=True,
    ).collect(as_lists=True)
    assert set(out.keys()) == {"x", "y", "v", "w"}
    assert out["x"] == [1, 2]
    assert out["y"] == [1, 2]


def test_join_coalesce_rejected_combinations() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )

    with pytest.raises(ValueError, match="cross join does not support coalesce"):
        left.join(right, how="cross", coalesce=True).to_dict()

    with pytest.raises(NotImplementedError, match="full joins"):
        class L(Schema):
            lid: int
            v: int

        class R(Schema):
            rid: int
            w: int

        l2 = DataFrame[L]({"lid": [1], "v": [10]})
        r2 = DataFrame[R]({"rid": [1], "w": [100]})
        l2.join(r2, left_on="lid", right_on="rid", how="full", coalesce=True).to_dict()

    with pytest.raises(NotImplementedError, match="expression keys"):
        left.join(right, left_on=left.id, right_on=right.id, how="inner", coalesce=True).to_dict()
