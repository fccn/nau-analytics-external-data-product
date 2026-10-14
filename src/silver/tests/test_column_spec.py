"""Unit tests for utils/column_spec.py's build_conditional_columns.

No pyspark/Spark session needed -- the function is pure list
manipulation. Duplicated in src/silver/tests/ (same as the module under
test itself) since gold/silver don't share an import path.
"""
import os

from utils.column_spec import build_conditional_columns


def test_plain_values_always_included():
    assert build_conditional_columns(["a", "b", "c"], enabled=False) == ["a", "b", "c"]
    assert build_conditional_columns(["a", "b", "c"], enabled=True) == ["a", "b", "c"]


def test_conditional_item_included_only_when_enabled():
    spec = ["a", ("b", True), "c"]
    assert build_conditional_columns(spec, enabled=True) == ["a", "b", "c"]
    assert build_conditional_columns(spec, enabled=False) == ["a", "c"]


def test_is_conditional_false_behaves_like_a_plain_value():
    # (value, False) is excluded only when is_conditional is True AND
    # enabled is False -- with is_conditional=False that can never
    # happen, so it's always included, same as a bare (non-tuple) value.
    spec = ["a", ("b", False), "c"]
    assert build_conditional_columns(spec, enabled=True) == ["a", "b", "c"]
    assert build_conditional_columns(spec, enabled=False) == ["a", "b", "c"]


def test_multiple_conditional_items_preserve_order():
    spec = [("a", True), "b", ("c", True), "d", ("e", True)]
    assert build_conditional_columns(spec, enabled=True) == ["a", "b", "c", "d", "e"]
    assert build_conditional_columns(spec, enabled=False) == ["b", "d"]


def test_empty_spec():
    assert build_conditional_columns([], enabled=True) == []
    assert build_conditional_columns([], enabled=False) == []


def test_works_with_non_string_values():
    # The function never inspects the value itself, so it should work
    # just as well with pyspark Column objects (production usage) as
    # with plain strings (these tests) or any other object.
    sentinel = object()
    spec = ["a", (sentinel, True)]
    assert build_conditional_columns(spec, enabled=True) == ["a", sentinel]
    assert build_conditional_columns(spec, enabled=False) == ["a"]


def test_gold_and_silver_copies_are_identical():
    # column_spec.py is deliberately duplicated verbatim under both
    # src/gold/utils/ and src/silver/utils/ (gold/silver don't share an
    # import path -- see this module's own docstring). This test is the
    # CI-enforced guarantee that the two copies never silently drift.
    this_dir = os.path.dirname(os.path.abspath(__file__))
    gold_path = os.path.join(this_dir, "..", "..", "gold", "utils", "column_spec.py")
    silver_path = os.path.join(this_dir, "..", "..", "silver", "utils", "column_spec.py")
    with open(gold_path) as f:
        gold_content = f.read()
    with open(silver_path) as f:
        silver_content = f.read()
    assert gold_content == silver_content, (
        "src/gold/utils/column_spec.py and src/silver/utils/column_spec.py "
        "have drifted apart -- they must be kept byte-identical (see the "
        "module's own docstring)."
    )
