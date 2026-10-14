"""Small, dependency-free helper for building flag-conditional column
lists (SELECT projections, MERGE UPDATE SET / INSERT column lists, etc.)
that stay structurally consistent with each other.

Deliberately has no pyspark import -- it's pure list manipulation, works
the same whether the items are pyspark Column objects, plain column-name
strings, or SQL text fragments -- so it can be unit-tested without a
Spark session or the pyspark package installed (see tests/).

This exact file is kept byte-identical under both src/gold/utils/ and
src/silver/utils/ (gold/silver don't share an import path -- each script
resolves `utils.*` relative to its own directory, so each needs its own
copy). test_column_spec.py's test_gold_and_silver_copies_are_identical
enforces this in CI; if you're editing this file, edit the sibling copy
to match.
"""


def build_conditional_columns(spec, enabled: bool) -> list:
    """Flattens `spec` into an ordered list, dropping a conditional
    item only when its condition is unmet.

    Each entry in `spec` is either:
      - a plain value -- always included, or
      - a `(value, is_conditional)` 2-tuple -- `value` is excluded only
        when `is_conditional` is True and `enabled` is False;
        `is_conditional=False` behaves exactly like a plain value
        (always included, regardless of `enabled`).

    This keeps every column list that's gated behind the same flag
    (e.g. a SELECT projection, a MERGE's UPDATE SET, and its INSERT
    column list) built from a single, ordered spec instead of each
    being hand-written with its own `if enabled: list.append(...)`
    calls -- so adding/removing a gated column only requires editing
    one spec, and all derived lists stay aligned automatically.
    """
    result = []
    for item in spec:
        if isinstance(item, tuple) and len(item) == 2:
            value, is_conditional = item
            if is_conditional and not enabled:
                continue
        else:
            value = item
        result.append(value)
    return result
