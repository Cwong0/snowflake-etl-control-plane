from __future__ import annotations

import textwrap

from sfetl.snowflake.merge_sql import MergeSpec, build_merge_sql


def test_build_merge_sql_basic():
    spec = MergeSpec(
        target="FIN_DB.RAW.BOND_PRICES",
        source="FIN_DB.STAGE.BOND_PRICES_STAGE",
        keys=("security_id", "asof_date"),
        columns=("security_id", "asof_date", "price", "ytm", "source"),
    )

    sql = build_merge_sql(spec)

    expected = textwrap.dedent(
        """\
        MERGE INTO FIN_DB.RAW.BOND_PRICES AS t
        USING FIN_DB.STAGE.BOND_PRICES_STAGE AS s
        ON t."security_id" = s."security_id" AND t."asof_date" = s."asof_date"
        WHEN MATCHED THEN UPDATE SET t."price" = s."price", t."ytm" = s."ytm", t."source" = s."source"
        WHEN NOT MATCHED THEN INSERT ("security_id", "asof_date", "price", "ytm", "source")
        VALUES (s."security_id", s."asof_date", s."price", s."ytm", s."source");"""
    )

    assert sql == expected


def test_build_merge_sql_requires_keys():
    spec = MergeSpec(target="X", source="Y", keys=(), columns=("a",))
    try:
        build_merge_sql(spec)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "keys" in str(e).lower()


def test_keys_must_be_in_columns():
    spec = MergeSpec(target="X", source="Y", keys=("id",), columns=("name",))
    try:
        build_merge_sql(spec)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "missing" in str(e).lower()
