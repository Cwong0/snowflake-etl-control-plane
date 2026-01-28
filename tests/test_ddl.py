from __future__ import annotations

from sfetl.contracts.bond_prices import BondPriceRow
from sfetl.snowflake.ddl import create_table_sql


def test_create_table_sql_from_bond_price_row():
    sql = create_table_sql("FIN_DB.RAW.BOND_PRICES", BondPriceRow)

    # Key expectations (keeps test stable but not overly brittle)
    assert "CREATE TABLE IF NOT EXISTS FIN_DB.RAW.BOND_PRICES" in sql
    assert '"security_id" VARCHAR NOT NULL' in sql
    assert '"asof_date" DATE NOT NULL' in sql
    assert '"price" NUMBER(38,9) NOT NULL' in sql
    assert '"ytm" NUMBER(38,9) NULL' in sql  # optional
    assert '"source" VARCHAR NOT NULL' in sql
