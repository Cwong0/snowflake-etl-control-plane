from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MergeSpec:
    """
    Defines how to MERGE a staged dataset into a target table.

    target: fully-qualified name of target table (e.g., "FIN_DB.RAW.BOND_PRICES")
    source: fully-qualified name of staging/source table (e.g., "FIN_DB.STAGE.BOND_PRICES_STAGE")
    keys: columns that uniquely identify a row (used in the ON clause)
    columns: all columns to consider for insert/update
    update_columns: columns to update when matched (defaults to non-key columns)
    """

    target: str
    source: str
    keys: tuple[str, ...]
    columns: tuple[str, ...]
    update_columns: tuple[str, ...] | None = None


def _quote_ident(ident: str) -> str:
    """
    Snowflake identifier quoting.
    For simplicity we quote each identifier with double-quotes and escape internal quotes.
    """
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _csv_idents(cols: Iterable[str]) -> str:
    return ", ".join(_quote_ident(c) for c in cols)


def build_merge_sql(spec: MergeSpec) -> str:
    """
    Build a Snowflake MERGE statement.

    Output pattern:
      MERGE INTO <target> AS t
      USING <source> AS s
      ON t.<key1> = s.<key1> AND ...
      WHEN MATCHED THEN UPDATE SET <col> = s.<col>, ...
      WHEN NOT MATCHED THEN INSERT (<cols...>) VALUES (<s.cols...>);

    Notes:
    - We update ONLY update_columns (defaults to non-key columns)
    - We insert all spec.columns
    """
    if not spec.keys:
        raise ValueError("MergeSpec.keys must not be empty")

    if not spec.columns:
        raise ValueError("MergeSpec.columns must not be empty")

    cols_set = set(spec.columns)
    missing_keys = [k for k in spec.keys if k not in cols_set]
    if missing_keys:
        raise ValueError(f"Keys must be included in columns. Missing: {missing_keys}")

    update_cols = spec.update_columns
    if update_cols is None:
        update_cols = tuple(c for c in spec.columns if c not in spec.keys)

    # ON clause (match keys)
    on_parts = [
        f"t.{_quote_ident(k)} = s.{_quote_ident(k)}"
        for k in spec.keys
    ]
    on_clause = " AND ".join(on_parts)

    # UPDATE SET clause
    if update_cols:
        set_parts = [
            f"t.{_quote_ident(c)} = s.{_quote_ident(c)}"
            for c in update_cols
        ]
        update_clause = "UPDATE SET " + ", ".join(set_parts)
    else:
        # Rare case: only keys exist; nothing to update
        update_clause = "UPDATE SET " + "/* no non-key columns */ t." + _quote_ident(spec.keys[0]) + " = t." + _quote_ident(spec.keys[0])

    # INSERT clause
    insert_cols = _csv_idents(spec.columns)
    values_cols = ", ".join(f"s.{_quote_ident(c)}" for c in spec.columns)

    sql = f"""MERGE INTO {spec.target} AS t
USING {spec.source} AS s
ON {on_clause}
WHEN MATCHED THEN {update_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({values_cols});"""

    return sql
