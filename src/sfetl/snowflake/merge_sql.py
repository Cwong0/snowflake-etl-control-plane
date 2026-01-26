from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MergeSpec:
    target: str
    source: str
    keys: tuple[str, ...]
    columns: tuple[str, ...]
    update_columns: tuple[str, ...] | None = None


def _quote_ident(ident: str) -> str:
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _csv_idents(cols: Iterable[str]) -> str:
    return ", ".join(_quote_ident(c) for c in cols)


def build_merge_sql(spec: MergeSpec) -> str:
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

    on_clause = " AND ".join([f't.{_quote_ident(k)} = s.{_quote_ident(k)}' for k in spec.keys])

    if update_cols:
        update_clause = "UPDATE SET " + ", ".join([f't.{_quote_ident(c)} = s.{_quote_ident(c)}' for c in update_cols])
    else:
        update_clause = "UPDATE SET " + f't.{_quote_ident(spec.keys[0])} = t.{_quote_ident(spec.keys[0])}'

    insert_cols = _csv_idents(spec.columns)
    values_cols = ", ".join([f's.{_quote_ident(c)}' for c in spec.columns])

    return f'''MERGE INTO {spec.target} AS t
USING {spec.source} AS s
ON {on_clause}
WHEN MATCHED THEN {update_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({values_cols});'''
