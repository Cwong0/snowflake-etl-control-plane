from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from sfetl.contracts.bond_prices import BondPriceRow


def read_csv_rows(path: str | Path) -> list[dict[str, Any]]:
    """Extract step: read CSV rows as dictionaries (values start as strings)."""
    p = Path(path)
    with p.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def validate_bond_prices(rows: list[dict[str, Any]]) -> list[BondPriceRow]:
    """Validate step: convert raw dict rows into strongly-typed BondPriceRow objects."""
    adapter = TypeAdapter(list[BondPriceRow])
    return adapter.validate_python(rows)


def format_validation_errors(e: ValidationError) -> list[dict[str, Any]]:
    """Create a simple error report: row index + field + message."""
    out: list[dict[str, Any]] = []
    for err in e.errors():
        loc = err.get("loc", ())
        row_index = loc[0] if loc and isinstance(loc[0], int) else None
        field = loc[1] if len(loc) > 1 else None
        out.append(
            {
                "row": row_index,
                "field": field,
                "message": err.get("msg"),
                "type": err.get("type"),
            }
        )
    return out


def validate_bond_prices_csv(path: str | Path) -> int:
    """
    End-to-end demo:
    - Extract CSV
    - Validate rows using Pydantic contracts
    - Print summary and first row (or a structured error report)
    """
    rows = read_csv_rows(path)

    try:
        validated = validate_bond_prices(rows)
    except ValidationError as e:
        errors = format_validation_errors(e)
        print(f"Validation failed: {len(errors)} issue(s)")
        for item in errors[:20]:
            print(item)
        return 1

    print(f"Validation succeeded: {len(validated)} row(s)")
    if validated:
        print("Example row:", validated[0].model_dump())
    return 0
