from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, get_args, get_origin

from pydantic import BaseModel


def _unwrap_optional(tp: Any) -> tuple[Any, bool]:
    """
    Returns (inner_type, is_optional).
    Supports Optional[T] and T | None.
    """
    origin = get_origin(tp)
    if origin is None:
        return tp, False

    args = get_args(tp)
    # Optional[T] becomes Union[T, NoneType]
    if len(args) == 2 and type(None) in args:
        inner = args[0] if args[1] is type(None) else args[1]
        return inner, True

    return tp, False


def snowflake_type_for(py_type: Any) -> str:
    """
    Minimal mapping from Python types to Snowflake column types.
    Good enough for interview-level ETL scaffolding.
    """
    py_type, _ = _unwrap_optional(py_type)

    if py_type is str:
        return "VARCHAR"
    if py_type is int:
        return "NUMBER(38,0)"
    if py_type is float:
        return "FLOAT"
    if py_type is bool:
        return "BOOLEAN"
    if py_type is date:
        return "DATE"
    if py_type is datetime:
        return "TIMESTAMP_NTZ"
    if py_type is Decimal:
        # common default for numeric measures
        return "NUMBER(38,9)"

    # fallback for complex/unmodeled shapes
    return "VARIANT"


def create_table_sql(table: str, model: type[BaseModel], *, if_not_exists: bool = True) -> str:
    """
    Generate a Snowflake CREATE TABLE statement from a Pydantic model.
    Uses Optional[T] to decide NULL vs NOT NULL.
    """
    ine = "IF NOT EXISTS " if if_not_exists else ""
    lines: list[str] = [f"CREATE TABLE {ine}{table} ("]

    fields = model.model_fields
    col_lines: list[str] = []

    for name, field in fields.items():
        anno = field.annotation
        inner, is_opt = _unwrap_optional(anno)
        sf_type = snowflake_type_for(inner)
        null_sql = "NULL" if is_opt else "NOT NULL"
        col_lines.append(f'  "{name}" {sf_type} {null_sql}')

    lines.append(",\n".join(col_lines))
    lines.append(");")
    return "\n".join(lines)
