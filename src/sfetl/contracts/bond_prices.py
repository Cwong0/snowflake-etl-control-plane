from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class BondPriceRow(BaseModel):
    """
    Data contract for a bond price record.
    This is the schema boundary between raw input and the pipeline.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    security_id: str = Field(min_length=3)
    asof_date: date
    price: Decimal = Field(ge=Decimal("0.0"))
    ytm: Decimal | None = Field(default=None, ge=Decimal("0.0"))  # yield to maturity
    source: str = Field(default="demo", min_length=2)
