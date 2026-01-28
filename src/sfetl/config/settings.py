from __future__ import annotations

from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    """
    Reads Snowflake connection values from environment variables and/or a .env file.

    We keep fields optional so commands like `show-settings` can run even if not fully configured.
    Use `require_complete()` before actually connecting.
    """

    account: str | None = None
    user: str | None = None
    password: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema: str | None = None
    role: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def missing_required(self) -> list[str]:
        required = ["account", "user", "password", "warehouse", "database", "schema"]
        missing = [k for k in required if getattr(self, k) in (None, "")]
        return missing

    def require_complete(self) -> None:
        missing = self.missing_required()
        if missing:
            raise ValueError(f"Missing required Snowflake settings: {missing}")

    def as_connect_kwargs(self) -> dict[str, Any]:
        self.require_complete()
        kw: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.role:
            kw["role"] = self.role
        return kw

    def safe_dump(self) -> dict[str, Any]:
        d = self.model_dump()
        if d.get("password"):
            d["password"] = "***"
        d["missing_required"] = self.missing_required()
        return d
