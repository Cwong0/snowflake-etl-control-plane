from __future__ import annotations

import argparse
import sys

from sfetl.config.settings import SnowflakeSettings
from sfetl.contracts.bond_prices import BondPriceRow
from sfetl.etl.pipeline import validate_bond_prices_csv
from sfetl.snowflake.ddl import create_table_sql
from sfetl.snowflake.merge_sql import MergeSpec, build_merge_sql


def _csv_list(text: str) -> tuple[str, ...]:
    # "a,b,c" -> ("a","b","c")
    items = [x.strip() for x in text.split(",")]
    items = [x for x in items if x]
    return tuple(items)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sfetl", description="Snowflake ETL Control Plane (demo)")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="Validate bond prices CSV using Pydantic contracts")
    v.add_argument("path", help="Path to CSV file")

    m = sub.add_parser("merge-sql", help="Print a Snowflake MERGE statement")
    m.add_argument("--target", required=True, help='Target table, e.g. FIN_DB.RAW.BOND_PRICES')
    m.add_argument("--source", required=True, help='Source/stage table, e.g. FIN_DB.STAGE.BOND_PRICES_STAGE')
    m.add_argument("--keys", required=True, help='Comma list, e.g. security_id,asof_date')
    m.add_argument("--columns", required=True, help='Comma list, e.g. security_id,asof_date,price,ytm,source')
    m.add_argument("--update-columns", default=None, help="Optional comma list (defaults to non-key columns)")

    d = sub.add_parser("ddl", help="Print CREATE TABLE for BondPriceRow")
    d.add_argument("--table", required=True, help='Table name, e.g. FIN_DB.RAW.BOND_PRICES')

    s = sub.add_parser("show-settings", help="Print Snowflake settings read from .env/env (password masked)")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "validate":
        return validate_bond_prices_csv(args.path)

    if args.cmd == "merge-sql":
        keys = _csv_list(args.keys)
        cols = _csv_list(args.columns)
        upd = _csv_list(args.update_columns) if args.update_columns else None

        spec = MergeSpec(
            target=args.target,
            source=args.source,
            keys=keys,
            columns=cols,
            update_columns=upd,
        )
        print(build_merge_sql(spec))
        return 0

    if args.cmd == "ddl":
        print(create_table_sql(args.table, BondPriceRow))
        return 0

    if args.cmd == "show-settings":
        settings = SnowflakeSettings()
        print(settings.safe_dump())
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
