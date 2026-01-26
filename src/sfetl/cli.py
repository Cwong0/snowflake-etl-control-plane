from __future__ import annotations

import argparse
import sys

from sfetl.etl.pipeline import validate_bond_prices_csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sfetl", description="Snowflake ETL Control Plane (demo)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="Validate bond prices CSV using Pydantic contracts")
    v.add_argument("path", help="Path to CSV file")

    args = parser.parse_args(argv)

    if args.cmd == "validate":
        return validate_bond_prices_csv(args.path)

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
