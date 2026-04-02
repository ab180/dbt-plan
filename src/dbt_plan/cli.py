"""CLI entry point for dbt-plan."""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="dbt-plan",
        description="Preview what DDL changes dbt run will execute on Snowflake",
    )
    subparsers = parser.add_subparsers(dest="command")

    # snapshot
    snap = subparsers.add_parser(
        "snapshot", help="Save current compiled state as baseline"
    )
    snap.add_argument(
        "--project-dir", default=".", help="dbt project directory (default: .)"
    )

    # check
    check = subparsers.add_parser(
        "check", help="Diff compiled SQL and predict DDL impact"
    )
    check.add_argument(
        "--project-dir", default=".", help="dbt project directory (default: .)"
    )
    check.add_argument(
        "--format",
        choices=["text", "github"],
        default="text",
        help="Output format (default: text)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "snapshot":
        print("dbt-plan snapshot: not yet implemented")
        sys.exit(0)

    if args.command == "check":
        print("dbt-plan check: not yet implemented")
        sys.exit(0)


if __name__ == "__main__":
    main()
