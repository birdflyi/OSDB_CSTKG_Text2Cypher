"""Read the same Table 1 source for planning and reports; no model dispatch."""

import argparse
import json
from pathlib import Path

from . import table
from .journal import write_status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "report", "validate"))
    parser.add_argument("table", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = table.load(args.table)
    result = table.report(value) if args.command == "report" else table.plan(value)
    if args.output:
        write_status(args.output, result)
    print(
        json.dumps(
            result if args.command != "validate" else dict(valid=True, total=result["total"]),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
