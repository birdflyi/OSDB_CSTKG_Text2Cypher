from __future__ import annotations

import argparse
import json
from pathlib import Path

from data.schema import load_examples
from eval.pipeline import ExperimentRunner, RunnerConfig
from generators.factory import build_generator
from repair.base import BaseRepairModule
from repair.lightweight_repair import LightweightRepairModule
from repair.simple_repair import SimpleRepairModule


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal experiment harness for Text-to-Cypher evaluation."
    )
    parser.add_argument(
        "--examples",
        type=str,
        default="data/examples.json",
        help="Path to JSON examples.",
    )
    parser.add_argument(
        "--generator",
        type=str,
        required=True,
        choices=["free_form", "template_first", "controlled"],
        help="Generator strategy name.",
    )
    parser.add_argument(
        "--apply-repair",
        action="store_true",
        help="Enable optional repair module for invalid outputs.",
    )
    parser.add_argument(
        "--repair-module",
        type=str,
        default="lightweight",
        choices=["lightweight", "simple"],
        help="Repair module to use when --apply-repair is enabled.",
    )
    parser.add_argument(
        "--normalized-match",
        action="store_true",
        help="Use normalized-match for execution_accuracy proxy.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="eval/results.json",
        help="Output path for result JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    examples = load_examples(args.examples)
    generator = build_generator(args.generator)
    repair_module: BaseRepairModule | None = None
    if args.apply_repair:
        repair_module = (
            LightweightRepairModule()
            if args.repair_module == "lightweight"
            else SimpleRepairModule()
        )
    runner = ExperimentRunner(
        generator=generator,
        config=RunnerConfig(
            apply_repair=args.apply_repair,
            normalized_match=args.normalized_match,
        ),
        repair_module=repair_module,
    )
    report = runner.run(examples)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(report["summary"], indent=2))
    print(f"Saved report to {output_path}")


if __name__ == "__main__":
    main()
