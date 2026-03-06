from __future__ import annotations

import argparse
import json
from pathlib import Path

from data.loaders import load_examples, load_graph_metadata
from eval.reporting import write_json
from generators.factory import baseline_and_method_names, build_generator
from repair.base import BaseRepairModule
from repair.lightweight_repair import LightweightRepairModule
from repair.simple_repair import SimpleRuleRepair
from runners.experiment_runner import ExperimentRunner, RunnerConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal Text-to-Cypher evaluation harness over normalized graph data."
    )
    parser.add_argument(
        "--examples-path",
        type=str,
        default="examples/query_examples.json",
        help="Path to normalized query examples JSON.",
    )
    parser.add_argument(
        "--graph-metadata-path",
        type=str,
        default="graph-migration/outputs/normalized_graph.json",
        help="Path to migrated graph JSON or explicit schema metadata JSON.",
    )
    parser.add_argument(
        "--generator",
        type=str,
        default="controlled",
        choices=baseline_and_method_names(),
        help="Generator to evaluate for single-run mode.",
    )
    parser.add_argument(
        "--compare-all",
        action="store_true",
        help="Run free_form/template_first/controlled and save comparable outputs.",
    )
    parser.add_argument(
        "--apply-repair",
        action="store_true",
        help="Apply optional repair module on invalid outputs.",
    )
    parser.add_argument(
        "--repair-module",
        type=str,
        default="lightweight",
        choices=["lightweight", "simple"],
        help="Repair module selection when --apply-repair is enabled.",
    )
    parser.add_argument(
        "--normalized-match",
        action="store_true",
        help="Use normalized text match proxy for execution accuracy.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="outputs/report.json",
        help="Single-run output JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/compare",
        help="Compare-all output directory.",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Optional config JSON to override apply_repair/normalized_match.",
    )
    return parser.parse_args()


def load_runtime_config(args: argparse.Namespace) -> RunnerConfig:
    config = RunnerConfig(
        apply_repair=args.apply_repair,
        normalized_match=args.normalized_match,
    )
    if not args.config_path:
        return config
    payload = json.loads(Path(args.config_path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        config.apply_repair = bool(payload.get("apply_repair", config.apply_repair))
        config.normalized_match = bool(
            payload.get("normalized_match", config.normalized_match)
        )
    return config


def run_single(args: argparse.Namespace) -> dict:
    examples = load_examples(args.examples_path)
    graph_metadata = load_graph_metadata(args.graph_metadata_path)
    config = load_runtime_config(args)
    repair_module: BaseRepairModule | None = None
    if config.apply_repair:
        repair_module = (
            LightweightRepairModule()
            if args.repair_module == "lightweight"
            else SimpleRuleRepair()
        )
    runner = ExperimentRunner(
        graph_metadata=graph_metadata,
        generator=build_generator(args.generator),
        config=config,
        repair_module=repair_module,
    )
    report = runner.run(examples)
    write_json(args.output_path, report)
    return report


def run_compare_all(args: argparse.Namespace) -> dict:
    examples = load_examples(args.examples_path)
    graph_metadata = load_graph_metadata(args.graph_metadata_path)
    config = load_runtime_config(args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    compare_report: dict[str, dict] = {"methods": {}}

    for method_name in baseline_and_method_names():
        repair_module: BaseRepairModule | None = None
        if config.apply_repair:
            repair_module = (
                LightweightRepairModule()
                if args.repair_module == "lightweight"
                else SimpleRuleRepair()
            )
        runner = ExperimentRunner(
            graph_metadata=graph_metadata,
            generator=build_generator(method_name),
            config=config,
            repair_module=repair_module,
        )
        report = runner.run(examples)
        method_path = output_dir / f"{method_name}.json"
        write_json(method_path, report)
        compare_report["methods"][method_name] = report["summary"]

    write_json(output_dir / "summary_compare.json", compare_report)
    return compare_report


def main() -> None:
    args = parse_args()
    if args.compare_all:
        result = run_compare_all(args)
    else:
        result = run_single(args)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
