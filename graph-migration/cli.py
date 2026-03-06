from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

from aligners.entity_aligner import (
    probe_github_api,
    rebuild_local_indexes_safe,
    resolve_actor_entity_id_with_meta,
    resolve_repo_entity_id_with_meta,
)
from extractors.entity_mention_extractor import extract_mentions
from normalizers.real_csv_preprocess import REQUIRED_AUG_COLS, preprocess_osdb_csv
from normalizers.real_mapping_materials import build_real_mappings
from normalizers.real_pilot_materials import generate_real_pilot_materials
from runners.real_pilot_query_runner import run_real_pilot_queries
from runners.group3_template_runner import run_group3_templates
from pipeline import run_migration


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate raw OSDB graph records into a normalized query-friendly graph schema."
    )
    subparsers = parser.add_subparsers(dest="command")

    migrate = subparsers.add_parser(
        "migrate",
        help="Run core migration pipeline on raw records.",
    )
    migrate.add_argument(
        "--input",
        required=True,
        help="Input data file path.",
    )
    migrate.add_argument(
        "--input-format",
        required=True,
        choices=["json", "jsonl", "csv", "pickle"],
        help="Input format.",
    )
    migrate.add_argument(
        "--output",
        dest="output",
        default=None,
        help="Output file or directory path. Optional for memory export.",
    )
    migrate.add_argument(
        "--export-mode",
        required=True,
        choices=["memory", "json", "jsonl", "cypher", "csv"],
        help="Export mode.",
    )
    migrate.add_argument(
        "--placeholder-policy",
        default="unknown",
        choices=["skip", "unknown", "external_if_url"],
        help="How to handle placeholder IDs such as 'nan'.",
    )
    migrate.add_argument(
        "--relation-mapping-config-path",
        default=None,
        help="JSON config path containing relation_rules overrides.",
    )
    migrate.add_argument(
        "--node-mapping-config-path",
        default=None,
        help="Optional JSON config path containing node_type_map overrides.",
    )
    migrate.add_argument(
        "--csv-delimiter",
        default=",",
        help="CSV delimiter for csv input format.",
    )

    preprocess = subparsers.add_parser(
        "preprocess-real",
        help="Preprocess real OSDB CSV with Reference-aware aggregation.",
    )
    preprocess.add_argument("--input", required=True, help="Raw CSV path.")
    preprocess.add_argument("--repo-id", required=True, type=int, help="Source repo_id.")
    preprocess.add_argument("--out", required=True, help="Augmented CSV output path.")
    preprocess.add_argument(
        "--mode",
        default="full",
        choices=["full", "fast_exid"],
        help="Preprocess mode.",
    )
    preprocess.add_argument(
        "--granular-script-path",
        default=None,
        help="Optional path to data_scripts/granular_aggregation.py.",
    )
    preprocess.add_argument(
        "--exid-parser-path",
        default=None,
        help="Optional path to exid parser module (e.g. data_scripts/exid_parse_utils.py or data_scripts/data_preprocess.py) for mode=fast_exid.",
    )
    preprocess.add_argument(
        "--exid-repair-report",
        default=None,
        help="Optional output path for exid repair report markdown.",
    )

    pilot = subparsers.add_parser(
        "run-real-pilot",
        help="Run real-data pilot normalization and generate dependency materials.",
    )
    pilot.add_argument("--input", required=True, help="Augmented CSV input path.")
    pilot.add_argument("--repo-id", required=True, type=int, help="Source repo_id.")
    pilot.add_argument("--outdir", required=True, help="Pilot output directory.")
    pilot.add_argument(
        "--placeholder-policy",
        default="unknown",
        choices=["skip", "unknown", "external_if_url"],
        help="Placeholder handling policy for migration path.",
    )
    pilot.add_argument(
        "--relation-mapping-config-path",
        default=None,
        help="Optional relation mapping config JSON path.",
    )
    pilot.add_argument(
        "--node-mapping-config-path",
        default=None,
        help="Optional node mapping config JSON path.",
    )
    pilot.add_argument(
        "--materials-dir",
        default=None,
        help="Output dir for generated materials. Default: fixtures/real_pilot_redis",
    )

    mapping = subparsers.add_parser(
        "build-real-mappings",
        help="Generate dataset-specific node/relation mapping materials from augmented real CSV.",
    )
    mapping.add_argument("--input", required=True, help="Augmented real CSV input path.")
    mapping.add_argument("--outdir", required=True, help="Output directory for mapping materials.")

    pilot_queries = subparsers.add_parser(
        "run-real-pilot-queries",
        help="Run free_form/template_first/controlled on real pilot queries with static schema validation.",
    )
    pilot_queries.add_argument("--queries", required=True, help="Path to queries_pilot.jsonl.")
    pilot_queries.add_argument("--taxonomy", required=True, help="Path to query_taxonomy.yaml.")
    pilot_queries.add_argument("--schema", required=True, help="Path to schema_metadata.yaml.")
    pilot_queries.add_argument(
        "--mappings",
        required=True,
        help="Directory containing node_type_mapping.csv, relation_mapping_native_minimal.csv, placeholder_rules.yaml.",
    )
    pilot_queries.add_argument("--outdir", required=True, help="Output directory for traces/summary.")

    group3_templates = subparsers.add_parser(
        "run-group3-templates",
        help="Run Group-3 minimal template pack v2 in template_first and controlled modes with traces.",
    )
    group3_templates.add_argument("--queries", required=True, help="Path to queries_pilot.jsonl.")
    group3_templates.add_argument("--templates", required=True, help="Path to minimal_template_pack_group3_v2.yaml.")
    group3_templates.add_argument("--schema", required=True, help="Path to schema_metadata.yaml.")
    group3_templates.add_argument("--token-conf", default=None, help="Optional token config path (kept for interface compatibility).")
    group3_templates.add_argument("--api-timeout-sec", default=30, type=int, help="API timeout seconds (kept for interface compatibility).")
    group3_templates.add_argument("--outdir", required=True, help="Output directory for Group-3 traces/summary.")

    resolve_entity = subparsers.add_parser(
        "resolve-entity",
        help="Resolve repo/actor user input to canonical entity_id using local index first, API fallback second. Example: python graph-migration/cli.py resolve-entity --type repo --value \"redis/redis\" --rebuild-index",
    )
    resolve_entity.add_argument(
        "--type",
        required=True,
        choices=["repo", "actor"],
        help="Entity type to resolve.",
    )
    resolve_entity.add_argument(
        "--value",
        required=True,
        help="Input value: repo_full_name/actor_login/canonical id or explicit numeric id.",
    )
    resolve_entity.add_argument(
        "--d-record-json",
        default=None,
        help="Optional JSON object carrying structured keys like repo_id/repo_name or actor_id/actor_login.",
    )
    resolve_entity.add_argument(
        "--api-timeout-sec",
        default=12,
        type=int,
        help="API fallback timeout seconds (default: 12).",
    )
    resolve_entity.add_argument(
        "--rebuild-index",
        action="store_true",
        help="Force rebuild local indexes from source CSV before resolving (safe mode).",
    )
    resolve_entity.add_argument(
        "--index-source-csv",
        default=None,
        help="Optional source CSV path for index rebuild. Defaults to data_real/redis_redis_2023_aug_exidfix.csv.",
    )
    resolve_entity.add_argument(
        "--debug-api",
        action="store_true",
        help="Enable safe API diagnostics (token source/count/fingerprints, status, error type, elapsed).",
    )
    resolve_entity.add_argument(
        "--token-conf",
        default=None,
        help="Optional token config file path exporting GITHUB_TOKENS (e.g., data_scripts/etc/authConf.py).",
    )

    probe_github = subparsers.add_parser(
        "probe-github-api",
        help="Lightweight GitHub API reachability probe with masked token diagnostics.",
    )
    probe_github.add_argument(
        "--api-timeout-sec",
        default=3,
        type=int,
        help="Probe timeout seconds (default: 3).",
    )
    probe_github.add_argument(
        "--token-conf",
        default=None,
        help="Optional token config file path exporting GITHUB_TOKENS (e.g., data_scripts/etc/authConf.py).",
    )

    extract_resolve = subparsers.add_parser(
        "extract-and-resolve",
        help="Extract entity mentions from a sentence and resolve each mention to canonical entity_id.",
    )
    extract_resolve.add_argument(
        "--text",
        required=True,
        help="Input sentence text.",
    )
    extract_resolve.add_argument(
        "--api-timeout-sec",
        default=12,
        type=int,
        help="API fallback timeout seconds for resolver (default: 12).",
    )
    extract_resolve.add_argument(
        "--token-conf",
        default=None,
        help="Optional token config file path exporting GITHUB_TOKENS.",
    )
    extract_resolve.add_argument(
        "--d-record-json",
        default=None,
        help="Optional JSON object with contextual keys (repo_id/repo_name/actor_id/actor_login).",
    )

    return parser


def _emit_regression_fixture(
    augmented_csv_path: str, fixtures_dir: Path, sample_rows: int = 40
) -> dict[str, str]:
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(augmented_csv_path, dtype=str, keep_default_na=False)
    if len(df) > sample_rows:
        ref_df = df[df.get("relation_type", "").astype(str) == "Reference"]
        non_ref_df = df[df.get("relation_type", "").astype(str) != "Reference"]
        ref_quota = min(len(ref_df), max(sample_rows // 2, 10))
        non_ref_quota = max(sample_rows - ref_quota, 0)
        frames = []
        if ref_quota > 0:
            frames.append(ref_df.sample(ref_quota, random_state=42))
        if non_ref_quota > 0 and len(non_ref_df) > 0:
            frames.append(non_ref_df.sample(min(non_ref_quota, len(non_ref_df)), random_state=42))
        sample_df = (
            pd.concat(frames, axis=0).sample(frac=1.0, random_state=42)
            if frames
            else df.head(sample_rows)
        )
    else:
        sample_df = df

    mini_sample_path = fixtures_dir / "mini_sample.csv"
    sample_df.to_csv(mini_sample_path, index=False, encoding="utf-8")

    expected_cols = list(sample_df.columns)
    for col in REQUIRED_AUG_COLS:
        if col not in expected_cols:
            expected_cols.append(col)
    expected_cols_path = fixtures_dir / "mini_sample_expected_cols.txt"
    expected_cols_path.write_text("\n".join(expected_cols) + "\n", encoding="utf-8")
    return {
        "mini_sample_csv": str(mini_sample_path),
        "mini_sample_expected_cols": str(expected_cols_path),
    }


def _run_migrate(args: argparse.Namespace) -> dict:
    return run_migration(
        input_path=args.input,
        input_format=args.input_format,
        output_path=args.output,
        export_mode=args.export_mode,
        placeholder_policy=args.placeholder_policy,
        relation_mapping_config_path=args.relation_mapping_config_path,
        node_mapping_config_path=args.node_mapping_config_path,
        csv_delimiter=args.csv_delimiter,
    )


def _run_preprocess_real(args: argparse.Namespace) -> dict:
    default_report = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "real_pilot_redis"
        / "exid_repair_report.md"
    )
    report_path = args.exid_repair_report if getattr(args, "exid_repair_report", None) else str(default_report)
    df_aug = preprocess_osdb_csv(
        input_csv_path=args.input,
        repo_id=args.repo_id,
        out_csv_path=args.out,
        mode=args.mode,
        granular_script_path=args.granular_script_path,
        exid_utils_path=args.exid_parser_path,
        exid_repair_report_path=report_path,
    )
    return {
        "command": "preprocess-real",
        "input": args.input,
        "repo_id": args.repo_id,
        "mode": args.mode,
        "output": args.out,
        "exid_repair_report": report_path,
        "row_count": int(len(df_aug)),
        "columns": list(df_aug.columns),
    }


def _run_real_pilot(args: argparse.Namespace) -> dict:
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    normalized_json_path = outdir / "normalized_graph.json"
    normalized_cypher_path = outdir / "import.cypher"
    normalized_csv_dir = outdir / "csv"

    migration_json = run_migration(
        input_path=args.input,
        input_format="csv",
        output_path=str(normalized_json_path),
        export_mode="json",
        placeholder_policy=args.placeholder_policy,
        relation_mapping_config_path=args.relation_mapping_config_path,
        node_mapping_config_path=args.node_mapping_config_path,
        csv_delimiter=",",
    )
    migration_cypher = run_migration(
        input_path=args.input,
        input_format="csv",
        output_path=str(normalized_cypher_path),
        export_mode="cypher",
        placeholder_policy=args.placeholder_policy,
        relation_mapping_config_path=args.relation_mapping_config_path,
        node_mapping_config_path=args.node_mapping_config_path,
        csv_delimiter=",",
    )
    migration_csv = run_migration(
        input_path=args.input,
        input_format="csv",
        output_path=str(normalized_csv_dir),
        export_mode="csv",
        placeholder_policy=args.placeholder_policy,
        relation_mapping_config_path=args.relation_mapping_config_path,
        node_mapping_config_path=args.node_mapping_config_path,
        csv_delimiter=",",
    )
    materials_dir = (
        Path(args.materials_dir)
        if args.materials_dir
        else Path(__file__).resolve().parent / "fixtures" / "real_pilot_redis"
    )
    material_paths = generate_real_pilot_materials(
        augmented_csv_path=args.input,
        output_dir=str(materials_dir),
        sample_size=200,
        random_seed=42,
    )
    regression_paths = _emit_regression_fixture(
        augmented_csv_path=args.input,
        fixtures_dir=materials_dir,
        sample_rows=40,
    )
    return {
        "command": "run-real-pilot",
        "input": args.input,
        "repo_id": args.repo_id,
        "outdir": str(outdir),
        "migration_json": migration_json,
        "migration_cypher": migration_cypher,
        "migration_csv": migration_csv,
        "materials": material_paths,
        "regression_fixture": regression_paths,
    }


def _run_build_real_mappings(args: argparse.Namespace) -> dict:
    result = build_real_mappings(
        input_csv_path=args.input,
        outdir=args.outdir,
    )
    return {
        "command": "build-real-mappings",
        **result,
    }


def _run_real_pilot_queries(args: argparse.Namespace) -> dict:
    result = run_real_pilot_queries(
        queries_path=args.queries,
        taxonomy_path=args.taxonomy,
        schema_path=args.schema,
        mappings_dir=args.mappings,
        outdir=args.outdir,
    )
    return {
        "command": "run-real-pilot-queries",
        **result,
    }


def _run_group3_templates(args: argparse.Namespace) -> dict:
    result = run_group3_templates(
        queries_path=args.queries,
        templates_path=args.templates,
        schema_path=args.schema,
        outdir=args.outdir,
        token_conf=args.token_conf,
        api_timeout_sec=args.api_timeout_sec,
    )
    return {
        "command": "run-group3-templates",
        "queries": args.queries,
        "templates": args.templates,
        "schema": args.schema,
        "token_conf": args.token_conf,
        "api_timeout_sec": args.api_timeout_sec,
        **result,
    }


def _run_resolve_entity(args: argparse.Namespace) -> dict:
    rebuild_meta = {
        "index_rebuilt": False,
        "rebuilt_indexes": [],
        "index_rows_summary": {},
        "index_rebuild_error": None,
    }
    if getattr(args, "rebuild_index", False):
        reb = rebuild_local_indexes_safe(clean_csv_path=args.index_source_csv)
        rebuild_meta["index_rebuilt"] = bool(reb.get("index_rebuilt", False))
        rebuild_meta["rebuilt_indexes"] = list(reb.get("rebuilt_indexes", []))
        rebuild_meta["index_rows_summary"] = dict(reb.get("rows_written", {}))
        if not reb.get("ok", False):
            rebuild_meta["index_rebuild_error"] = reb.get("error") or "rebuild_failed"

    d_record = None
    if args.d_record_json:
        try:
            loaded = json.loads(args.d_record_json)
            if isinstance(loaded, dict):
                d_record = loaded
            else:
                return {
                    "command": "resolve-entity",
                    "entity_type": args.type,
                    "input_value": args.value,
                    "entity_id": None,
                    "provenance": "failed",
                    "api_called": False,
                    "error": "d_record_json_must_be_object",
                    **rebuild_meta,
                }
        except Exception as exc:
            return {
                "command": "resolve-entity",
                "entity_type": args.type,
                "input_value": args.value,
                "entity_id": None,
                "provenance": "failed",
                "api_called": False,
                "error": f"invalid_d_record_json:{type(exc).__name__}",
                **rebuild_meta,
            }

    if args.type == "repo":
        result = resolve_repo_entity_id_with_meta(
            input_str=args.value,
            d_record=d_record,
            api_timeout_sec=args.api_timeout_sec,
            debug_api=bool(getattr(args, "debug_api", False)),
            token_conf_path=getattr(args, "token_conf", None),
        )
    else:
        result = resolve_actor_entity_id_with_meta(
            input_str=args.value,
            d_record=d_record,
            api_timeout_sec=args.api_timeout_sec,
            debug_api=bool(getattr(args, "debug_api", False)),
            token_conf_path=getattr(args, "token_conf", None),
        )

    payload = result.to_dict()
    payload.update(
        {
            "command": "resolve-entity",
            "entity_type": args.type,
            "input_value": args.value,
            "timeout_sec": args.api_timeout_sec,
            "debug_api_enabled": bool(getattr(args, "debug_api", False)),
            "token_conf": getattr(args, "token_conf", None),
            **rebuild_meta,
        }
    )
    return payload


def _run_probe_github_api(args: argparse.Namespace) -> dict:
    probe = probe_github_api(
        api_timeout_sec=args.api_timeout_sec,
        token_conf_path=args.token_conf,
    )
    return {
        "command": "probe-github-api",
        "timeout_sec": args.api_timeout_sec,
        "token_conf": args.token_conf,
        **probe,
    }


def _run_extract_and_resolve(args: argparse.Namespace) -> dict:
    d_record = None
    if args.d_record_json:
        try:
            loaded = json.loads(args.d_record_json)
            if isinstance(loaded, dict):
                d_record = loaded
        except Exception:
            d_record = None

    mentions = extract_mentions(args.text or "")
    resolved_entities: list[dict] = []
    for mention in mentions:
        hint_type = str(mention.get("hint_type") or "")
        normalized = str(mention.get("normalized_text") or "")
        resolver_target = None
        if hint_type.startswith("repo_"):
            resolver_target = "repo"
        elif hint_type.startswith("actor_"):
            resolver_target = "actor"

        if resolver_target == "repo":
            rs = resolve_repo_entity_id_with_meta(
                input_str=normalized,
                d_record=d_record,
                api_timeout_sec=args.api_timeout_sec,
                debug_api=False,
                token_conf_path=getattr(args, "token_conf", None),
            ).to_dict()
        elif resolver_target == "actor":
            rs = resolve_actor_entity_id_with_meta(
                input_str=normalized,
                d_record=d_record,
                api_timeout_sec=args.api_timeout_sec,
                debug_api=False,
                token_conf_path=getattr(args, "token_conf", None),
            ).to_dict()
        else:
            rs = {
                "entity_id": None,
                "provenance": "failed",
                "api_called": False,
                "error": "unsupported_hint_type",
            }

        resolved_entities.append(
            {
                "mention": mention,
                "resolver_target": resolver_target,
                **rs,
            }
        )

    unique_entity_ids: list[str] = []
    for item in resolved_entities:
        eid = item.get("entity_id")
        if isinstance(eid, str) and eid and eid not in unique_entity_ids:
            unique_entity_ids.append(eid)

    return {
        "command": "extract-and-resolve",
        "text": args.text,
        "mentions_extracted": len(mentions),
        "mentions": mentions,
        "resolved_entities": resolved_entities,
        "unique_entity_ids": unique_entity_ids,
        "unique_entity_count": len(unique_entity_ids),
        "timeout_sec": args.api_timeout_sec,
        "token_conf": getattr(args, "token_conf", None),
    }


def main() -> None:
    argv = sys.argv[1:]
    # Legacy invocation compatibility:
    # python graph-migration/cli.py --input ... --input-format ... --export-mode ...
    if argv and argv[0].startswith("-") and argv[0] not in {"-h", "--help"}:
        argv = ["migrate", *argv]

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "migrate":
        result = _run_migrate(args)
    elif args.command == "preprocess-real":
        result = _run_preprocess_real(args)
    elif args.command == "run-real-pilot":
        result = _run_real_pilot(args)
    elif args.command == "build-real-mappings":
        result = _run_build_real_mappings(args)
    elif args.command == "run-real-pilot-queries":
        result = _run_real_pilot_queries(args)
    elif args.command == "run-group3-templates":
        result = _run_group3_templates(args)
    elif args.command == "resolve-entity":
        result = _run_resolve_entity(args)
    elif args.command == "probe-github-api":
        result = _run_probe_github_api(args)
    elif args.command == "extract-and-resolve":
        result = _run_extract_and_resolve(args)
    else:
        parser.print_help()
        return

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
