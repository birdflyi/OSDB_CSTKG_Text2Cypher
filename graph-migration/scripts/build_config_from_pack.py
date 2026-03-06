from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build migration tool JSON config from osdb_material_pack CSV mappings."
    )
    parser.add_argument(
        "--pack-root",
        required=True,
        help="Path to materials/osdb_material_pack.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output JSON config path for migration tool.",
    )
    return parser.parse_args()


def load_node_map(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = str(row.get("raw_node_type", "")).strip().lower()
            mapped = str(row.get("normalized_label", "")).strip()
            if raw and mapped:
                out[raw] = mapped
    return out


def load_relation_rules(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_keywords = str(row.get("contains_any", "")).strip()
            mapped = str(row.get("maps_to", "")).strip()
            if not raw_keywords or not mapped:
                continue
            keywords = [k.strip() for k in raw_keywords.split("|") if k.strip()]
            rows.append(
                {
                    "contains_any": keywords,
                    "maps_to": mapped,
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    pack = Path(args.pack_root)
    node_csv = pack / "02_mappings" / "node_type_mapping.csv"
    rel_csv = pack / "02_mappings" / "relation_mapping.csv"
    if not node_csv.exists() or not rel_csv.exists():
        raise FileNotFoundError("Missing mapping CSV files under 02_mappings/.")

    payload = {
        "node_type_map": load_node_map(node_csv),
        "relation_rules": load_relation_rules(rel_csv),
    }
    out = Path(args.output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"output_path": str(out), "node_types": len(payload["node_type_map"]), "relation_rules": len(payload["relation_rules"])}, indent=2))


if __name__ == "__main__":
    main()

