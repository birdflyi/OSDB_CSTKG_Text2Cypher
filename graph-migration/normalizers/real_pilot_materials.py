from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def _looks_like_object_string(value: str) -> bool:
    text = value.strip()
    return (text.startswith("{") and text.endswith("}")) or (
        text.startswith("[") and text.endswith("]")
    )


def _guess_parse_hint(series: pd.Series) -> str:
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return "str"
    sample = non_null.head(50)
    if sample.str.match(r"^-?\d+$").mean() > 0.8:
        return "int"
    if sample.str.match(r"^-?\d+(\.\d+)?$").mean() > 0.8:
        return "float"
    if sample.str.contains(r"^\d{4}-\d{2}-\d{2}", regex=True).mean() > 0.5:
        return "datetime"
    if sample.map(_looks_like_object_string).mean() > 0.5:
        return "json_like"
    return "str"


def _observed_missing_sentinels(series: pd.Series) -> str:
    defaults = {"nan", "none", ""}
    observed = set()
    for val in series.astype(str).head(5000):
        norm = val.strip().lower()
        if norm in {"", "nan", "none", "null", "na", "n/a", "unknown"}:
            observed.add(norm)
    merged = sorted(defaults.union(observed))
    return "|".join(merged)


def _choose_example(series: pd.Series) -> str:
    for val in series:
        if pd.isna(val):
            continue
        text = str(val).strip()
        if text:
            return text[:200]
    return ""


def generate_real_pilot_materials(
    augmented_csv_path: str,
    output_dir: str,
    sample_size: int = 200,
    random_seed: int = 42,
    rare_tail_n: int = 10,
) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(augmented_csv_path, dtype=str, keep_default_na=False)

    for col in ["relation_type", "tar_entity_type_fine_grained"]:
        if col not in df.columns:
            df[col] = ""

    ref_df = df[df["relation_type"] == "Reference"].copy()
    non_ref_df = df[df["relation_type"] != "Reference"].copy()

    if len(df) <= sample_size:
        sample_df = df.copy()
    else:
        if len(ref_df) == 0:
            sample_df = df.sample(sample_size, random_state=random_seed)
        else:
            ref_quota = min(len(ref_df), max(sample_size // 2, 30))
            non_ref_quota = max(sample_size - ref_quota, 0)
            ref_sample = ref_df.sample(ref_quota, random_state=random_seed)
            if non_ref_quota > 0 and len(non_ref_df) > 0:
                non_ref_sample = non_ref_df.sample(
                    min(non_ref_quota, len(non_ref_df)),
                    random_state=random_seed,
                )
                sample_df = pd.concat([ref_sample, non_ref_sample], axis=0).sample(
                    frac=1.0, random_state=random_seed
                )
            else:
                sample_df = ref_sample

    raw_jsonl_path = out_dir / "raw_records_sample.jsonl"
    with raw_jsonl_path.open("w", encoding="utf-8") as handle:
        for _, row in sample_df.iterrows():
            payload = {
                str(col): (None if pd.isna(val) else val)
                for col, val in row.to_dict().items()
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    dict_rows: list[dict[str, Any]] = []
    for col in df.columns:
        series = df[col]
        non_null = series.replace("", pd.NA).dropna()
        nullable = bool(non_null.shape[0] < df.shape[0])
        example = _choose_example(series)
        is_obj = bool(non_null.astype(str).head(100).map(_looks_like_object_string).any())
        parse_hint = _guess_parse_hint(series)
        notes = ""
        keep_or_drop = "keep"
        if any(token in col.lower() for token in ["match_text", "match_pattern"]):
            notes = "auxiliary matching signal; review if needed downstream"
        if col.lower().endswith("_id") and "event" not in col.lower():
            notes = (notes + "; " if notes else "") + "entity identifier-like field"
        dict_rows.append(
            {
                "field_name": col,
                "level": "row",
                "meaning": "TBD",
                "example": example,
                "is_required": "no",
                "nullable": "yes" if nullable else "no",
                "missing_sentinels": _observed_missing_sentinels(series),
                "is_stringified_object": "true" if is_obj else "false",
                "parse_hint": parse_hint,
                "keep_or_drop": keep_or_drop,
                "notes": notes,
            }
        )
    field_dict_path = out_dir / "field_dictionary.csv"
    pd.DataFrame(dict_rows).to_csv(field_dict_path, index=False, encoding="utf-8")

    ref_stats = ref_df.copy()
    ref_stats["tar_entity_type_fine_grained"] = (
        ref_stats["tar_entity_type_fine_grained"].fillna("").astype(str).replace("", "__MISSING__")
    )
    stats_df = (
        ref_stats["tar_entity_type_fine_grained"]
        .value_counts(dropna=False)
        .rename_axis("tar_entity_type_fine_grained")
        .reset_index(name="count")
    )
    total = float(stats_df["count"].sum()) if not stats_df.empty else 1.0
    stats_df["ratio"] = stats_df["count"] / total
    stats_df["is_rare_tail"] = False
    if not stats_df.empty:
        tail_labels = set(stats_df.sort_values("count", ascending=True).head(rare_tail_n)["tar_entity_type_fine_grained"])
        stats_df.loc[
            stats_df["tar_entity_type_fine_grained"].isin(tail_labels), "is_rare_tail"
        ] = True
    stats_path = out_dir / "reference_tar_entity_type_fine_grained_stats.csv"
    stats_df.to_csv(stats_path, index=False, encoding="utf-8")

    return {
        "raw_records_sample_jsonl": str(raw_jsonl_path),
        "field_dictionary_csv": str(field_dict_path),
        "reference_type_stats_csv": str(stats_path),
    }

