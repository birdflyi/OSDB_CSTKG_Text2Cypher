"""Offline stratified selection. Never opens model outputs or a database.

Each nonempty stratum receives one place, then remaining places are allocated
in proportion to its remaining population. Sampling uses seeded SHA-256 ranks.
The output is a position index, not a model request or an execution admission.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def allocate(counts, target, minimums=None):
    """Allocate the floor first, then proportionally distribute remaining capacity."""
    if type(target) is not int or target < 1:
        raise ValueError("Target must be a positive integer")
    if not counts or any(type(n) is not int or n < 1 for n in counts.values()):
        raise ValueError("Stratum populations must be positive integers")
    floors = dict.fromkeys(counts, 1) if minimums is None else dict(minimums)
    if set(floors) != set(counts) or any(
        type(n) is not int or not 1 <= n <= counts[key] for key, n in floors.items()
    ):
        raise ValueError("Invalid stratum minimums")
    if not sum(floors.values()) <= target <= sum(counts.values()):
        raise ValueError("Target cannot cover all strata or exceeds population")
    allocated = dict(floors)
    remaining = target - sum(floors.values())
    capacity = sum(counts[key] - floors[key] for key in counts)
    if not remaining:
        return allocated
    remainders = []
    for key in sorted(counts):
        whole, remainder = divmod(remaining * (counts[key] - floors[key]), capacity)
        allocated[key] += whole
        remainders.append((remainder, key))
    left = target - sum(allocated.values())
    for _, key in sorted(remainders, key=lambda item: (-item[0], item[1]))[:left]:
        allocated[key] += 1
    return allocated


def select_population(rows, policy):
    """Return (selected position records, stratum counts) from pinned metadata."""
    mode = policy.get("mode")
    if mode not in ("full_release", "fixed_stratified"):
        raise ValueError("Independent authored tasks require their own audited source manifest")
    seed = policy.get("seed")
    if type(seed) is not int or seed < 0:
        raise ValueError("Provide a nonnegative integer seed")
    benchmark = policy.get("benchmark")
    if not isinstance(benchmark, str) or not benchmark:
        raise ValueError("Provide a benchmark identity")
    if mode == "fixed_stratified" and (type(policy.get("target")) is not int or policy["target"] < 1):
        raise ValueError("Target must be a positive integer")
    fields = policy.get("primary_fields")
    if not isinstance(fields, list) or not fields or len(set(fields)) != len(fields):
        raise ValueError("Provide distinct primary fields")
    if any(f not in ("graph", "split", "source_group") for f in fields):
        raise ValueError("Primary strata may only use graph, split and source_group")
    groups = defaultdict(list)
    seen = set()
    for row in rows:
        identity = row.get("position_id")
        if not isinstance(identity, str) or not identity or identity in seen:
            raise ValueError("Missing or duplicate source position ID")
        seen.add(identity)
        if row.get("benchmark") != benchmark:
            raise ValueError("Mixed or incorrect benchmark")
        if any(not isinstance(row.get(f), str) or not row[f] for f in ("task_id", "graph")):
            raise ValueError("Missing task or graph identity")
        values = tuple(row.get(f) for f in fields)
        if any(not isinstance(v, str) or not v for v in values):
            raise ValueError("Missing primary stratum value")
        bucket = row.get("structure_bucket")
        if bucket not in ("simple", "composed", "nested_or_variable", "unknown"):
            raise ValueError("Missing or unsupported lexical structure bucket")
        groups[values].append(row)
    if not groups:
        raise ValueError("Empty population")
    populations = {key: len(value) for key, value in groups.items()}
    if mode == "full_release":
        targets = populations
    elif "primary_quotas" in policy:
        targets = {}
        for quota in policy["primary_quotas"]:
            key = tuple(quota["values"])
            if key in targets or key not in groups:
                raise ValueError("Duplicate or nonexistent quota stratum")
            count = quota["count"]
            if type(count) is not int or not 1 <= count <= populations[key]:
                raise ValueError("Invalid quota count")
            targets[key] = count
        if set(targets) != set(groups):
            raise ValueError("Every primary stratum needs an explicit quota")
        if sum(targets.values()) != policy.get("target"):
            raise ValueError("Quotas do not sum to target")
    else:
        minimums = {key: len({row["structure_bucket"] for row in values})
                    for key, values in groups.items()}
        targets = allocate(populations, policy["target"], minimums)

    selected, strata = [], []
    for key in sorted(groups):
        buckets = defaultdict(list)
        for row in groups[key]:
            buckets[row["structure_bucket"]].append(row)
        sizes = {b: len(values) for b, values in buckets.items()}
        counts = sizes if mode == "full_release" else allocate(sizes, targets[key])
        for bucket in sorted(buckets):
            n, population = counts[bucket], sizes[bucket]
            stratum = dict(zip(fields, key), structure_bucket=bucket)
            ranked = sorted(buckets[bucket], key=lambda row: (
                sha(canonical([seed, benchmark, stratum, row["position_id"]])), row["position_id"]))
            for row in ranked[:n]:
                selected.append({
                    "position_id": row["position_id"], "benchmark": benchmark,
                    "task_id": row.get("task_id"), "graph": row.get("graph"),
                    "split": row.get("split"), "source_group": row.get("source_group"),
                    "stratum": stratum, "population_row_sha256": sha(canonical(row)),
                    "exposure_status": row.get("exposure_status", "unknown"),
                    "inclusion_probability": n / population,
                    "design_weight": population / n,
                })
            strata.append(dict(stratum=stratum, population=population, selected=n,
                               inclusion_probability=n / population, design_weight=population / n))
    return sorted(selected, key=lambda row: row["position_id"]), strata


def write_bundle(population_path, policy_path, output):
    policy_raw = policy_path.read_bytes()
    policy = json.loads(policy_raw)
    raw = population_path.read_bytes()
    if sha(raw) != policy.get("population_sha256"):
        raise ValueError("Population SHA differs from the pinned policy")
    rows = [json.loads(line) for line in raw.decode("utf-8-sig").splitlines() if line.strip()]
    selected, strata = select_population(rows, policy)
    output.mkdir(parents=True, exist_ok=False)
    selection = b"".join(canonical(row) + b"\n" for row in selected)
    (output / "selection.jsonl").write_bytes(selection)
    (output / "strata.json").write_bytes(canonical(strata) + b"\n")
    manifest = {
        "kind": "offline_preparation_selection", "selection_mode": policy["mode"],
        "benchmark": policy["benchmark"], "population_count": len(rows),
        "selected_count": len(selected), "seed": policy["seed"],
        "algorithm": "one_per_lexical_stratum_then_hierarchical_residual_proportional_sha256_rank_v1",
        "source": {"path": str(population_path.resolve()), "sha256": sha(raw)},
        "policy": {"path": str(policy_path.resolve()), "sha256": sha(policy_raw)},
        "selector_sha256": sha(Path(__file__).read_bytes()),
        "selection_sha256": sha(selection),
        "strata_sha256": sha((output / "strata.json").read_bytes()),
        "run_allowed": False, "formal_scoring_and_profiles_frozen": False,
        "statistical_power_verified": False,
        "inference": "Report primary strata separately; N_h/n_h weights target released positions, not independent semantic tasks",
        "unknown_reference_policy": "Preserve selected positions; reference validity does not determine selection",
    }
    (output / "manifest.json").write_bytes(canonical(manifest) + b"\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(write_bundle(args.population, args.policy, args.output), indent=2))


if __name__ == "__main__":
    main()
