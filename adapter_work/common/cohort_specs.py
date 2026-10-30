"""One explicit adapter preparation path for cohort producer and consumer.

The new cohort may pin an Ours prompt profile without modifying legacy adapter
defaults. Direct and feedback retain their existing shared Direct profile.
"""
import json
from pathlib import Path

from common import loader, ours
from common.cohort import digest, verify_pin


def prepare_cell_specs(cell):
    source, raw = verify_pin(cell["positions_source"])
    document = json.loads(raw)
    if document.get("schema_version") != "cohort_positions_v1":
        raise ValueError("Explicit cohort positions are required")
    if any(document.get(k) != cell.get(k) for k in ("cell_id", "method", "benchmark")):
        raise ValueError("Cell and source identities differ")
    if document.get("cohort") != cell.get("cohort"):
        raise ValueError("Cell and source cohort differ")
    profile, profile_raw = verify_pin(cell["prompt_profile"])
    method = cell["method"]
    if method == "ours":
        specs = ours.prepare(source, cell["benchmark"], cell["cell_id"], prompt_profiles=profile)
    elif method in {"direct", "direct-feedback"}:
        default = Path(__file__).resolve().parents[1] / "prompts/direct.json"
        if digest(profile_raw) != digest(default.read_bytes()):
            raise ValueError("Direct profile does not match the adapter's shared profile")
        adapter_path, _ = verify_pin(cell["adapter"])
        specs = loader.load(adapter_path).prepare(source)
    else:
        raise ValueError("Controls use a saved plan, not request preparation")
    expected = document["positions"]
    if len(expected) != len(specs):
        raise ValueError("Adapter changed the selected denominator")
    for row, spec in zip(expected, specs):
        value = spec if isinstance(spec, dict) else spec.as_dict()
        if any(value.get(k) != row.get(k) for k in ("position_id", "task_id", "graph", "benchmark")):
            raise ValueError("Adapter changed a selected position")
    return specs
