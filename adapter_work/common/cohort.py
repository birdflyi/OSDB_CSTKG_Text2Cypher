"""Read a pinned cohort without selecting again or loading evaluator answers."""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path


BENCHMARKS = {
    "cypherbench": ("B01", "CypherBench"), "text2gql": ("B02", "Text2GQL"),
    "mindthequery": ("B03", "MindTheQuery"), "zograscope": ("B04", "ZOGRASCOPE"),
    "osdb_stkg": ("B05", "OSDB-STKG"),
}
MODES = {"full_release", "fixed_stratified", "independently_authored"}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def verify_pin(pin):
    if not isinstance(pin, dict) or not isinstance(pin.get("path"), str) or not isinstance(pin.get("sha256"), str):
        raise ValueError("Artifact needs path and SHA256")
    path = Path(pin["path"])
    if not path.is_absolute():
        raise ValueError("Artifact path must be absolute")
    raw = path.read_bytes()
    if digest(raw) != pin["sha256"]:
        raise ValueError("Artifact SHA mismatch: " + str(path))
    return path, raw


def _jsonl(raw):
    return [json.loads(line) for line in raw.decode("utf-8-sig").splitlines() if line.strip()]


@dataclass(frozen=True)
class Cohort:
    path: Path
    sha256: str
    manifest: dict
    positions: tuple

    @property
    def benchmark(self):
        return BENCHMARKS[self.manifest["benchmark"]][1]

    @property
    def pin(self):
        return {"path": str(self.path), "sha256": self.sha256}


def execution_task_id(cohort_sha256, position_id):
    """Unique scheduler identity; original task ID remains the reference identity."""
    return "cohort-" + digest(canonical([cohort_sha256, position_id]))[:32]


def load_cohort(pin):
    """Validate the selector's manifest and its complete local input closure.

    References are hashed as bytes, never decoded here. The public record is
    checked against its source position and payload digest before being used.
    Existing offline_preparation_selection v1 manifests remain supported.
    """
    path, raw = verify_pin(pin)
    manifest = json.loads(raw)
    if manifest.get("kind") not in {"offline_preparation_selection", "cohort_v1"}:
        raise ValueError("Unsupported cohort manifest kind")
    if manifest.get("schema_version", 1) not in {1, "cohort_v1"}:
        raise ValueError("Unsupported cohort schema version")
    if manifest.get("benchmark") not in BENCHMARKS or manifest.get("selection_mode") not in MODES:
        raise ValueError("Unsupported cohort benchmark or mode")
    _, population_raw = verify_pin(manifest["source"])
    _, policy_raw = verify_pin(manifest["policy"])
    policy = json.loads(policy_raw)
    if policy.get("population_sha256") != manifest["source"]["sha256"] or policy.get("benchmark") != manifest["benchmark"] or policy.get("mode") != manifest["selection_mode"]:
        raise ValueError("Policy and manifest disagree")
    selection_pin = manifest.get("selection") or {"path": str(path.parent / "selection.jsonl"), "sha256": manifest["selection_sha256"]}
    _, selection_raw = verify_pin(selection_pin)
    if "strata_sha256" in manifest:
        verify_pin({"path": str(path.parent / "strata.json"), "sha256": manifest["strata_sha256"]})
    population = _jsonl(population_raw)
    selected = _jsonl(selection_raw)
    by_id = {}
    for row in population:
        identity = row.get("position_id")
        if not isinstance(identity, str) or not identity or identity in by_id:
            raise ValueError("Missing or duplicate population identity")
        if row.get("benchmark") != manifest["benchmark"]:
            raise ValueError("Population contains another benchmark")
        by_id[identity] = row
    if type(manifest.get("population_count")) is not int or len(population) != manifest["population_count"]:
        raise ValueError("Population denominator mismatch")
    if type(manifest.get("selected_count")) is not int or len(selected) != manifest["selected_count"] or not selected:
        raise ValueError("Selection denominator mismatch")
    ids = [row.get("position_id") for row in selected]
    if any(not isinstance(i, str) or not i for i in ids) or ids != sorted(ids) or len(set(ids)) != len(ids):
        raise ValueError("Selection must have unique, sorted source positions")
    if manifest["selection_mode"] == "full_release" and set(ids) != set(by_id):
        raise ValueError("Full release selection omits or adds positions")
    file_cache, public_cache, line_counts = {}, {}, {}

    def verified_file(binding):
        key = (binding.get("path"), binding.get("sha256"))
        if key not in file_cache:
            file_cache[key] = verify_pin(binding)[1]
            line_counts[key] = file_cache[key].count(b"\n") + (bool(file_cache[key]) and not file_cache[key].endswith(b"\n"))
        return file_cache[key]

    positions = []
    for ordinal, selection in enumerate(selected):
        source = by_id.get(selection["position_id"])
        if source is None or digest(canonical(source)) != selection.get("population_row_sha256"):
            raise ValueError("Selected source row mismatch")
        for field in ("benchmark", "task_id", "graph", "split", "source_group"):
            if selection.get(field) != source.get(field):
                raise ValueError("Selected identity differs: " + field)
        if not isinstance(source.get("task_id"), str) or not source["task_id"] or not isinstance(source.get("graph"), str) or not source["graph"]:
            raise ValueError("Missing task/graph identity")
        verified_file({"path": source["source_file"], "sha256": source["source_sha256"]})
        reference = source.get("reference_binding")
        if not isinstance(reference, dict) or type(reference.get("line_1based")) is not int or reference["line_1based"] < 1 or not reference.get("record_sha256"):
            raise ValueError("Missing reference record binding")
        ref_raw = verified_file(reference)
        # Counting lines does not read reference semantics or expose answers.
        if reference["line_1based"] > line_counts[(reference["path"], reference["sha256"])]:
            raise ValueError("Reference record locator is outside file")
        public_binding = source["public_binding"]
        public_raw = verified_file(public_binding)
        key = (public_binding["path"], public_binding["sha256"])
        if key not in public_cache:
            public_cache[key] = _jsonl(public_raw)
        line = public_binding.get("line_1based")
        if type(line) is not int or not 1 <= line <= len(public_cache[key]):
            raise ValueError("Public record locator is outside file")
        public_row = public_cache[key][line - 1]
        if digest(canonical(public_row)) != public_binding.get("record_sha256"):
            raise ValueError("Public record SHA mismatch")
        if any(public_row.get(f) != source.get(f) for f in ("position_id", "task_id", "graph")):
            raise ValueError("Public record belongs to another position")
        if digest(canonical(public_row["public_input"])) != source.get("public_input_sha256"):
            raise ValueError("Public payload SHA mismatch")
        positions.append({**source, **selection, "source_position_id": selection["position_id"],
                          "ordinal": ordinal, "category": source.get("category"),
                          "cohort_sha256": pin["sha256"],
                          "execution_task_id": execution_task_id(pin["sha256"], selection["position_id"]),
                          "public_payload": public_row["public_input"]})
    return Cohort(path, pin["sha256"], manifest, tuple(positions))


def select_positions(cohort, cell_id):
    if not isinstance(cell_id, str) or len(cell_id.split("-")) != 2 or cell_id.split("-")[1] != BENCHMARKS[cohort.manifest["benchmark"]][0]:
        raise ValueError("Cell belongs to another benchmark")
    return cohort.positions
