"""Continue a fixed pilot while counting failed model batches as explicit ERROR.

Never repair a semantic answer, infer missing rows, or retry a failed dispatch.
Preserves original provider responses and distinguishes protocol failures.
"""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

import run_baselines as base

ORIGINAL_CALL = base.Client.call


def call_recording_failures(self, stage, prompt, data, ids, keys, max_tokens=6000):
    try:
        return ORIGINAL_CALL(self, stage, prompt, data, ids, keys, max_tokens)
    except (RuntimeError, ValueError, TypeError, KeyError) as exc:
        folder = self.out / "calls" / stage
        response, transport = folder / "response.json", folder / "transport_error.json"
        if not response.exists() and not transport.exists():
            # A saved request alone is not proof the provider finished.
            raise
        failure = {"status": "ERROR", "generated_by": "RUNNER_FAILURE_RECORD_NOT_MODEL_ANSWER",
                   "ids": ids, "error_type": type(exc).__name__, "retry": False,
                   "provider_response_sha256": base.sha(response) if response.exists() else None}
        path = folder / "recorded_failure.json"
        if not path.exists():
            base.save(path, failure)
        elif json.loads(path.read_text(encoding="utf-8")) != failure:
            raise RuntimeError("Failure record changed")
        if set(keys) == {"id", "status", "cypher", "reason"}:
            return [{"id": request_id, "status": "ERROR", "cypher": None,
                     "reason": "Model dispatch/protocol failed; see retained raw response"} for request_id in ids]
        if set(keys) == {"id", "status", "rows", "evidence_ids"}:
            return [{"id": request_id, "status": "ERROR", "rows": [], "evidence_ids": []} for request_id in ids]
        raise ValueError("No failure record representation for unknown stage")


def main(args):
    output = Path(args.output).resolve()
    if args.resume_from:
        source = Path(args.resume_from).resolve()
        if not (source / "attempt_outcome.json").exists():
            raise RuntimeError("Source attempt must be confirmed terminal")
        if output.exists():
            raise FileExistsError("Recovery output must be new")
        output.mkdir(parents=True)
        shutil.copytree(source / "calls", output / "calls")
        base.save(output / "cache_reuse_provenance.json", {
            "source_run": str(source), "policy": "Identical completed response cache; no redispatch of failed batches",
            "copied_hashes": {str(p.relative_to(source)): base.sha(p) for p in (source / "calls").rglob("*.json")}})
    base.save(output / "failure_policy.json", {"mode": "Explicit failed-batch ERROR records, not generated answers",
        "retry": False, "preserve_planned_denominator": True,
        "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    base.Client.call = call_recording_failures
    try:
        base.run(args.config, args.data, args.graph_receipt, output, args.batch_size)
    finally:
        base.Client.call = ORIGINAL_CALL
    failures = [json.loads(p.read_text(encoding="utf-8")) for p in (output / "calls").glob("*/recorded_failure.json")]
    result = {"failed_batches": len(failures), "failed_task_stage_instances": sum(len(f["ids"]) for f in failures),
              "failures": failures, "policy": "All retained and scored as ERROR; no semantic completion or retry"}
    base.save(output / "protocol_failure_summary.json", result)
    print(json.dumps({"protocol_failure_batches": len(failures)}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--graph-receipt", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--resume-from")
    parser.add_argument("--batch-size", type=int, default=2)
    main(parser.parse_args())
