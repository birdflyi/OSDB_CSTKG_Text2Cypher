"""Independent B smoke: consume -> one operational feedback -> consume feedback.

The A request phase has already sealed its replies. DB work never owns model
slots. All new feedback requests use the same scheduler, pool and paid ledger.
"""

import argparse
import asyncio
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "adapter_work")]
from engine.config import digest
from engine.ledger import Ledger, scan_legacy
from engine.recovery import checkpoint_replies
from offline_pipeline import read_pin, save, run as consume
import request_pipeline


def pin(path):
    return dict(path=str(path), sha256=digest(path.read_bytes()))


def run(config):
    output = Path(config["output"])
    output.mkdir(parents=True, exist_ok=False)
    save(output / "config.json", config)
    offline = read_pin(config["offline"])
    initial = consume(offline)
    queue = sorted(Path(offline["output"]).glob("M*/B*/feedback_request.json"))
    if not queue:
        save(
            output / "summary.json",
            dict(initial=initial, feedback_positions=0, model_calls=0),
        )
        return
    queue_pin = save(output / "feedback_queue.json", [pin(p) for p in queue])
    requests = read_pin(config["requests"])
    requests.update(
        feedback_queue=queue_pin,
        run_dir=str(output / "feedback_requests"),
        name=requests["name"] + "-operational-feedback",
    )
    legacy = scan_legacy(
        Path(requests["legacy_ledger"]), requests.get("path_rewrites", [])
    )
    manifest = output / "recovery.json"
    with Ledger(
        Path(requests["ledger_dir"]),
        Path(requests["approval_path"]),
        legacy,
        requests["budget_scope"],
    ) as ledger:
        checkpoint_replies(manifest, ledger)
    requests["recovery"] = pin(manifest)
    save(output / "feedback_config.json", requests)
    generated = asyncio.run(request_pipeline.run(requests))
    selection = defaultdict(list)
    for path in queue:
        row = json.loads(path.read_bytes())["position"]
        selection[row["cell_id"]].append(row["position_id"])
    followup = dict(
        offline,
        output=str(output / "feedback_answers"),
        selected_positions=dict(selection),
        source_finals=[
            pin(p) for p in sorted((output / "feedback_requests/finals").glob("*.json"))
        ],
    )
    save(output / "feedback_consumer_config.json", followup)
    repaired = consume(followup)
    # Preserve both rounds; the final view replaces only the declared DF cases.
    final = {(r["cell_id"], r["position_id"]): r for r in initial["results"]}
    for row in repaired["results"]:
        final[row["cell_id"], row["position_id"]] = row
    save(
        output / "summary.json",
        dict(
            initial=initial,
            feedback=repaired,
            feedback_generation=generated,
            final=list(final.values()),
            final_counts=dict(Counter(r["status"] for r in final.values())),
            feedback_positions=len(queue),
            paper_result=False,
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    run(json.loads(args.config.read_bytes()))
