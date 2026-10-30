"""One adapter path through the existing pool, ledger and HTTP scheduler.

The matrix producer schedules these coroutines across cell boundaries. This
module owns neither HTTP retries nor concurrency limits; Scheduler owns both.
DB feedback is submitted by the separate consumer, not fabricated here.
"""
from engine.config import canonical
from engine.scheduler import Item
from common.errors import IntegrityError
from common.stages import advance


def request_item(spec, *, model, sampling, benchmark=None, method=None):
    value = spec if isinstance(spec, dict) else spec.as_dict()
    body = dict(value.get("body") or {"messages": list(value["messages"])})
    if any(key in sampling for key in ("model", "messages", "prompt")):
        raise IntegrityError("Sampling cannot change model identity or public input")
    # Stage-specific stop and remaining budget take priority over run defaults.
    wire = {**sampling, **body, "model": model}
    return Item(
        benchmark or value["benchmark"], value["task_id"], method or value["method"],
        value["graph"], canonical(wire), model,
        phase=value.get("generation_stage", "initial"),
    )


async def generate_path(adapter, spec, scheduler, *, model, sampling, benchmark=None, method=None):
    """Persist each physical result before deriving a possible second stage."""
    replies, current = [], spec
    for _ in range(2):
        item = request_item(current, model=model, sampling=sampling, benchmark=benchmark, method=method)
        reply = await scheduler.generate(item)
        replies.append(reply)
        if reply.get("status") not in ("ok", "length", "refused", "empty"):
            return dict(status=reply.get("status", "unknown"), replies=replies, prediction=None)
        if reply.get("status") != "ok":
            # A syntactically plausible truncated answer is still incomplete.
            # Preserve the paid response, but never execute or reissue it here.
            return dict(status=reply["status"], replies=replies, prediction=None)
        text = reply.get("content") or ""
        if getattr(current, "generation_stage", None) == "schema_select":
            usage = reply.get("usage") or {}
            step = advance(adapter, current, text,
                           generated_tokens=usage.get("completion_tokens"),
                           finish_reason=reply.get("finish_reason"), matched_stop=reply.get("matched_stop"))
            scheduler.journal.append("adapter_next_step", parent_request_id=reply.get("request_id"),
                                     action=step.action, state=step.state, reason=step.reason)
            if step.action != "request":
                return dict(status=step.action, reason=step.reason, replies=replies, prediction=None)
            current = step.request
            continue
        prediction = adapter.parse(text, http_status=reply.get("http_status"))
        return dict(status=prediction.status, replies=replies, prediction=prediction)
    raise IntegrityError("Adapter exceeded the two-stage generation contract")
