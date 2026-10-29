"""Paper-derived offline primitives, not authenticated author reproductions.

No model, tokenizer, prompt, training or network defaults are invented here.
The caller must bind those missing assets before any runtime admission.
"""

import json
import math


def _unique_fields(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate_selection_key")
        value[key] = item
    return value


def ri_schema_selection(rollout, *, entity_labels, relation_labels):
    """Cypher-RI §3: invalid JSON/unknown labels retrieve the complete schema.

    The training template uses {name, entities, relations} inside JSON tags.
    Return labels only; the pinned schema renderer owns the original format.
    """
    full = dict(entities=list(entity_labels), relations=list(relation_labels))
    reason = None
    try:
        if rollout.count("<json>") != 1 or rollout.count("</json>") != 1:
            raise ValueError("schema_tags")
        start = rollout.index("<json>") + len("<json>")
        end = rollout.index("</json>")
        if start > end:
            raise ValueError("schema_tags")
        selected = json.loads(rollout[start:end], object_pairs_hook=_unique_fields)
        if (type(selected) is not dict or set(selected) != {"name", "entities", "relations"}
                or type(selected["name"]) is not str):
            raise ValueError("selection_shape")
        for field in ("entities", "relations"):
            labels = selected[field]
            if (type(labels) is not list or any(type(label) is not str for label in labels)
                    or len(set(labels)) != len(labels) or set(labels) - set(full[field])):
                raise ValueError("unknown_or_invalid_labels")
        return dict(entities=selected["entities"], relations=selected["relations"],
                    fallback=False, reason=None)
    except (ValueError, TypeError) as error:
        reason = str(error)
    return dict(**full, fallback=True, reason=reason)


def ri_append_schema(rollout, rendered_schema):
    """Continue the same assistant rollout; do not start a fresh chat turn."""
    if not rollout.endswith("</json>") or type(rendered_schema) is not str:
        raise ValueError("Expected a rollout stopped exactly at </json> and rendered schema")
    return rollout + "<schema>" + rendered_schema + "</schema>"


def smith_filter_decision(full_logprob, reduced_logprobs, *, rule):
    """Expose the unresolved paper alternatives instead of silently choosing one.

    Log probabilities avoid underflow. The three reduced contexts omit the
    ontology, question and relations respectively. This does not compute LLM
    likelihoods or assert which rule the authors actually implemented.
    """
    if len(reduced_logprobs) != 3:
        raise ValueError("Three reduced contexts required")
    values = [full_logprob, *reduced_logprobs]
    if any(type(v) not in (int, float) or math.isnan(v) or v > 0 for v in values):
        raise ValueError("Log probabilities must be nonpositive and not NaN")
    if rule == "prose_full_le_max":
        rejected = full_logprob <= max(reduced_logprobs)
    elif rule == "printed_full_lt_min":
        rejected = full_logprob < min(reduced_logprobs)
    else:
        raise ValueError("Explicit unresolved paper rule required")
    return dict(rule=rule, retained=not rejected, author_rule_confirmed=False)
