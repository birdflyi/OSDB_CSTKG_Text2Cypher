"""Build method-independent score policies from frozen reference queries.

This conservative lexical check is not a Cypher parser. Unrecognized forms
remain UNADJUDICATED; they never become BAG by guessing. No query executes.
"""
from hashlib import sha256
import json
import re


class ScorePolicyError(ValueError):
    pass


BENCHMARKS = {
    "CypherBench": ("cb_pinned_result_ex_v1", "author_result_comparator"),
    "Text2GQL": ("t2g_author_derived_rowset_v1", "author_derived"),
    "MindTheQuery": ("project_numeric_exact_output_v1", "project_defined"),
    "ZOGRASCOPE": ("project_numeric_exact_output_v1", "project_defined"),
    "OSDB-STKG": ("project_osdb_typed_numeric_bag_v1", "project_defined"),
}
REFERENCE_STATUSES = frozenset({"VALID", "UNVERIFIED", "MISSING", "EXECUTION_ERROR", "TIMEOUT", "AMBIGUOUS", "SNAPSHOT_MISMATCH", "MALFORMED"})


def policy_digest(policy):
    return sha256(json.dumps(policy, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf8")).hexdigest()


def _tokens(query):
    """Return (word-or-punctuation, nesting-depth) outside quoted text."""
    tokens, stack = [], []
    i = 0
    while i < len(query):
        char = query[i]
        if char.isspace():
            i += 1
        elif query.startswith("//", i):
            end = query.find("\n", i+2)
            i = len(query) if end < 0 else end+1
        elif query.startswith("/*", i):
            end = query.find("*/", i+2)
            if end < 0:
                raise ScorePolicyError("Unclosed comment")
            i = end+2
        elif char in "'\"`":
            quote = char
            i += 1
            while i < len(query):
                if query[i] == "\\" and quote != "`":
                    i += 2
                elif query[i] == quote:
                    if i+1 < len(query) and query[i+1] == quote:
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
            else:
                raise ScorePolicyError("Unclosed quoted value")
            tokens.append(("<quoted>", len(stack)))
        elif char in "([{":
            tokens.append((char, len(stack)))
            stack.append(char)
            i += 1
        elif char in ")]}":
            if not stack or stack.pop() != {")": "(", "]": "[", "}": "{"}[char]:
                raise ScorePolicyError("Unbalanced delimiters")
            tokens.append((char, len(stack)))
            i += 1
        elif char.isalpha() or char == "_":
            end = i+1
            while end < len(query) and (query[end].isalnum() or query[end] == "_"):
                end += 1
            tokens.append((query[i:end].upper(), len(stack)))
            i = end
        else:
            tokens.append((char, len(stack)))
            i += 1
    if stack:
        raise ScorePolicyError("Unbalanced delimiters")
    return tokens


def classify_ordering(query):
    """Classify a single read query's final output, retaining limitations."""
    if type(query) is not str or not query.strip():
        return dict(order_rule="UNADJUDICATED", reason="missing_reference_query", concerns=[])
    try:
        tokens = _tokens(query)
    except ScorePolicyError as exc:
        return dict(order_rule="UNADJUDICATED", reason=str(exc), concerns=[])
    top = [word for word, depth in tokens if depth == 0]
    # Ignore a property such as n.order: it is not a clause keyword.
    words = [word if index == 0 or top[index-1] != "." else "<property>" for index, word in enumerate(top)]
    concerns = []
    for index, (word, _) in enumerate(tokens[:-1]):
        if word in {"RAND", "RANDOMUUID", "TIMESTAMP"} and tokens[index+1][0] == "(":
            concerns.append("nondeterministic_function:" + word.lower())
        if word in {"DATE", "TIME", "DATETIME", "LOCALTIME", "LOCALDATETIME"} and index+2 < len(tokens) and [p[0] for p in tokens[index+1:index+3]] == ["(", ")"]:
            concerns.append("clock_function:" + word.lower())
    if not words or words[0] not in {"MATCH", "OPTIONAL", "WITH", "UNWIND", "CALL", "RETURN"}:
        return dict(order_rule="UNADJUDICATED", reason="unrecognized_query_form", concerns=sorted(set(concerns)))
    if "UNION" in words or any(word in words for word in ("CREATE", "DELETE", "MERGE", "SET", "REMOVE", "LOAD")):
        return dict(order_rule="UNADJUDICATED", reason="compound_or_unrecognized_clause", concerns=sorted(set(concerns)))
    if words.count(";") > 1 or (";" in words and words[-1] != ";") or words.count("RETURN") != 1:
        return dict(order_rule="UNADJUDICATED", reason="requires_one_final_return", concerns=sorted(set(concerns)))
    final = words[words.index("RETURN")+1:]
    if not final or any(word in final for word in ("MATCH", "OPTIONAL", "WITH", "UNWIND", "CALL", "YIELD")):
        return dict(order_rule="UNADJUDICATED", reason="unsupported_after_return", concerns=sorted(set(concerns)))
    ordered = any(final[i:i+2] == ["ORDER", "BY"] for i in range(len(final)-1))
    if "ORDER" in final and not ordered:
        return dict(order_rule="UNADJUDICATED", reason="unrecognized_order_clause", concerns=sorted(set(concerns)))
    if "LIMIT" in final or "SKIP" in final or "OFFSET" in final:
        concerns.append("ordered_slice_ties_unverified" if ordered else "slice_without_final_order")
    return dict(order_rule="ORDERED" if ordered else "BAG", reason="final_output_lexical_clause", concerns=sorted(set(concerns)))


def build_task_policy(benchmark, source_position_id, reference_query, *, reference_status="UNVERIFIED", comparator_pin=None, cohort_sha256=None, reference_anomalies=()):
    """Create an explicit task policy; caller freezes the resulting JSONL."""
    if benchmark not in BENCHMARKS or type(source_position_id) is not str or not source_position_id:
        raise ScorePolicyError("Known benchmark and source position identity required")
    if reference_status not in REFERENCE_STATUSES:
        raise ScorePolicyError("Unknown reference status")
    if comparator_pin is not None and (type(comparator_pin) is not dict or type(comparator_pin.get("path")) is not str or not re.fullmatch(r"[0-9a-f]{64}", comparator_pin.get("sha256", ""))):
        raise ScorePolicyError("Comparator pin requires path and SHA256")
    if cohort_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", cohort_sha256):
        raise ScorePolicyError("Invalid cohort digest")
    semantic = classify_ordering(reference_query)
    if benchmark == "CypherBench":
        order_rule = "UNADJUDICATED" if type(reference_query) is not str or not reference_query.strip() else ("ORDERED" if "order by" in reference_query.lower() else "BAG")
        order_source = "pinned_author_substring_rule"
    elif benchmark in ("Text2GQL", "OSDB-STKG"):
        order_rule, order_source = "BAG", "metric_rowset" if benchmark == "Text2GQL" else "declared_numeric_bag"
    else:
        order_rule, order_source = semantic["order_rule"], semantic["reason"]
    metric_id, authority = BENCHMARKS[benchmark]
    return dict(version="V47", benchmark=benchmark, source_position_id=source_position_id,
                metric_id=metric_id, metric_authority=authority, cohort_sha256=cohort_sha256,
                reference_query_sha256=sha256(reference_query.encode("utf8")).hexdigest() if type(reference_query) is str else None,
                reference_status=reference_status, order_rule=order_rule, order_source=order_source,
                diagnostic_order_rule=semantic["order_rule"], concerns=semantic["concerns"],
                reference_anomalies=list(reference_anomalies), comparator_pin=comparator_pin,
                formal_admitted=False)
