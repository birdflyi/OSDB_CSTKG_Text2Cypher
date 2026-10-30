"""Score actual saved answers with explicit reference and denominator states.

Inputs are the existing offline backend answer/reference dictionaries. This
module performs no query/model calls. Receipt reading is optional and local;
production consumers must call verify_answer_receipt before loading gold.
"""
from copy import deepcopy
from hashlib import sha256
import importlib.util
import json
import math
from pathlib import Path
import sys

from adapter_work.scoring_contract import ScoringContractError, compare_numeric_exact
from adapter_work.task_scoring_policy import BENCHMARKS, REFERENCE_STATUSES, ScorePolicyError, build_task_policy, policy_digest


ROOT = Path(__file__).resolve().parents[1]
NONTERMINAL = frozenset({"UNKNOWN", "UNDISPATCHED", "UNTERMINATED", "QUEUED", "PENDING", "FEEDBACK_PENDING", "NOT_RUN_HARD_STOP", "SOURCE_PLAN_NOT_EXECUTED", "INTERRUPTED_AFTER_FINAL"})
FAILURES = frozenset({"REFUSED", "DECLINED", "UNSUPPORTED", "INSUFFICIENT_EVIDENCE", "MODEL_ERROR", "MODEL_TRUNCATED", "TRUNCATED", "TRUNCATED_RESULT", "INCOMPLETE_ANSWER", "MALFORMED_ANSWER", "EXECUTION_ERROR", "EXECUTION_TIMEOUT", "INFRASTRUCTURE_ERROR", "TRANSPORT_EXHAUSTED", "BACKEND_ERROR", "PROTOCOL_ERROR", "MODEL_PROTOCOL_ERROR", "PLAN_VALIDATION_ERROR", "READ_POLICY_ERROR", "GUARD_REJECTED", "INITIAL_FAILED_NO_MODEL_REPAIR", "EMPTY"})


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf8")


def verify_answer_receipt(answer, receipt):
    """Verify the existing final.json pin BEFORE the caller loads references."""
    if type(receipt) is not dict or set(receipt) != {"path", "sha256"}:
        raise ScorePolicyError("Answer receipt requires path and SHA256")
    raw = Path(receipt["path"]).read_bytes()
    if sha256(raw).hexdigest() != receipt["sha256"] or _canonical(json.loads(raw)) != _canonical(answer):
        raise ScorePolicyError("Saved final answer differs from score input")


def _module(name, relative_path):
    """Load a project pure helper once; never import an author runner."""
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT/relative_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def load_comparator(policy):
    """Load the existing pinned pure author comparator, not its DB runner."""
    benchmark = policy["benchmark"]
    if benchmark not in ("CypherBench", "Text2GQL"):
        return None
    pin = policy.get("comparator_pin")
    if type(pin) is not dict:
        raise ScorePolicyError("Author comparison needs a frozen comparator pin")
    source = Path(pin["path"])
    if sha256(source.read_bytes()).hexdigest() != pin["sha256"]:
        raise ScorePolicyError("Author comparator source changed")
    if benchmark == "CypherBench":
        module = _module("_v47_cb_answer_scoring", "experiment-harness/public_benchmark_v1/answer_scoring.py")
        return module.load_official_comparator(source.parents[2])
    module = _module("_v47_t2_pure_extract", "operations/analysis/audit_text2gql_score_boundaries_20261027.py")
    evaluator, _ = module.subset(source, "ExecutionEvaluator", ["compare_results"])
    return evaluator.compare_results


def normalize_reference(policy, raw):
    """Accept current CB gold, T2 store, OSDB gold, or typed native answers.

    A canonical {status:'VALID',answer:...} is also accepted. VALID with no
    answer suffices to classify an executed-path failure but not a success.
    Missing raw data never invents a successful empty reference.
    """
    if raw is None:
        return dict(status=policy["reference_status"], answer=None)
    if type(raw) is not dict:
        return dict(status="MALFORMED", answer=None)
    query = raw.get("gold_cypher", raw.get("query", raw.get("reference_query")))
    received_sha = raw.get("query_sha256") or (sha256(query.encode("utf8")).hexdigest() if type(query) is str else None)
    if received_sha is not None and policy.get("reference_query_sha256") != received_sha:
        raise ScorePolicyError("Reference query differs from frozen task policy")
    if policy["reference_status"] not in ("VALID", "UNVERIFIED"):
        return dict(status=policy["reference_status"], answer=None)
    status = raw.get("status")
    if status in REFERENCE_STATUSES:
        return dict(status=status, answer=raw.get("answer"), evidence=raw.get("evidence"))
    if raw.get("reference_known") is False:
        return dict(status="EXECUTION_ERROR", answer=None)
    benchmark = policy["benchmark"]
    try:
        if benchmark == "CypherBench" and "answer_json" in raw:
            rows = json.loads(raw["answer_json"])
            return dict(status="VALID", answer=dict(status="EXECUTED", complete=True, rows=rows))
        if benchmark == "OSDB-STKG" and "numeric_reference_rows" in raw:
            rows = raw["numeric_reference_rows"]
            return dict(status="MISSING" if rows is None else "VALID", answer=None if rows is None else dict(status="EXECUTED", complete=True, rows=rows))
    except (TypeError, json.JSONDecodeError):
        return dict(status="MALFORMED", answer=None)
    answer = raw.get("answer", raw)
    if type(answer) is dict and answer.get("status") == "EXECUTED" and answer.get("complete") is True:
        return dict(status="VALID", answer=answer)
    failure = str(raw.get("reference_status", status or "UNVERIFIED")).upper()
    return dict(status="TIMEOUT" if "TIMEOUT" in failure else "EXECUTION_ERROR", answer=None)


def _rows(answer):
    if type(answer) is not dict or answer.get("status") != "EXECUTED" or answer.get("complete") is not True:
        raise ScoringContractError("Reference or prediction is not complete")
    rows = answer.get("rows")
    if type(rows) is not list or any(type(row) is not list for row in rows):
        raise ScoringContractError("Malformed result rows")
    widths = {len(row) for row in rows}
    if len(widths) > 1:
        raise ScoringContractError("Inconsistent row width")
    if "columns" in answer and (type(answer["columns"]) is not list or any(type(c) is not str for c in answer["columns"]) or any(len(row) != len(answer["columns"]) for row in rows)):
        raise ScoringContractError("Malformed result columns")
    return rows


def _typed_json(value):
    if value is None:
        return ("null",)
    if type(value) in (bool, int, str):
        return (type(value).__name__, value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ScoringContractError("Non-finite JSON number")
        return ("float", value.hex())
    if type(value) is list:
        return ("list", tuple(_typed_json(item) for item in value))
    if type(value) is dict and all(type(k) is str for k in value):
        return ("map", tuple((k, _typed_json(v)) for k, v in sorted(value.items())))
    raise ScoringContractError("Unsupported JSON result value")


def _strict_json(reference, prediction, ordered):
    from collections import Counter
    left, right = [_typed_json(row) for row in _rows(reference)], [_typed_json(row) for row in _rows(prediction)]
    if reference.get("columns") is not None and prediction.get("columns") is not None and len(reference["columns"]) != len(prediction["columns"]):
        return False
    return left == right if ordered else Counter(left) == Counter(right)


def _validate_reference_answer(benchmark, answer):
    _rows(answer)
    if benchmark in ("MindTheQuery", "ZOGRASCOPE"):
        compare_numeric_exact(answer, answer, ordered=False)
    else:
        [_typed_json(row) for row in answer["rows"]]


def prepare_score_policies(positions, reference_loader, *, comparator_pins=None, cohort_sha256=None):
    """Prepare every selected position from pinned local reference material.

    reference_loader(position) returns {record, reference_binding, query?}.
    The loader owns verifying file pins before returning records. This API
    never reads model predictions and never executes a reference query.
    Native source validation labels alone cannot qualify a reference.
    """
    entries, seen = [], set()
    comparator_pins = comparator_pins or {}
    for position in positions:
        identity = position["source_position_id"]
        if identity in seen:
            raise ScorePolicyError("Duplicate source position")
        seen.add(identity)
        benchmark = position["benchmark"]
        loaded = reference_loader(position) or {}
        raw = loaded.get("record")
        query = loaded.get("query")
        if query is None and type(raw) is dict:
            query = raw.get("gold_cypher", raw.get("query", raw.get("reference_query")))
        if type(raw) is dict:
            source_id = raw.get("qid", raw.get("task_id"))
            if source_id is not None and source_id != position["task_id"]:
                raise ScorePolicyError("Reference task differs from selected task")
            if "task_key" in raw and list(raw["task_key"]) != [position["graph"], position["task_id"]]:
                raise ScorePolicyError("Reference graph/task differs from selection")
        policy = build_task_policy(benchmark, identity, query, comparator_pin=comparator_pins.get(benchmark),
                                   cohort_sha256=cohort_sha256, reference_anomalies=loaded.get("reference_anomalies", ()))
        qualified = normalize_reference(policy, raw)
        status = qualified["status"]
        # Native author labels and query text are not executed reference evidence.
        if benchmark in ("MindTheQuery", "ZOGRASCOPE") and qualified.get("answer") is None and type(raw) is dict and not any(key in raw for key in ("status", "reference_status", "reference_known")):
            status = "UNVERIFIED" if query else "MISSING"
        if status == "VALID":
            if qualified["answer"] is None:
                status = "UNVERIFIED"
            else:
                try:
                    _validate_reference_answer(benchmark, qualified["answer"])
                except (ScoringContractError, KeyError, TypeError, ValueError):
                    status = "MALFORMED"
        policy["reference_status"] = status
        entries.append(dict(source_position_id=identity, graph=position["graph"], task_id=position["task_id"],
                            policy=policy, reference_binding=loaded.get("reference_binding"),
                            reference_qualification_required=status == "UNVERIFIED",
                            qualification_rule="pinned_complete_reference_answer" if status == "VALID" else "reference_receipt_required_or_error_retained"))
    from collections import Counter
    return dict(version="V47", positions=entries,
                reference_status_counts=dict(Counter(entry["policy"]["reference_status"] for entry in entries)),
                pending_reference_positions=[entry["source_position_id"] for entry in entries if entry["reference_qualification_required"]],
                model_predictions_inspected=False, model_calls=0, database_queries=0, formal_admitted=False)


def score_position(policy, prediction, reference, *, method, comparator=None, answer_receipt=None, reference_receipt=None):
    """Return backend-compatible status/value plus one C/W/R/U/NA partition.

    comparator is the already pinned callback from load_comparator. Consumers
    may load it once per frozen backend rather than once per position.
    """
    benchmark = policy.get("benchmark")
    if benchmark not in BENCHMARKS or (policy.get("metric_id"), policy.get("metric_authority")) != BENCHMARKS[benchmark]:
        raise ScorePolicyError("Unknown metric/benchmark policy")
    if policy.get("reference_status") not in REFERENCE_STATUSES or policy.get("order_rule") not in ("BAG", "ORDERED", "UNADJUDICATED"):
        raise ScorePolicyError("Malformed scoring policy")
    if type(method) is not str or not method or type(prediction) is not dict:
        raise ScorePolicyError("Method and actual answer record required")
    if answer_receipt is not None:
        verify_answer_receipt(prediction, answer_receipt)
    gold = normalize_reference(policy, reference)
    state = str(prediction.get("status", "UNKNOWN")).upper()
    record = dict(status="not_scored", value=None, source_position_id=policy["source_position_id"], benchmark=benchmark, method=method,
                  metric_id=policy["metric_id"], metric_authority=policy["metric_authority"], contract_sha256=policy_digest(policy),
                  cohort_sha256=policy.get("cohort_sha256"), reference_status=gold["status"], prediction_status=state,
                  prediction_terminal=state not in NONTERMINAL, partition=None, reason=None, diagnostics={},
                  answer_receipt=answer_receipt, reference_receipt=reference_receipt, formal_admitted=False)

    def finish(partition, reason, value=None):
        record.update(partition=partition, reason=reason, value=value)
        record["status"] = "scored" if partition in ("C", "W") else "not_applicable" if partition == "NA" else "reference_missing" if partition == "R" else "not_scored"
        return record

    if state == "NOT_APPLICABLE":
        if method not in ("native-set-count", "correct-detail", "simple-correct"):
            raise ScorePolicyError("NA is only an explicitly reported control applicability state")
        return finish("NA", "CONTROL_NOT_APPLICABLE")
    if gold["status"] != "VALID":
        return finish("R", "REFERENCE_" + gold["status"])
    ref = gold["answer"]
    # A supplied malformed reference wins over a prediction failure. When no
    # answer is supplied, VALID must come from the frozen reference manifest.
    if ref is not None:
        try:
            _validate_reference_answer(benchmark, ref)
        except (ScoringContractError, KeyError, TypeError, ValueError) as exc:
            record.update(reference_status="MALFORMED", error_type=type(exc).__name__)
            return finish("R", "REFERENCE_REPRESENTATION_UNRESOLVED")
    if state in NONTERMINAL:
        return finish("U", "PREDICTION_NOT_TERMINAL")
    if state in FAILURES:
        return finish("W", "TERMINAL_NO_ANSWER", False)
    if state != "EXECUTED":
        record["prediction_terminal"] = False
        return finish("U", "UNRECOGNIZED_PREDICTION_STATUS")
    if prediction.get("complete") is not True:
        return finish("W", "INCOMPLETE_FINAL_ANSWER", False)
    if ref is None:
        record["reference_status"] = "MISSING"
        return finish("R", "REFERENCE_ANSWER_NOT_AVAILABLE")
    if policy["order_rule"] == "UNADJUDICATED":
        return finish("U", "ORDERING_POLICY_UNADJUDICATED")
    try:
        _rows(prediction)
        if benchmark == "CypherBench":
            compare = comparator or load_comparator(policy)
            module = _module("_v47_cb_answer_scoring", "experiment-harness/public_benchmark_v1/answer_scoring.py")
            value = bool(module.score_executed_answer(status="EXECUTED", predicted_rows=prediction["rows"], reference_rows=ref["rows"], order_matters=policy["order_rule"] == "ORDERED", qid=policy["source_position_id"], compare=compare)["ex"])
        elif benchmark == "Text2GQL":
            compare = comparator or load_comparator(policy)
            width_left = len(ref["columns"]) if "columns" in ref else (len(ref["rows"][0]) if ref["rows"] else None)
            width_right = len(prediction["columns"])
            author_rows = lambda rows: [{str(i): cell for i, cell in enumerate(row)} for row in rows]
            value = (width_left is None or width_left == width_right) and bool(compare(author_rows(ref["rows"]), author_rows(prediction["rows"])))
            record["diagnostics"]["strict_typed_bag"] = _strict_json(ref, prediction, False)
            if policy["diagnostic_order_rule"] != "UNADJUDICATED":
                record["diagnostics"]["strict_task_order"] = _strict_json(ref, prediction, policy["diagnostic_order_rule"] == "ORDERED")
        elif benchmark in ("MindTheQuery", "ZOGRASCOPE"):
            value = compare_numeric_exact(ref, prediction, ordered=policy["order_rule"] == "ORDERED")
            # The old strict representation metric remains a distinct diagnostic.
            record["diagnostics"]["strict_typed_output_exact"] = _strict_json(ref, prediction, policy["order_rule"] == "ORDERED")
        else:
            value = _strict_json(ref, prediction, ordered=policy["order_rule"] == "ORDERED")
    except ScorePolicyError:
        raise
    except (ScoringContractError, KeyError, TypeError, ValueError) as exc:
        record["error_type"] = type(exc).__name__
        return finish("U", "COMPARISON_CONTRACT_ERROR")
    if type(value) is not bool:
        return finish("U", "COMPARATOR_DID_NOT_RETURN_BOOLEAN")
    return finish("C" if value else "W", "COMPLETE_FINAL_ANSWER_COMPARED", value)


def summarize_scores(policies, records, *, method):
    """Fixed denominator, exactly one mutually exclusive partition per task."""
    expected = {}
    for policy in policies:
        identity = policy["source_position_id"]
        if identity in expected:
            raise ScorePolicyError("Duplicate source position in frozen denominator")
        expected[identity] = policy
    if not expected:
        raise ScorePolicyError("Empty selected population")
    seen = {}
    for record in records:
        identity = record["source_position_id"]
        if record.get("method") != method or identity not in expected or identity in seen:
            raise ScorePolicyError("Foreign method/position or duplicate score record")
        if record.get("contract_sha256") != policy_digest(expected[identity]):
            raise ScorePolicyError("Score record uses a different task policy")
        if record.get("partition") not in ("C", "W", "R", "U", "NA"):
            raise ScorePolicyError("Unknown score partition")
        seen[identity] = record
    counts = dict.fromkeys(("C", "W", "R", "U", "NA"), 0)
    for identity, policy in expected.items():
        record = seen.get(identity)
        partition = record["partition"] if record else ("U" if policy["reference_status"] == "VALID" else "R")
        counts[partition] += 1
    applicable = len(expected)-counts["NA"]
    unresolved = counts["R"]+counts["U"]
    valid = counts["C"]+counts["W"]+counts["U"]
    return dict(method=method, N_selected=len(expected), N_applicable=applicable, N_reference_valid_applicable=valid,
                N_reference_unknown=counts["R"], N_unresolved_valid_reference=counts["U"], correct=counts["C"], incorrect=counts["W"],
                not_applicable=counts["NA"], missing_records=len(expected)-len(seen), partitions=counts,
                lower_bound_applicable=counts["C"]/applicable if applicable else None,
                upper_bound_applicable=(counts["C"]+unresolved)/applicable if applicable else None,
                resolved_point_applicable=counts["C"]/applicable if applicable and not unresolved else None,
                score_on_fixed_valid_reference_subset=counts["C"]/valid if valid and not counts["U"] else None,
                formal_admitted=False)
