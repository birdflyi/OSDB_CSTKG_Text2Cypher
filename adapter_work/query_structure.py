"""Conservative, offline Cypher lexical features, not an AST or difficulty score.

References may be inspected by cohort preparation, never by model input assembly.
Unknown syntax is retained in its own stratum rather than dropped from sampling.
"""
from __future__ import annotations

import hashlib
import re


TOKEN = re.compile(
    r"(?P<space>\s+)|(?P<line>//[^\n]*)|(?P<block>/\*[\s\S]*?\*/)"
    r"|(?P<string>'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\")"
    r"|(?P<quoted>`(?:``|[^`])*`)"
    r"|(?P<number>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
    r"|(?P<word>[A-Za-z_][A-Za-z_0-9]*)"
    r"|(?P<symbol><-|->|\.\.|<=|>=|<>|!=|[^\w\s])",
    re.UNICODE,
)
AGGREGATES = {"COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT", "STDEV", "STDEVP", "PERCENTILECONT", "PERCENTILEDISC"}


def extract_structure(query: str | None) -> dict:
    """Return deterministic lexical features; reject malformed token boundaries.

    Backtick identifiers, comments, literal strings and property/label names are
    never interpreted as keyword operators. This deliberately does not validate
    query semantics, infer actual hop cost, or determine method applicability.
    """
    unknown = {"status": "unknown", "structure_bucket": "unknown", "structure_group": "unknown", "features": None,
               "literal_normalized_fingerprint": None, "is_ast": False}
    if not isinstance(query, str) or not query.strip():
        return {**unknown, "reason": "missing_reference_query"}
    tokens = []
    offset = 0
    while offset < len(query):
        match = TOKEN.match(query, offset)
        if match is None or (match.lastgroup == "symbol" and match.group() in "'\"`"):
            return {**unknown, "reason": "unrecognized_or_unterminated_token"}
        if query.startswith("/*", offset) and match.lastgroup != "block":
            return {**unknown, "reason": "unterminated_comment"}
        if match.lastgroup not in {"space", "line", "block"}:
            tokens.append((match.lastgroup, match.group()))
        offset = match.end()
    stack = []
    pairs = {"(": ")", "[": "]", "{": "}"}
    for kind, value in tokens:
        if kind != "symbol":
            continue
        if value in pairs:
            stack.append(value)
        elif value in pairs.values():
            if not stack or pairs[stack.pop()] != value:
                return {**unknown, "reason": "unbalanced_delimiters"}
    if stack:
        return {**unknown, "reason": "unbalanced_delimiters"}

    def keyword(i: int) -> str:
        kind, value = tokens[i]
        if kind != "word" or (i and tokens[i - 1][1] in {".", ":", "$"}):
            return ""
        if i + 1 < len(tokens) and tokens[i + 1][1] == ":":
            return ""  # map key
        return value.upper()

    keywords = [keyword(i) for i in range(len(tokens))]
    aggregates = sum(k in AGGREGATES and i + 1 < len(tokens) and tokens[i + 1][1] == "("
                     for i, k in enumerate(keywords))
    subqueries = sum(k in {"CALL", "EXISTS", "COUNT", "COLLECT"} and
                     i + 1 < len(tokens) and tokens[i + 1][1] == "{"
                     for i, k in enumerate(keywords))
    # Only CALL's immediate optional variable-scope list may precede its body.
    # A later RETURN map does not turn a procedure call into a subquery.
    for i, k in enumerate(keywords):
        if k == "CALL" and i + 1 < len(tokens) and tokens[i + 1][1] == "(":
            depth, j = 1, i + 2
            while j < len(tokens) and depth:
                if tokens[j] == ("symbol", "("):
                    depth += 1
                elif tokens[j] == ("symbol", ")"):
                    depth -= 1
                j += 1
            subqueries += j < len(tokens) and tokens[j] == ("symbol", "{")
    relationships = 0
    variable_length = 0
    for i, token in enumerate(tokens):
        if token == ("symbol", "[") and i and tokens[i - 1][1] in {"-", "<-"}:
            depth, j = 1, i + 1
            while j < len(tokens) and depth:
                if tokens[j] == ("symbol", "["):
                    depth += 1
                elif tokens[j] == ("symbol", "]"):
                    depth -= 1
                j += 1
            if j < len(tokens) and tokens[j][1] in {"-", "->"}:
                relationships += 1
                nested = 0
                top_level_star = False
                for kind, value in tokens[i + 1:j - 1]:
                    if kind != "symbol":
                        continue
                    if value in {"{", "(", "["}:
                        nested += 1
                    elif value in {"}", ")", "]"}:
                        nested -= 1
                    elif value == "*" and nested == 0:
                        top_level_star = True
                variable_length += top_level_star
        if token[1] == ")" and i + 2 < len(tokens):
            # Anonymous unbracketed relationship: ()-->() or ()--().
            if tokens[i + 1][1] in {"-", "<-"} and tokens[i + 2][1] in {"-", "->"}:
                relationships += 1
    features = {
        "match_clauses": keywords.count("MATCH"), "with_clauses": keywords.count("WITH"),
        "aggregate_calls": aggregates, "subquery_markers": subqueries,
        "union_markers": keywords.count("UNION"), "relationship_patterns": relationships,
        "variable_length_patterns": variable_length, "optional_markers": keywords.count("OPTIONAL"),
        "distinct_markers": keywords.count("DISTINCT"), "order_markers": keywords.count("ORDER"),
        "unwind_markers": keywords.count("UNWIND"), "limit_markers": keywords.count("LIMIT"),
    }
    if subqueries or variable_length or features["union_markers"]:
        bucket = "nested_or_variable"
    elif aggregates or relationships > 1 or features["match_clauses"] > 1 or any(
        features[k] for k in ("with_clauses", "optional_markers", "distinct_markers", "order_markers", "unwind_markers")
    ):
        bucket = "composed"
    else:
        bucket = "simple"
    normalized = " ".join("<literal>" if k in {"string", "number"} else v for k, v in tokens)
    return {"status": "lexical_only", "structure_bucket": bucket, "structure_group": bucket, "features": features,
            "literal_normalized_fingerprint": hashlib.sha256(normalized.encode()).hexdigest(),
            "is_ast": False, "reason": "lexical_features_not_execution_difficulty"}
