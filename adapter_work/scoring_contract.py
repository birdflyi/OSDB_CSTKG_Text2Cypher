"""Pure candidate numeric equality for frozen Neo4j answer records.

This is a project result contract, not an official benchmark evaluator or a
complete implementation of Cypher equality. Existing scorers are unchanged.
No driver, model, database or filesystem operations occur here.
"""

from collections import Counter
import math
import re
import struct


METRIC_ID = "project_numeric_exact_output_v1"
CAPTURE_PROFILE = "neo4j-5.28.1-typed-answer-preparation-v1"


class ScoringContractError(ValueError):
    """An answer cannot safely be compared under the declared contract."""


class UnresolvedNumericValue(ScoringContractError):
    """The contract has no admitted rule for a non-finite number."""


def _fields(value, names):
    if set(value) != {"t", *names}:
        raise ScoringContractError("Unexpected encoded value fields")


def _text(value):
    return type(value) is str and bool(value)


def _integer(value):
    return type(value) is int


def _cell_has_entities(value, depth=0):
    """Check the concrete capture format and derive entity presence.

    Metadata (labels, dates, paths) is distinct from typed child values. This
    validates the encoder's shape, not all Cypher value semantics.
    """
    if depth > 64 or type(value) is not dict or type(value.get("t")) is not str:
        raise ScoringContractError("Expected bounded typed cell")
    kind = value["t"]
    child = lambda item: _cell_has_entities(item, depth + 1)
    if kind == "null":
        _fields(value, ())
        return False
    if kind in ("bool", "int", "float64", "str", "bytes"):
        _fields(value, ("v",))
        item = value["v"]
        valid = {
            "bool": lambda: type(item) is bool,
            "str": lambda: type(item) is str,
            "int": lambda: type(item) is str and bool(re.fullmatch(r"0|-?[1-9][0-9]*", item)),
            "float64": lambda: type(item) is str and bool(re.fullmatch(r"[0-9a-f]{16}", item)),
            "bytes": lambda: type(item) is str and bool(re.fullmatch(r"(?:[0-9a-f]{2})*", item)),
        }[kind]()
        if not valid:
            raise ScoringContractError("Malformed encoded " + kind)
        return False
    if kind == "list":
        _fields(value, ("v",))
        if type(value["v"]) is not list:
            raise ScoringContractError("Malformed encoded list")
        # Materialize all checks: any(generator) would skip later bad cells.
        return any([child(item) for item in value["v"]])
    if kind == "map":
        _fields(value, ("v",))
        pairs = value["v"]
        if type(pairs) is not list or any(type(pair) is not list or len(pair) != 2 or type(pair[0]) is not str for pair in pairs):
            raise ScoringContractError("Malformed encoded map")
        keys = [pair[0] for pair in pairs]
        if keys != sorted(set(keys)):
            raise ScoringContractError("Noncanonical encoded map keys")
        return any([child(pair[1]) for pair in pairs])
    if kind in ("node", "relationship"):
        names = ("element_id", "labels", "properties") if kind == "node" else ("element_id", "type", "start", "end", "properties")
        _fields(value, names)
        if not _text(value["element_id"]):
            raise ScoringContractError("Missing entity identity")
        if kind == "node":
            labels = value["labels"]
            if type(labels) is not list or any(type(label) is not str for label in labels) or labels != sorted(set(labels)):
                raise ScoringContractError("Malformed node labels")
        elif not all(_text(value[key]) for key in ("type", "start", "end")):
            raise ScoringContractError("Malformed relationship identity")
        if type(value["properties"]) is not dict or value["properties"].get("t") != "map":
            raise ScoringContractError("Entity properties require typed map")
        child(value["properties"])
        return True
    if kind == "path":
        _fields(value, ("nodes", "relationships", "directions"))
        nodes, edges, directions = value["nodes"], value["relationships"], value["directions"]
        if any(type(items) is not list for items in (nodes, edges, directions)) or len(nodes) != len(edges)+1 or len(directions) != len(edges):
            raise ScoringContractError("Malformed path shape")
        for item in nodes:
            if type(item) is not dict or item.get("t") != "node":
                raise ScoringContractError("Path requires nodes")
            child(item)
        for index, edge in enumerate(edges):
            if type(edge) is not dict or edge.get("t") != "relationship":
                raise ScoringContractError("Path requires relationships")
            child(edge)
            direction = directions[index]
            if type(direction) is not int or direction not in (-1, 1):
                raise ScoringContractError("Malformed path direction")
            start, end = (edge["start"], edge["end"]) if direction == 1 else (edge["end"], edge["start"])
            if (start, end) != (nodes[index]["element_id"], nodes[index+1]["element_id"]):
                raise ScoringContractError("Disconnected path")
        return True
    if kind == "point":
        _fields(value, ("srid", "coordinates"))
        coordinates = value["coordinates"]
        if not _integer(value["srid"]) or type(coordinates) is not list:
            raise ScoringContractError("Malformed point metadata")
        for item in coordinates:
            if type(item) is not dict or item.get("t") not in ("int", "float64"):
                raise ScoringContractError("Point requires numeric coordinates")
            child(item)
        return False
    if kind in ("date", "time", "datetime", "duration", "python_timedelta"):
        date_fields = ("year", "month", "day")
        clock_fields = ("hour", "minute", "second", "nanosecond")
        names = {
            "date": date_fields,
            "time": (*clock_fields, "offset", "zone"),
            "datetime": (*date_fields, *clock_fields, "offset", "zone"),
            "duration": ("months", "days", "seconds", "nanoseconds"),
            "python_timedelta": ("days", "seconds", "microseconds"),
        }[kind]
        _fields(value, names)
        if any(not _integer(value[name]) for name in names if name not in ("offset", "zone")):
            raise ScoringContractError("Malformed temporal integers")
        if kind in ("time", "datetime"):
            offset, zone = value["offset"], value["zone"]
            if offset is not None and (type(offset) is not list or len(offset) != 3 or not all(_integer(item) for item in offset)):
                raise ScoringContractError("Malformed temporal offset")
            if zone is not None and type(zone) is not str:
                raise ScoringContractError("Malformed temporal zone")
        return False
    raise ScoringContractError("Unknown encoded value tag: " + kind)


def numeric_key(value):
    """Normalize encoded numeric cells, preserving all other structure.

    Float ratios are exact; converting an integer to float would lose large
    integer distinctions. Booleans remain tagged booleans. Lists keep order.
    This is deliberately zero-tolerance, not approximate float matching.
    """
    if isinstance(value, dict):
        kind = value.get("t")
        if kind == "int":
            if set(value) != {"t", "v"} or type(value["v"]) is not str:
                raise ScoringContractError("Malformed encoded integer")
            try:
                number = int(value["v"])
            except ValueError as exc:
                raise ScoringContractError("Malformed encoded integer") from exc
            if str(number) != value["v"]:
                raise ScoringContractError("Noncanonical encoded integer")
            return ("number", number, 1)
        if kind == "float64":
            try:
                if set(value) != {"t", "v"} or type(value["v"]) is not str or len(value["v"]) != 16:
                    raise ValueError("Invalid IEEE754 encoding")
                number = struct.unpack("!d", bytes.fromhex(value["v"]))[0]
            except (ValueError, struct.error) as exc:
                raise ScoringContractError("Malformed encoded float64") from exc
            if not math.isfinite(number):
                raise UnresolvedNumericValue("Non-finite value requires a separate frozen policy")
            numerator, denominator = number.as_integer_ratio()
            return ("number", numerator, denominator)
        if any(type(key) is not str for key in value):
            raise ScoringContractError("Non-string encoded object key")
        return ("object", tuple((key, numeric_key(item)) for key, item in sorted(value.items())))
    if isinstance(value, list):
        return ("array", tuple(numeric_key(item) for item in value))
    if value is None:
        return ("none",)
    if type(value) in (str, int, bool):
        return (type(value).__name__, value)
    raise ScoringContractError("Unexpected primitive in encoded answer")


def compare_numeric_exact(reference, prediction, *, ordered):
    """Compare complete answer tables without relaxing graph identity scope.

    Unknown/incomplete answers return None. Bad identity or encoding raises a
    contract error, which the caller must preserve as unresolved, not wrong.
    Only aliases are ignored. No column permutation, truncation, or coercion.
    """
    if type(ordered) is not bool:
        raise ScoringContractError("Frozen ordering rule must be boolean")
    if reference is None or prediction is None:
        return None
    for answer in (reference, prediction):
        if answer.get("status") != "EXECUTED" or answer.get("complete") is not True:
            return None
        if answer.get("profile") != CAPTURE_PROFILE:
            raise ScoringContractError("Foreign answer encoding profile")
        columns, rows = answer.get("columns"), answer.get("rows")
        if type(columns) is not list or any(type(c) is not str for c in columns):
            raise ScoringContractError("Malformed columns")
        if type(rows) is not list or any(type(row) is not list or len(row) != len(columns) for row in rows):
            raise ScoringContractError("Malformed row width")
        if type(answer.get("contains_graph_entities")) is not bool:
            raise ScoringContractError("Missing graph entity presence flag")
        contains_entities = any([_cell_has_entities(cell) for row in rows for cell in row])
        if contains_entities != answer["contains_graph_entities"]:
            raise ScoringContractError("Graph entity presence flag contradicts typed cells")
        scope = answer.get("scope")
        if type(scope) is not dict or any(type(scope.get(k)) is not str or not scope[k] for k in ("snapshot_sha256", "database", "capture_id")):
            raise ScoringContractError("Missing capture scope")
    left, right = reference["scope"], prediction["scope"]
    if any(left[key] != right[key] for key in ("snapshot_sha256", "database")):
        raise ScoringContractError("Different snapshot/database")
    if (reference["contains_graph_entities"] or prediction["contains_graph_entities"]) and left["capture_id"] != right["capture_id"]:
        raise ScoringContractError("Cross-capture entity identity is not certified")
    if len(reference["columns"]) != len(prediction["columns"]):
        return False
    a = [numeric_key(row) for row in reference["rows"]]
    b = [numeric_key(row) for row in prediction["rows"]]
    return a == b if ordered else Counter(a) == Counter(b)
