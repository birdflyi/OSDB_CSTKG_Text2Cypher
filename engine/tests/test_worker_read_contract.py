from __future__ import annotations

from types import SimpleNamespace
import pytest

from engine.workers.neo4j import ReadHandler


class Cursor:
    def __init__(self, rows):
        self.rows = rows

    def keys(self):
        return ["n"]

    def __iter__(self):
        return iter(self.rows)

    def consume(self):
        return SimpleNamespace(query_type="r")


class Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def run(self, query, params):
        self.driver.queries.append((query, params))
        return Cursor([] if query[0].startswith("EXPLAIN") else [{"n": 1}, {"n": 2}])


class Driver:
    def __init__(self):
        self.queries = []
        self.sessions = 0

    def session(self, **kwargs):
        self.sessions += 1
        return Session(self)


def test_explain_precedes_query_with_separate_session_and_frozen_limits():
    driver = Driver()
    handler = ReadHandler(
        driver, None, lambda query: query, lambda x: x, lambda text, timeout: (text, timeout)
    )
    one = handler(dict(query="RETURN 1", parameters={}))
    two = handler(dict(query="RETURN 2", parameters={}))
    assert driver.sessions == 2
    assert driver.queries[0] == (("EXPLAIN RETURN 1", 30), {})
    assert driver.queries[1] == (("RETURN 1", 60), {})
    assert one["rows"] == [[1], [2]] and one["columns"] == ["n"] and one["complete"]


def test_candidates_and_parameter_validation():
    handler = ReadHandler(
        Driver(),
        SimpleNamespace(find=lambda text: [{"name": text}]),
        lambda q: q,
        lambda x: x,
        lambda text, timeout: (text, timeout),
    )
    assert handler(dict(kind="candidates", question="entity"))["status"] == "CANDIDATES"
    with pytest.raises(ValueError):
        handler(dict(query="RETURN 1", parameters=[]))
