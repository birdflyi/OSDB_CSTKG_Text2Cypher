"""A missing deployment is not a general HTTP400 or a free model attempt."""
import asyncio
from dataclasses import replace
import json

import pytest

from engine.config import Endpoint, RetryPolicy, canonical, digest
from engine.pool import DynamicPool, RouteUnavailable
from engine.transport import classify, endpoint_health, failure_kind, retryable
from engine.fake import FakeTransport
from engine.tests.test_scheduler import make_scheduler
from engine.recovery import merge_saved_reply


def rejection(error=None):
    return dict(http_status=400, write_started=True, charged=True,
                raw_body="original evidence", body=dict(error=error or dict(
                    code="400", message="/chat/completions: Invalid model name passed in model=missing.")))


@pytest.mark.parametrize("error", [None, {"code":"model_not_found"}, {"type":"model_not_found"}])
def test_explicit_rejection_retains_paid_original_body_and_has_known_non_generation(error):
    raw = rejection(error)
    result = classify(raw, "missing")
    assert result["status"] == "configuration_error"
    assert result["failure_class"] == "EndpointFatal"
    assert result["generation_outcome_unknown"] is False
    assert result["charged"] is True and result["body"] == raw["body"] and result["raw_body"] == raw["raw_body"]
    assert failure_kind(result) == "model_unavailable"
    assert endpoint_health(result) == "configuration_error"
    assert retryable(result, RetryPolicy(max_attempts=3))
    assert not retryable(dict(result, charged=False), RetryPolicy(max_attempts=3))


@pytest.mark.parametrize("raw", [
    rejection({"message":"context length exceeded", "code":"400"}),
    rejection({"message":"invalid model parameter temperature"}),
    dict(http_status=400,body={"error":"Invalid model name passed"}),
    dict(http_status=400,raw_body="Invalid model name passed"),
    dict(rejection(),http_status=500),
])
def test_other_errors_are_not_reclassified_as_known_pre_generation(raw):
    result = classify(raw, "m")
    assert result["status"] == "unknown"
    assert result["generation_outcome_unknown"] is True
    assert failure_kind(result) != "model_unavailable"


def test_old_unknown_terminal_is_interpreted_without_mutating_evidence():
    raw = dict(rejection(), status="unknown", generation_outcome_unknown=True)
    before = json.dumps(raw, sort_keys=True)
    assert failure_kind(raw) == "model_unavailable"
    assert retryable(raw, RetryPolicy(max_attempts=3))
    assert json.dumps(raw, sort_keys=True) == before


def test_fatal_route_stays_disabled_across_time_and_reload_other_family_route_works():
    async def scenario():
        now = [1.0]
        pool = DynamicPool(2, clock=lambda: now[0])
        bad = Endpoint("a-bad", "http://unused/bad", "family-missing", "unused", 1, family="family")
        good = Endpoint("z-good", "http://unused/good", "family-good", "unused", 1, family="family")
        await pool.update([bad, good])
        permit = await pool.acquire("family-logical", 1)
        assert permit.endpoint.name == bad.name
        event = await pool.record(permit, endpoint_health(classify(rejection(),bad.model)))
        await permit.release()
        assert event["transition"] == "endpoint_fatal" and event["effective_cap"] == 0
        now[0] += 100000
        await pool.update([good])
        await pool.update([replace(bad,cap=2),good])
        for _ in range(3):
            permit = await pool.acquire("family-logical", 1)
            assert permit.endpoint.name == good.name
            await permit.release()
        with pytest.raises(ValueError, match="corrected endpoint identity"):
            await pool.reset(bad.name)
        await pool.update([bad])
        with pytest.raises(RouteUnavailable, match="configuration_error"):
            await pool.acquire("family-logical",1)
        snapshot = pool.snapshot()
        assert snapshot["endpoints"][0]["effective_cap"] == 0
        json.dumps(snapshot, allow_nan=False)
    asyncio.run(scenario())


def test_scheduler_retries_only_on_healthy_route_and_keeps_both_paid_attempts(tmp_path):
    endpoints = []
    def respond(call, wire, endpoint):
        endpoints.append(endpoint.name)
        if endpoint.name == "a-bad":
            return classify(rejection(), endpoint.model)
        return dict(status="ok",http_status=200,content="RETURN 1")
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path,cap=1,count=3,transport=FakeTransport(0,respond))
        scheduler.spec = replace(scheduler.spec,retry=RetryPolicy(max_attempts=3,wait_seconds=10))
        delays=[]
        async def record_delay(seconds):
            delays.append(seconds)
        scheduler.retry_sleep=record_delay
        await scheduler.pool.update([
            Endpoint("a-bad","http://unused/bad","m","unused",1),
            Endpoint("z-good","http://unused/good","m","unused",1)])
        await scheduler.run(items)
        assert endpoints.count("a-bad") == 1 and endpoints.count("z-good") == 3
        assert ledger.summary()["new_charged"] == 4 and not ledger.summary()["pending"]
        assert len(delays)==1 and 9 < delays[0] <= 10
        assert sum(t["status"]=="configuration_error" for t in ledger.terminals.values()) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("old_status", ["unknown", "configuration_error"])
def test_saved_rejection_keeps_history_and_retries_original_attempt_number(tmp_path, old_status):
    async def scenario():
        scheduler, items, transport, ledger = await make_scheduler(tmp_path,cap=1,count=1,transport=FakeTransport(0))
        scheduler.spec=replace(scheduler.spec,retry=RetryPolicy(max_attempts=3,wait_seconds=10))
        old_dir=tmp_path/'old';old_dir.mkdir()
        request=old_dir/'request.json';request.write_bytes(items[0].wire)
        terminal=old_dir/'terminal.json'
        value=dict(rejection(),status=old_status,request_id='old-paid',budget_slot=1,
                   generation_outcome_unknown=old_status=='unknown',attempt_no=1,logical_key=items[0].key)
        terminal.write_bytes(canonical(value)); before=terminal.read_bytes()
        previous={}
        record=dict(request_id='old-paid',logical_key=items[0].key,slot=1,request_path=str(request),request_sha256=digest(items[0].wire))
        merge_saved_reply(previous,record)
        scheduler.saved_replies=previous
        delays=[]
        async def delay(seconds):delays.append(seconds)
        scheduler.retry_sleep=delay
        paths=await scheduler.run(items)
        assert transport.calls==1 and ledger.summary()['new_charged']==1
        assert next(iter(ledger.terminals.values()))['attempt_no']==2
        history=[event for event in scheduler.events if event.get('logical_key')==items[0].key]
        assert len(history)==2 and history[0]['reused'] is True
        assert terminal.read_bytes()==before and len(delays)==1
        # A later successful paid reply may be merged after either historical
        # label, while the original failed terminal remains unchanged.
        new_dir=tmp_path/'new';new_dir.mkdir()
        new_request=new_dir/'request.json';new_request.write_bytes(items[0].wire)
        (new_dir/'terminal.json').write_bytes(canonical(dict(request_id='new-paid',budget_slot=2,status='ok')))
        merge_saved_reply(previous,dict(request_id='new-paid',logical_key=items[0].key,slot=2,request_path=str(new_request),request_sha256=digest(items[0].wire)))
        assert len(previous[items[0].key]['history'])==2
    asyncio.run(scenario())
