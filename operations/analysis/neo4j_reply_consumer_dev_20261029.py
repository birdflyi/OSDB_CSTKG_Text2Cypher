"""Connect pinned model replies to native final-answer evidence, without a sender.

Driver construction/ownership, graph immutability, real DB acceptance, scoring
and engine registration remain external. Tests supply fake drivers only.
"""

from copy import deepcopy
import os
from pathlib import Path
import time

from neo4j.exceptions import ClientError, DriverError, Neo4jError

from audited_model_reply_dev_20261029 import RecordedReply, strict_json
from neo4j_native_answer_contract_dev_20261029 import encoded
from neo4j_native_plan_dev_20261029 import NativePlanChain, PlanEvidence, PreparedPlan
from neo4j_native_selection_dev_20261029 import NativeSchema, UnsupportedSelection
from neo4j_read_capture_dev_20261029 import NativeReadCapture
from public_read_guard import ReadOnlyViolation


def save_new(path, value):
    raw = encoded(value)
    with Path(path).open('xb') as stream:
        if stream.write(raw) != len(raw):
            raise OSError('Short outcome write')
        stream.flush()
        os.fsync(stream.fileno())


def failure(status, error_type=None):
    return dict(answer=dict(status=status,complete=False,columns=None,rows=None),
                error_type=error_type,new_model_calls=0,monetary_cost=None,formal_admission=False)


def _plan(content, schema):
    if len(content) > 200000:
        raise UnsupportedSelection('IR text exceeds its bounded envelope')
    value = strict_json(content)
    if type(value) is not dict:
        raise UnsupportedSelection('Expected one IR object')
    if value.get('status') == 'UNSUPPORTED':
        if (set(value) != {'status','reason'} or type(value['reason']) is not str
                or not value['reason'].strip() or len(value['reason']) > 2000):
            raise UnsupportedSelection('Unsupported envelope needs one bounded reason')
        return None
    return PreparedPlan(value,schema)


def _feedback(source, result):
    # Only an unsent operation diagnostic. No scorer, reference, correct count
    # or gold query enters this module; feedback-phase replies cannot recur.
    if source.identity.phase != 'initial' or result['answer']['status'] not in (
            'PROTOCOL_ERROR','GUARD_REJECTED','EXECUTION_ERROR'):
        return None
    body = deepcopy(source.request)
    body['messages'] += [dict(role='assistant',content=source.terminal['content']),
        dict(role='user',content='Operation failed: '+result['answer']['status']+'. '+
             str(result.get('error_type') or '')+'. Return one corrected answer under the same contract.')]
    return dict(request=body,target_phase='feedback',dispatched=False,
                maximum_operation_feedback=1,uses_reference=False)


def consume_recorded_reply(source, schema, *, driver, directory, capture_settings,
                           run_controls=False, allow_feedback=False):
    """Persist source before reads and method final before shared controls.

    Ordinary protocol/guard/DB errors save an incomplete outcome. Evidence,
    ownership or unexpected implementation failures propagate and keep partial
    files. No retry or implicit DB connection is performed for non-READY replies.
    Controls share the Ours plan; they are not independent model methods.
    """
    if not isinstance(source,RecordedReply):
        raise ValueError('Validated pinned model reply required')
    if source.identity.benchmark not in ('mindthequery','zograscope'):
        raise ValueError('This prepared native consumer supports MTQ/ZOGRASCOPE only')
    if type(run_controls) is not bool or type(allow_feedback) is not bool:
        raise ValueError('Explicit boolean control/feedback policy required')
    if run_controls and source.identity.method != 'ours':
        raise ValueError('Shared-plan controls require an Ours source, not a Direct query')
    if source.identity.method == 'ours' and source.status == 'READY':
        expected_format = {'mindthequery':'MTQ','zograscope':'ZOGRASCOPE'}[source.identity.benchmark]
        if not isinstance(schema,NativeSchema) or schema.format != expected_format:
            raise ValueError('Native schema format must match the benchmark public-input track')
    directory = Path(directory)
    directory.mkdir(parents=True,exist_ok=False)
    source_link = source.source_link()
    save_new(directory/'source.json',source_link)
    started = time.monotonic()
    prepared, controls = None, None
    if source.status != 'READY':
        result = failure(source.status)
    else:
        try:
            if source.identity.method == 'ours':
                prepared = _plan(source.terminal['content'],schema)
                result = failure('UNSUPPORTED') if prepared is None else None
            else:
                result = None
        except ValueError as error:
            result = failure('PROTOCOL_ERROR',type(error).__name__)
        if result is None:
            evidence = PlanEvidence(directory/'capture',max_record_bytes=16000000)
            settings = dict(capture_settings)
            if 'emit' in settings or 'driver' in settings:
                raise ValueError('Capture writer/driver must not be overridden')
            try:
                with NativeReadCapture(driver,emit=evidence,**settings) as capture:
                    scope = capture.scope
                    def event(kind,**fields):
                        return evidence(dict(kind=kind,capture_id=scope.capture_id,
                            database=scope.database,snapshot_sha256=scope.snapshot_sha256,
                            epoch=time.time(),**fields))
                    event('source_reply_bound',source=source_link)
                    if prepared is None:
                        # Pass the original Cypher to the shared guard. The guard
                        # strips outer whitespace/one terminal semicolon; it does
                        # not extract code fences, invent parameters or use IR.
                        answer = capture.read(source.terminal['content'],{},purpose='method')
                        result = dict(answer=answer,representation='direct_native_query',
                            completed_read_operations=1,new_model_calls=0,monetary_cost=None)
                        event('method_final_saved',result=result)
                    else:
                        chain = NativePlanChain(prepared,capture,evidence)
                        result = chain.run_method()
                    save_new(directory/'method_final.json',dict(source=source_link,result=result))
                    if run_controls:
                        # A control failure cannot erase an already sealed method.
                        try:
                            controls = dict(status='COMPLETED',results=chain.run_controls())
                        except (ReadOnlyViolation,Neo4jError,DriverError) as error:
                            controls = dict(status='INCOMPLETE',error_type=type(error).__name__)
            except ReadOnlyViolation as error:
                result = failure('GUARD_REJECTED',type(error).__name__)
            except (Neo4jError,DriverError) as error:
                # An error during close must not replace an already saved answer.
                if (directory/'method_final.json').exists():
                    raise
                status = 'EXECUTION_ERROR' if isinstance(error,ClientError) else 'INFRASTRUCTURE_ERROR'
                result = failure(status,type(error).__name__)
    outcome = dict(source=source_link,method=result,controls=controls,
        feedback_draft=_feedback(source,result) if allow_feedback else None,
        feedback_dispatched=False,consumer_wall_seconds=time.monotonic()-started,
        new_model_calls=0,monetary_cost=None,formal_admission=False,
        adapter_registered=False,graph_immutability_verified=False,
        author_primary_scoring_integrated=False)
    save_new(directory/'outcome.json',outcome)
    return outcome
