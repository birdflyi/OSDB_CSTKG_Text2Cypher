"""New reply/native-journal integration checks with fabricated HTTP bytes only.

No model, live graph, socket or reference-answer input. This does not rerun
old native engine acceptance or turn simulated DB receipts into measured cost.
"""
from pathlib import Path
import hashlib
import json
import sys
import traceback

R = Path('D:/ch7')
REPO = R/'code/OSDB_CSTKG_Text2Cypher_t1_tugraph'
sys.path.insert(0, str(REPO/'experiment-harness/public_benchmark_v1'))
from tugraph_execution import NativeQueryJournal, JournalConflict
from tugraph_schema import schema_from_import_config
from test_tugraph_schema import CONFIG
from text2gql_response_boundary_dev_20261027 import replay_path

OUT = R/'reports/text2gql_response_boundary_dev_v2_20261027'


def native(rows, columns=('n',)):
    raw = json.dumps({'header': [{'name': c} for c in columns], 'result': rows,
                      'size': len(rows)}).encode()
    return {'raw': raw, 'http_status': 200, 'transport_complete': True, 'execution_complete': True}


def explain():
    return native([['ReadOnly:1\nExecution Plan:\nFabricated fixture\n']], ('@plan',))


def reply(text, **kw):
    return dict({'http_status': 200, 'finish_reason': 'stop', 'content': text}, **kw)


def main():
    OUT.mkdir(exist_ok=False)
    checks = [];schema = schema_from_import_config(CONFIG)
    initial = [{'role': 'system', 'content': 'Fabricated contract'},
               {'role': 'user', 'content': 'Fabricated question; no answer supplied'}]
    def run(name, arm, records, wires):
        seen = []
        def transport(payload, wall, cap):
            seen.append(json.loads(payload))
            if not wires:
                raise AssertionError('Unplanned physical request')
            return wires.pop(0)
        journal = NativeQueryJournal(OUT/name, transport)
        result = replay_path(arm, initial, schema, records, journal,
                             request_prefix='fixture', graph='fabricated_graph')
        return result, seen
    def check(name, fn):
        try:
            fn();checks.append({'name': name, 'passed': True})
        except Exception:
            checks.append({'name': name, 'passed': False, 'error': traceback.format_exc()})
    def direct_query():
        query='CALL { RETURN 1 AS n UNION RETURN 2 AS n } RETURN n'
        v,seen=run('raw_native','direct',[reply(query)],[explain(),native([[1],[2]])])
        assert v['final_answer']['rows']==[[1],[2]] and seen[-1]['script']==query
        assert v['new_physical_requests']==2 and v['next_feedback_messages'] is None
    check('Direct_raw_native_query_not_finite_IR_or_JSON_envelope',direct_query)
    def repaired():
        err={'raw':b'{"error_message":"ParserException: bad syntax"}','http_status':400,
             'transport_complete':True,'execution_complete':False}
        v,seen=run('repaired','direct',[reply('RETURN bad'),reply('RETURN 2 AS n')],
                   [err,explain(),native([[2]])])
        assert v['initial_answer']['status']=='EXECUTION_ERROR' and v['final_answer']['rows']==[[2]]
        assert len(v['history'])==2 and len(seen)==3 and v['next_feedback_messages'] is None
        assert v['model_usage_from_receipts']==[None,None] and v['monetary_cost'] is None
    check('one_operational_feedback_retains_initial_and_actual_final',repaired)
    def completed(rows,name):
        v,seen=run(name,'direct',[reply('RETURN 999 AS n'),reply('RETURN 1 AS n')],[explain(),native(rows)])
        assert v['final_answer']['rows']==rows and v['response_records_consumed']==1
        assert v['next_feedback_messages'] is None and len(seen)==2
    check('valid_empty_answer_cannot_trigger_gold_feedback',lambda:completed([],'empty'))
    check('valid_arbitrary_answer_cannot_trigger_gold_feedback',lambda:completed([[999]],'arbitrary'))
    def ours():
        selection={'nodes':[{'id':'p','label':'Person'}],'edges':[],'where':[],
                   'returns':[{'field':'p.id','alias':'id'}]}
        text=json.dumps({'status':'ANSWER','calls':[selection],'combine':{'op':'single'}})
        v,seen=run('finite','ours',[reply(text)],[explain(),native([[1]],('id',))])
        assert v['final_answer']=={'status':'EXECUTED','native_status':'EXECUTED',
                                  'complete':True,'columns':['id'],'rows':[[1]]}
        assert len(seen)==2
    check('Ours_uses_unchanged_native_finite_compiler_and_journal',ours)
    def unsupported():
        r=reply('{"status":"UNSUPPORTED","reason":"outside finite IR"}')
        v,seen=run('unsupported','ours',[r,r],[])
        assert v['final_answer']['status']=='UNSUPPORTED' and len(v['history'])==2
        assert v['next_feedback_messages'] is None and not seen
    check('finite_decline_is_not_correct_and_max_one_repair',unsupported)
    def duplicate():
        v,seen=run('duplicate','ours',[reply('{"status":"ANSWER","status":"UNSUPPORTED"}')],[])
        assert v['final_answer']['status']=='PROTOCOL_ERROR' and not seen
        assert len(v['next_feedback_messages'])==4
    check('duplicate_JSON_keys_rejected_without_DB',duplicate)
    def fenced():
        v,seen=run('fenced','direct',[reply('```cypher\nRETURN 1 AS n\n```')],[])
        assert v['final_answer']['status']=='EXECUTION_ERROR' and not seen
    check('Direct_code_fence_not_silently_extracted',fenced)
    def no_db(name,record,status):
        v,seen=run(name,'direct',[record,reply('RETURN 1 AS n')],[])
        assert v['final_answer']['status']==status and not seen
        assert v['response_records_consumed']==1 and v['next_feedback_messages'] is None
    check('length_is_not_empty_or_infrastructure_failure',lambda:no_db('length',reply('',finish_reason='length'),'MODEL_TRUNCATED'))
    check('missing_model_terminal_stays_nonterminal_UNKNOWN',lambda:no_db('unknown',{'outcome_unknown':True},'UNKNOWN'))
    check('HTTP_failure_has_no_semantic_feedback',lambda:no_db('http500',reply('',http_status=500),'INFRASTRUCTURE_ERROR'))
    check('model_refusal_not_a_correct_answer',lambda:no_db('refusal',reply('',finish_reason='content_filter'),'REFUSED'))
    def native_failure(name,wire,status):
        v,seen=run(name,'direct',[reply('RETURN 1 AS n'),reply('RETURN 2 AS n')],[explain(),wire])
        assert v['final_answer']['status']==status and len(seen)==2
        assert v['response_records_consumed']==1 and v['new_physical_requests']==2
        assert v['next_feedback_messages'] is None
    partial={'raw':b'{','http_status':200,'transport_complete':False,
             'execution_complete':False,'failure_kind':'TimeoutError'}
    check('unknown_DB_execution_stays_unknown_and_is_not_retried',lambda:native_failure('dbunknown',partial,'UNKNOWN'))
    cap=dict(partial,failure_kind='BYTE_CAP')
    check('DB_byte_cap_not_empty_success_or_retry',lambda:native_failure('bytecap',cap,'TRUNCATED'))
    malformed={'raw':b'{}','http_status':200,'transport_complete':True,'execution_complete':True}
    check('malformed_DB_success_not_valid_answer',lambda:native_failure('malformed',malformed,'MALFORMED_ANSWER'))
    def journal_conflict():
        class Conflicting:
            def execute(self,*args):raise JournalConflict('pending previous request')
        try:
            replay_path('direct',initial,schema,[reply('RETURN 1 AS n')],Conflicting(),
                        request_prefix='pending',graph='fabricated')
        except JournalConflict:return
        raise AssertionError('Unknown physical state was swallowed')
    check('journal_conflict_propagates_for_owner_audit_not_zero_cost_failure',journal_conflict)
    result={'kind':'FABRICATED_NATIVE_REPLY_BRIDGE_CHECKS_NOT_MODEL_OR_DB_EX',
            'planned':len(checks),'completed':len(checks),'passed':sum(c['passed'] for c in checks),
            'failed':sum(not c['passed'] for c in checks),'undispatched':0,'checks':checks,
            'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
               [Path(__file__),Path(__file__).with_name('text2gql_response_boundary_dev_20261027.py')]},
            'new_model_calls':0,'new_live_database_requests':0,'new_network_calls':0,
            'real_task_or_reference_values_read':0,'formal_admission':False,'budget_activated':False}
    (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))
    if result['failed']:raise SystemExit(1)


if __name__=='__main__':main()
