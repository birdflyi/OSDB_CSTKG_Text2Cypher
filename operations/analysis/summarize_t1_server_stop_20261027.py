"""Summarize stored liveness/stop evidence; no network or generation."""
from pathlib import Path
import json
from osdb_scoped_attempts_dev import read,sha,immutable

ROOT=Path('D:/ch7');AUDIT=ROOT/'reports/t1_server_liveness_audit_20261027'
ROUTE=ROOT/'reports/t1_node3107_route_recheck_v1_20261027'


def main():
 stop=read(AUDIT/'capacity_stop_summary.json');live=read(AUDIT/'remote_liveness.json');boot=read(AUDIT/'node_boot_and_monitor_processes.json')
 approval=read(ROUTE/'human96_approval_receipt.json');generation=read(ROUTE/'generation_summary.json')
 old=read(ROUTE/'readiness.json');new=read(ROUTE/'new_relay_readiness.json');local=read(ROUTE/'local_readiness.json')
 assert live['ssh_returncode']==0 and live['remote']['workers']==[] and live['remote']['real_experiment_jobs']==[]
 assert len(boot['panes'])==2 and all(':0:' in p for p in boot['panes'])
 assert stop['transport_failures']==22 and stop['error_types']=={'ConnectionResetError':22}
 assert stop['planned']==512 and stop['dispatched']==299 and stop['undispatched']==213
 assert generation['slot']==6665 and generation['accepted'] is False
 assert approval['new_cap']==12096 and new['http_status']==200 and local['http_status']==200
 files=[AUDIT/'capacity_stop_summary.json',AUDIT/'capacity_attempt_timeline.json',AUDIT/'remote_liveness.json',
  AUDIT/'node_boot_and_monitor_processes.json',ROUTE/'readiness.json',ROUTE/'local_readiness.json',
  ROUTE/'new_relay_readiness.json',ROUTE/'generation_protocol.json',ROUTE/'terminal.json',ROUTE/'generation_summary.json']
 summary={'kind':'POINT_IN_TIME_EXPERIMENT_STOP_AND_NODE_LIVENESS_AUDIT','timezone':'Asia/Shanghai',
  'checked_CST':boot['current_CST'],'experiment_currently_running':False,'experiment_workers':0,'live_monitor_panes':2,
  'node3107_SSH':True,'hostname_unchanged':live['remote']['expected_hostname_matches'],
  'node_container_pid1_start_CST':boot['container_pid1_start_CST'],
  'experiment_control_node_recycled_last_night_proven':False,'model_server_recycle_exact_time':None,
  'platform_reclamation_events_available':False,'concurrency_caused_server_reclamation_proven':False,
  'last_capacity_batch':stop,'last_capacity_batch_is_TRAIN_not_formal_T1':True,
  'old_capacity_batch_not_resumed':True,'CypherBench_core_original_results_preserved':True,
  'current_OSDB_model_paths_planned':48,'current_OSDB_model_paths_dispatched':0,
  'route_check':{'old_own_forward_refused':old['readiness'][0].get('error_type')=='ConnectionRefusedError',
   'node_direct_old_public_gateway_timeout':old['readiness'][1].get('error_type')=='TimeoutError',
   'local_gateway_models_GET200':True,'new_own_reverse_forward_remote18894_models_GET200':True,
   'directory_readiness_is_not_generation_readiness':True,'other_services_modified':False},
  'new_single_generation':{'slot':6665,'purpose':'Original CypherBench developer resource compatibility, not OSDB scope',
   'http_status':None,'client_seconds':generation['client_seconds'],'accepted':False,'usage_unknown':True,
   'client_terminal_recorded':True,'server_outcome_unknown':True,'not_resent':True,
   'audit_finding':'Fresh check script retained10s read timeout after connect; this new check does not prove server failure.',
   'followup_240s_TRAIN_check_dispatched':False},
  'OSDB96_human_approved':True,'combined_cap':12096,'actual_reserved':6665,
  'combined_remaining':5431,'original_CypherBench_remaining':5335,'OSDB_remaining':96,'budget_pending_slots':[],
  'new_model_requests_this_turn':1,'new_model_requests_after_user_liveness_question':0,
  'formal_admission':False,'entire_T1_complete':False,'source_sha256':{str(p):sha(p) for p in files}}
 for p in (AUDIT/'summary.json',ROOT/'reports/t1_server_liveness_audit_20261027.json',Path(__file__).parent/'t1_server_liveness_audit_20261027.json'):immutable(p,summary)
 print(json.dumps({k:summary[k] for k in ('experiment_currently_running','checked_CST','actual_reserved','OSDB96_human_approved')},ensure_ascii=True))


if __name__=='__main__':main()
