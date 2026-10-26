"""Equal refusal/answer envelopes for OSDB development; old messages retained."""
from copy import deepcopy
import hashlib
from osdb_common_input import canonical
from osdb_common_input_v2 import common_input as previous,OURS_SYSTEM as PLAN_SYSTEM

VERSION='osdb-equal-answer-decline-envelope-dev-3'
ENVELOPE=('Return one JSON object only. Both methods use status ANSWER, UNSUPPORTED or '
 'INSUFFICIENT_EVIDENCE. A decline has only status and a nonempty reason. Declines are not '
 'correct numeric answers. Never invent missing history, people identity or employment facts. '
 'Use the common input only; a valid executed answer receives no gold-driven repair. ')
DIRECT_SYSTEM=ENVELOPE+('For ANSWER use {"status":"ANSWER","query":"one read-only Neo4j 5.15 Cypher"}. '
 'No parameters outside the query, explanations or extra fields. Native read patterns, aggregates, '
 'UNION and read subqueries remain available. No write/admin/procedure/database-switch commands. '
 'Use declared data and explicit absolute-instant conversions; no implicit LIMIT.')
OURS_SYSTEM=ENVELOPE+PLAN_SYSTEM
FEEDBACK_POLICY={'max_feedback_generations_per_arm':1,'uses_gold_or_answer_checks':False,
 'eligible':['MODEL_PROTOCOL_ERROR','PLAN_VALIDATION_ERROR','READ_POLICY_ERROR','EXECUTION_ERROR','EXECUTION_TIMEOUT','TRUNCATED_RESULT'],
 'ineligible':['EXECUTED','UNSUPPORTED','INSUFFICIENT_EVIDENCE','INFRASTRUCTURE_ERROR','MODEL_TRUNCATED'],
 'operational_error_max_characters':2000,'no_semantic_answer_change_or_implicit_limit':True}


def common_input(**kwargs):
 old=previous(**kwargs);payload=deepcopy(old['payload'])
 payload['response_contract']={'statuses':['ANSWER','UNSUPPORTED','INSUFFICIENT_EVIDENCE'],
  'decline_fields':['status','reason'],'declines_are_numeric_correct':False,
  'feedback':deepcopy(FEEDBACK_POLICY)}
 wire=canonical(payload)
 return {'payload':payload,'serialized':wire,'sha256':hashlib.sha256(wire.encode('utf-8')).hexdigest(),
  'version':VERSION,'formal_admission':False}


def messages(arm,**kwargs):
 if arm not in ('direct','ours'):raise ValueError('Unknown arm')
 item=common_input(**kwargs)
 return {'messages':[{'role':'system','content':DIRECT_SYSTEM if arm=='direct' else OURS_SYSTEM},
  {'role':'user','content':item['serialized']}],'common_input_sha256':item['sha256'],'formal_admission':False}
