"""Evaluator-only sealed archived-reference store. Never a model input source.

No database/model/network/config imports. Completeness comes from the sealed
historical terminal/acceptance chain, not HTTP200 alone. Hashes establish local
byte identity, not semantic correctness or author provenance.
"""
from copy import deepcopy
from datetime import datetime
import hashlib,json
from pathlib import Path

from tugraph_results import parse_rest_result


class ReferenceIdentityError(ValueError):
    pass


def sha_bytes(raw):return hashlib.sha256(raw).hexdigest()


def beneath(root,relative):
    if type(relative) is not str or Path(relative).is_absolute():
        raise ReferenceIdentityError('A relative cache path is required')
    p=(root/relative).resolve()
    if root not in p.parents:
        raise ReferenceIdentityError('Cache path escapes its sealed root')
    return p


def checked_bytes(path,digest):
    try:raw=path.read_bytes()
    except OSError as exc:raise ReferenceIdentityError('Archived reference file unavailable') from exc
    if sha_bytes(raw)!=digest:raise ReferenceIdentityError('Archived reference byte hash differs')
    return raw


class SealedReferenceStore:
    def __init__(self,seal_path,*,expected_seal_sha256):
        self.seal_path=Path(seal_path).resolve()
        raw=checked_bytes(self.seal_path,expected_seal_sha256)
        self.seal=json.loads(raw)
        if self.seal.get('kind')!='EVALUATOR_ONLY_ARCHIVED_REFERENCE_INDEX_NOT_FORMAL_FREEZE':
            raise ReferenceIdentityError('Unexpected reference seal kind')
        self.root=Path(self.seal['cache_root']).resolve()
        index=beneath(self.seal_path.parent,self.seal['index_file'])
        data=json.loads(checked_bytes(index,self.seal['index_sha256']))
        self._entries={}
        for r in data:
            identity=(r['graph'],r['id'])
            if any(type(v) is not str for v in identity) or identity in self._entries:
                raise ReferenceIdentityError('Invalid or duplicate graph/task identity')
            if type(r['reference_known']) is not bool:
                raise ReferenceIdentityError('Unfixed reference cohort')
            beneath(self.root,r['record_file']);beneath(self.root,r['response_file'])
            self._entries[identity]=r
        if len(self._entries)!=self.seal['positions']:
            raise ReferenceIdentityError('Incomplete reference population')
        if sum(r['reference_known'] for r in data)!=self.seal['known_reference_positions']:
            raise ReferenceIdentityError('Reference cohort count differs')

    def cohort(self):
        # No reference values, query text, source labels or answers are emitted.
        return [(k,{'reference_known':r['reference_known'],
                    'reference_contract_concern':r.get('reference_contract_concern',False),
                    'order_rule':'UNADJUDICATED'}) for k,r in self._entries.items()]

    def identities(self):
        return tuple(self._entries)

    def metadata(self,graph,task_id):
        if (graph,task_id) not in self._entries:raise ReferenceIdentityError('Foreign graph/task identity')
        return deepcopy(self._entries[(graph,task_id)])

    def load(self,graph,task_id,*,expected_query_sha256):
        identity=(graph,task_id)
        if identity not in self._entries:raise ReferenceIdentityError('Foreign graph/task identity')
        r=self._entries[identity]
        if r['query_sha256']!=expected_query_sha256:
            raise ReferenceIdentityError('Requested query differs from pinned task query')
        record_path=beneath(self.root,r['record_file']);response_path=beneath(self.root,r['response_file'])
        record=json.loads(checked_bytes(record_path,r['record_sha256']))
        raw=checked_bytes(response_path,r['response_sha256'])
        for name,value in [('id',task_id),('index',r['ordinal']),('query_sha256',expected_query_sha256),
                           ('response_sha256',r['response_sha256']),('status',r['reference_status'])]:
            if record.get(name)!=value:raise ReferenceIdentityError('Reference terminal identity/status differs')
        try:
            start=datetime.fromisoformat(record['request_started']);end=datetime.fromisoformat(record['request_finished'])
            if start.tzinfo is None or end.tzinfo is None or end<start:
                raise ValueError('Invalid archived terminal clock')
        except (KeyError,ValueError,TypeError) as exc:
            raise ReferenceIdentityError('No valid archived terminal interval') from exc
        result={'task_key':[graph,task_id],'reference_known':r['reference_known'],
                'reference_status':r['reference_status'],'query_sha256':expected_query_sha256,
                'response_sha256':r['response_sha256'],'answer':None,
                'author_Bolt_REST_equivalence_established':False,'semantic_gold_certified':False}
        if not r['reference_known']:
            if record.get('http_status')==200 or record['status']!='GOLD_EXECUTION_ERROR':
                raise ReferenceIdentityError('Unknown-reference terminal does not match fixed error cohort')
            body=json.loads(raw)
            if type(body) is not dict or not isinstance(body.get('error_message'),str):
                raise ReferenceIdentityError('Missing archived reference execution error')
            return result
        if record.get('http_status')!=200 or not record['status'].startswith('EXECUTED_'):
            raise ReferenceIdentityError('Known-reference terminal is not a successful complete execution')
        try:answer=parse_rest_result(raw,transport_complete=True,execution_complete=True)
        except Exception as exc:
            raise ReferenceIdentityError('Known-reference response is incomplete or malformed') from exc
        if record.get('size')!=len(answer['rows']):
            raise ReferenceIdentityError('Archived terminal result size differs')
        if (record['status']=='EXECUTED_EMPTY') != (len(answer['rows'])==0):
            raise ReferenceIdentityError('Archived empty/nonempty status differs')
        result['answer']=answer
        return result
