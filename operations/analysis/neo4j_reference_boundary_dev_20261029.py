"""Post-seal reference reads and explicitly auxiliary typed comparison.

This is a preparation boundary, not either benchmark's authenticated scorer.
There is no sender or graph startup. It enforces call order in this consumer,
not operating-system isolation from a malicious method or caller.
"""

from dataclasses import asdict,dataclass
import hashlib
from pathlib import Path
import re

from neo4j.exceptions import DriverError,Neo4jError

from audited_model_reply_dev_20261029 import PinnedFile,strict_json
from neo4j_native_answer_contract_dev_20261029 import compare_complete_auxiliary,encoded
from neo4j_read_capture_dev_20261029 import NativeReadCapture,CaptureStateError
from neo4j_reply_consumer_dev_20261029 import save_new
from public_read_guard import ReadOnlyViolation


class ReferenceConflict(ValueError):
    pass


class PinnedReferences:
    """Load the whole reference population lazily, after a method seal check.

    task_ids come from the verified public loader. No reference, source label,
    return_count or template is inspected by this constructor or returned as
    a model-facing object. Queries are not executed by this loader.
    """

    def __init__(self, package, manifest_sha256, task_ids):
        self.package,self.manifest_sha256=Path(package),manifest_sha256
        self.task_ids=tuple(task_ids)
        if (not self.task_ids or any(type(v) is not str or not v for v in self.task_ids)
                or len(set(self.task_ids))!=len(self.task_ids)):
            raise ReferenceConflict('Exact unique public task population required')
        self._ids=frozenset(self.task_ids)
        self._records,self._reference_sha256=None,None

    def get(self, task_id):
        if task_id not in self._ids:raise ReferenceConflict('Undeclared reference task')
        if self._records is None:
            manifest=PinnedFile(str(self.package/'manifest.json'),self.manifest_sha256).load()
            if (type(manifest['positions_per_variant']) is not int
                    or manifest['positions_per_variant']!=len(self.task_ids)):
                raise ReferenceConflict('Reference/public denominator differs')
            path=self.package/'evaluation/references.jsonl'
            raw=path.read_bytes()
            expected=manifest['files']['evaluation/references.jsonl']
            if hashlib.sha256(raw).hexdigest()!=expected:
                raise ReferenceConflict('Reference bytes differ from the pinned package')
            records={}
            for line in raw.splitlines():
                row=strict_json(line)
                if type(row) is not dict or row.get('task_id') not in self._ids or row['task_id'] in records:
                    raise ReferenceConflict('Extra, duplicate or malformed reference position')
                # Validation labels and return counts are deliberately ignored.
                records[row['task_id']]=row.get('reference_query')
            if set(records)!=self._ids:
                raise ReferenceConflict('Reference population is incomplete; never shrink N')
            self._records,self._reference_sha256=records,expected
        return dict(task_id=task_id,query=self._records[task_id],
                    reference_file_sha256=self._reference_sha256)


@dataclass(frozen=True)
class AuxiliaryTaskPolicy:
    benchmark: str
    task_id: str
    graph: str
    snapshot_sha256: str
    database: str
    ordered: bool

    def __post_init__(self):
        if (self.benchmark not in ('mindthequery','zograscope')
                or any(type(v) is not str or not v for v in
                       (self.task_id,self.graph,self.snapshot_sha256,self.database))
                or not re.fullmatch('[0-9a-f]{64}',self.snapshot_sha256) or type(self.ordered) is not bool):
            raise ReferenceConflict('Method-independent task, snapshot and explicit ordering policy required')


class ReferenceEvaluator:
    """One reference query, in the method's still-live explicit read transaction.

    Hash/identity/scope checks precede even loading the reference file. Ordering
    comes from a predeclared auxiliary task policy, never the prediction text.
    No result is certified as the author primary score. Failure is not an empty
    reference answer, and no reference error can request model feedback.
    """

    def __init__(self, references, policy):
        if not isinstance(references,PinnedReferences) or not isinstance(policy,AuxiliaryTaskPolicy):
            raise ReferenceConflict('Pinned references and explicit auxiliary policy required')
        self.references,self.policy,self.used=references,policy,False

    def __call__(self, capture, method_path, method_sha256, identity):
        if self.used:raise ReferenceConflict('Reference evaluation cannot automatically repeat')
        self.used=True
        if not isinstance(capture,NativeReadCapture) or capture.state!='open':
            raise CaptureStateError('Reference requires the method live capture')
        policy=self.policy
        if any(getattr(identity,k)!=getattr(policy,k) for k in ('benchmark','task_id','graph')):
            raise ReferenceConflict('Reference task differs from method identity')
        if capture.scope.database!=policy.database or capture.scope.snapshot_sha256!=policy.snapshot_sha256:
            raise ReferenceConflict('Reference graph snapshot/database differs')
        method=PinnedFile(str(method_path),method_sha256).load()
        if method.get('source',{}).get('identity')!=asdict(identity):
            raise ReferenceConflict('Sealed method belongs to a different request')
        answer=method['result']['answer']
        if (answer.get('status')!='EXECUTED' or answer.get('complete') is not True
                or answer.get('scope')!=asdict(capture.scope)):
            raise ReferenceConflict('Reference pairing needs the actual complete same-capture method answer')
        # First reference access occurs here, after all method seal checks.
        record=self.references.get(identity.task_id)
        folder=Path(method_path).parent/'evaluation';folder.mkdir(exist_ok=False)
        query=record['query']
        reference,status,error=None,'UNRESOLVED_REFERENCE',None
        if type(query) is str and query.strip():
            try:
                reference=capture.read(query,{},purpose='evaluator')
                status='EXECUTED'
            except (ReadOnlyViolation,Neo4jError,DriverError) as exc:
                status,error='REFERENCE_EXECUTION_ERROR',type(exc).__name__
        # The method answer must remain unchanged through evaluation.
        PinnedFile(str(method_path),method_sha256).load()
        exact=compare_complete_auxiliary(reference,answer,ordered=policy.ordered) if reference is not None else None
        private=dict(kind='REFERENCE_ONLY_NOT_MODEL_OR_FEEDBACK_INPUT',source=record,
            reference_answer=reference,reference_status=status,error_type=error,
            method_file_sha256=method_sha256,policy=asdict(policy),auxiliary_exact=exact,
            primary_score=None,author_comparator_authenticated=False,new_model_calls=0,monetary_cost=None)
        path=folder/'reference.json';save_new(path,private)
        return dict(reference_status=status,auxiliary_exact=exact,primary_score=None,
            prediction_status='EXECUTED',comparison_profile='strict-typed-auxiliary-v1',
            policy_sha256=hashlib.sha256(encoded(asdict(policy))).hexdigest(),
            evidence_file=str(path),evidence_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            author_comparator_authenticated=False,new_model_calls=0,monetary_cost=None)


def summarize_auxiliary(task_ids, observations):
    """Retain the declared denominator and expose missing/unknown observations.

    Bounds concern only this auxiliary comparison, never a published primary
    EX. Missing observations are unresolved, not discarded completed tasks.
    A recorded refusal/failure cannot count as a correct answer.
    """
    task_ids=tuple(task_ids)
    if (not task_ids or any(type(v) is not str or not v for v in task_ids)
            or len(set(task_ids))!=len(task_ids)):
        raise ReferenceConflict('Fixed unique nonempty denominator required')
    allowed=frozenset(task_ids)
    seen={}
    failures={'REFUSED','MODEL_TRUNCATED','EMPTY','UNSUPPORTED','PROTOCOL_ERROR','GUARD_REJECTED',
              'EXECUTION_ERROR','INFRASTRUCTURE_ERROR','UNKNOWN','UNDISPATCHED','INCOMPLETE_ANSWER'}
    for row in observations:
        task_id=row['task_id']
        if task_id not in allowed or task_id in seen:
            raise ReferenceConflict('Extra/duplicate observation cannot alter the denominator')
        exact=row['auxiliary_exact']
        if exact is not None and type(exact) is not bool:
            raise ReferenceConflict('Explicit true/false/unknown auxiliary result required')
        if row['prediction_status'] in failures:exact=False
        elif row['prediction_status']!='EXECUTED':
            raise ReferenceConflict('Unrecognized prediction status is not an incorrect answer')
        seen[task_id]=exact
    correct=sum(value is True for value in seen.values())
    incorrect=sum(value is False for value in seen.values())
    unknown=len(task_ids)-correct-incorrect
    return dict(N=len(task_ids),correct=correct,incorrect=incorrect,unknown=unknown,
        missing=len(task_ids)-len(seen),coverage_complete=len(seen)==len(task_ids),
        auxiliary_lower_bound=correct/len(task_ids),auxiliary_upper_bound=(correct+unknown)/len(task_ids),
        primary_score=None,author_comparator_authenticated=False,formal_admission=False)
