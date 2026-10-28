"""One explicit Neo4j read transaction for native identity-bearing answers.

Preparation component, not a registered benchmark adapter. The caller supplies
an owned immutable graph and a synchronous durable audit sink. READ routing is
not an authorization boundary: lexical checks and EXPLAIN are also required.
No managed-transaction retry, graph startup, model client, or scorer is used.
"""

from copy import deepcopy
import hashlib
import math
import re
import threading
import time
import uuid

import neo4j

from neo4j_native_answer_contract_dev_20261029 import (
    CaptureScope, UnsupportedNativeValue, encoded, freeze_answer,
)
from public_read_guard import read_query


class CaptureStateError(RuntimeError):
    pass


def native_identity_projection(variable, alias, bound_entities):
    """Lower an entity-key projection without fabricating a schema property.

    Bindings must come from validated MATCH declarations. Return the actual
    node/relationship, so its type, identity and payload survive serialization;
    an elementId string alone would lose its identity namespace/provenance.
    The full selection compiler and grouping integration are separate work.
    """
    for value in (variable, alias):
        if type(value) is not str or not re.fullmatch('[A-Za-z][A-Za-z0-9_]{0,31}', value):
            raise ValueError('Expected a bounded compiler variable/alias')
    if type(bound_entities) is not dict or bound_entities.get(variable) not in ('node', 'relationship'):
        raise ValueError('Identity projection needs a validated native entity binding')
    return {'expression': variable, 'projection': variable + ' AS `' + alias + '`',
            'entity_kind': bound_entities[variable], 'identity': 'native_value.element_id',
            'identity_scope': 'one_explicit_read_transaction',
            'cross_transaction_identity_certified': False}


class NativeReadCapture:
    """Own exactly one session and one transaction; use from one thread only.

    emit(event) must durably save the whole event before returning. Failures
    propagate and poison this capture. The answer event includes the actual
    typed answer, not just a digest. Snapshot immutability/ownership must be
    established by a future graph lifecycle adapter, not by this metadata.

    transaction_timeout bounds the ENTIRE explicit transaction. Neo4j5.28.1
    Transaction.run rejects Query(timeout=...). This does not replace the old
    per-query EXPLAIN/read timeout contract or certify a new formal protocol.
    """

    def __init__(self, driver, *, snapshot_sha256, database, transaction_timeout,
                 max_rows, max_answer_bytes, emit):
        if neo4j.__version__ != '5.28.1':
            raise ValueError('Pinned Neo4j5.28.1 driver required')
        self.scope = CaptureScope(snapshot_sha256, database, uuid.uuid4().hex)
        if type(transaction_timeout) not in (int, float) or not math.isfinite(transaction_timeout) or transaction_timeout <= 0:
            raise ValueError('Explicit finite positive whole-transaction timeout required')
        if any(type(value) is not int or value <= 0 for value in (max_rows, max_answer_bytes)):
            raise ValueError('Explicit positive row and answer evidence limits required')
        if not callable(emit):
            raise ValueError('Synchronous durable audit sink required')
        self.driver, self.emit = driver, emit
        self.timeout, self.max_rows, self.max_bytes = transaction_timeout, max_rows, max_answer_bytes
        self.session = self.transaction = None
        self.state, self.thread, self.sequence = 'new', None, 0

    def _event(self, kind, **fields):
        self.sequence += 1
        event = dict(kind=kind, capture_id=self.scope.capture_id, sequence=self.sequence,
                     database=self.scope.database, snapshot_sha256=self.scope.snapshot_sha256,
                     epoch=time.time(), **fields)
        self.emit(deepcopy(event))

    def __enter__(self):
        if self.state != 'new':
            raise CaptureStateError('A capture cannot be reused or reopened')
        self.thread = threading.get_ident()
        self.state = 'opening'
        try:
            self._event('capture_open_intent', transaction_timeout=self.timeout)
            self.session = self.driver.session(database=self.scope.database, default_access_mode='READ')
            self.transaction = self.session.begin_transaction(
                timeout=self.timeout, metadata={'native_capture_id': self.scope.capture_id})
            self._event('capture_opened')
            self.state = 'open'
            return self
        except BaseException as error:
            self.state = 'failed'
            try:
                self._close()
            finally:
                self._event('capture_open_failed', error_type=type(error).__name__)
            raise

    def _close(self):
        # Explicit close rolls back the read transaction; never commit a method
        # or evaluator query. Session close runs even if rollback itself fails.
        try:
            if self.transaction is not None:
                self.transaction.close()
        finally:
            if self.session is not None:
                self.session.close()

    def __exit__(self, exc_type, exc_value, traceback):
        failed = self.state == 'failed' or exc_type is not None
        self.state = 'closed'
        try:
            self._close()
        except BaseException as error:
            self._event('capture_close_failed', error_type=type(error).__name__)
            raise
        self._event('capture_closed', failed=failed)
        return False

    def _certify(self, summary):
        if (summary.query_type != 'r' or summary.counters.contains_updates is not False
                or summary.database != self.scope.database):
            raise CaptureStateError('Read summary/database did not certify the declared capture')

    def read(self, query, parameters, *, purpose):
        """Guard, EXPLAIN, fetch positionally, consume, then persist one answer.

        purpose labels costs only; it does not enforce gold separation. The
        adapter must seal the method answer before evaluator/reference reads.
        A row/byte overflow or any failed read invalidates the whole capture;
        no partial answer is returned as successful and no retry is performed.
        """
        if self.state != 'open' or threading.get_ident() != self.thread:
            raise CaptureStateError('Reads require the live owning transaction/thread')
        started = time.monotonic()
        try:
            if purpose not in ('method', 'control', 'evaluator'):
                raise ValueError('Explicit method/control/evaluator cost purpose required')
            query = read_query(query)
            if type(parameters) is not dict:
                raise ValueError('Expected a parameter object')
            parameters = deepcopy(parameters)
            frozen_parameters = freeze_answer(['parameters'], [[parameters]], self.scope,
                                              complete=True, max_bytes=self.max_bytes)
            hashes = dict(query_sha256=hashlib.sha256(query.encode('utf-8')).hexdigest(),
                          parameters_sha256=hashlib.sha256(encoded(frozen_parameters['rows'])).hexdigest())
            self._event('read_intent', purpose=purpose, **hashes)
            explain_started = time.monotonic()
            explained = self.transaction.run('EXPLAIN ' + query, parameters).consume()
            self._certify(explained)
            after_explain = time.monotonic()
            self._event('read_explained', purpose=purpose, **hashes)
            fetch_started = time.monotonic()
            cursor = self.transaction.run(query, parameters)
            columns, rows, encoded_row_bytes = list(cursor.keys()), [], 0
            for record in cursor:
                if len(rows) == self.max_rows:
                    raise UnsupportedNativeValue('Row limit exceeded; capture is incomplete')
                # Measure native typed bytes linearly while fetching. The final
                # freeze checks the exact envelope cap, without silent slicing.
                one = freeze_answer(columns, [record], self.scope, complete=False, max_bytes=self.max_bytes)
                encoded_row_bytes += len(encoded(one['rows'][0]))
                if encoded_row_bytes > self.max_bytes:
                    raise UnsupportedNativeValue('Answer evidence limit exceeded while fetching')
                rows.append(record)
            summary = cursor.consume()
            self._certify(summary)
            after_fetch = time.monotonic()
            answer = freeze_answer(columns, rows, self.scope, complete=True, max_bytes=self.max_bytes)
            after_encode = time.monotonic()
            self._event('answer_captured', purpose=purpose, answer=answer,
                        answer_sha256=hashlib.sha256(encoded(answer)).hexdigest(),
                        explain_roundtrip_seconds=after_explain-explain_started,
                        fetch_and_materialize_seconds=after_fetch-fetch_started,
                        final_answer_encode_seconds=after_encode-after_fetch,
                        pre_answer_persist_seconds=after_encode-started,
                        pure_server_execution_seconds=None,
                        server_available_after_ms=summary.result_available_after,
                        server_consumed_after_ms=summary.result_consumed_after, **hashes)
            return answer
        except BaseException as error:
            self.state = 'failed'
            self._event('read_failed', purpose=purpose, error_type=type(error).__name__,
                        local_total_seconds=time.monotonic()-started)
            raise
