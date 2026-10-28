"""Read-only query layer with immutable run-header reuse and explicit demand APIs.

``full_query`` preserves RealStore's full JSON result, ANN fallbacks and paging.
Only WHRun's ON-CREATE metadata/config are cached. A cache version comprises
run_id, created_at and every real-source identity marker; mutable watermarks,
robot/task/custody/event rows and ANN results always come from Neo4j. Writers
must never edit ON-CREATE fields in place; a replacement run gets created_at.
No schema bootstrap, graph mutation, oracle lookup or hidden result cache.
"""
from collections import OrderedDict
import json
import math
import os
import threading
import time

from ..knowledge_graph import Neo4jMirror
from ..real_scale.store import (RealStore, build_query, identity, integer,
    _payload, DIMENSIONS, VECTOR_INDEX, EXPANDED_QUERY, EXACT_QUERY)
from ..wide.queries import COOPERATION_KINDS


IDENTITY_FIELDS = ('real_scale_version', 'real_source_kind', 'real_source_run',
                   'real_dimensions', 'real_vector_kind')
_VERSION_CONDITION = ' AND '.join(
    ['toString(r.created_at)=$known_created_at'] +
    [f'r.{field}=$known_{field}' for field in IDENTITY_FIELDS])


def build_full_query(mode='ordered'):
    """Same full query, with a validated immutable-header omission on the wire."""
    query = build_query(mode)
    query = query.replace('r.metadata_json AS metadata_json,r.config_json AS config_json,',
        f'CASE WHEN {_VERSION_CONDITION} THEN null ELSE r.metadata_json END AS metadata_json,'
        f'CASE WHEN {_VERSION_CONDITION} THEN null ELSE r.config_json END AS config_json,'
        'toString(r.created_at) AS run_created_at,')
    return query


ROBOT_CONTEXT_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (step:WHStep {key:$step_key})
    WITH r.committed_seq AS head,step.checksum AS batch_checksum
    UNWIND $robots AS robot
    MATCH (p:WHAgentState {key:$run+':'+toString($seq)+':'+toString(robot)})
    WHERE p:WHRealState AND p.run_id=$run AND p.seq=$seq AND p.robot_id=robot
    CALL (p) {
        OPTIONAL MATCH (ts:WHTaskState {run_id:$run,task_id:p.task_id})
        USING INDEX ts:WHTaskState(run_id,task_id,seq) WHERE ts.seq<=$seq
        RETURN ts.payload AS task_payload
        ORDER BY ts.run_id DESC,ts.task_id DESC,ts.seq DESC LIMIT 1 }
    RETURN head,batch_checksum,robot AS robot_id,p.payload AS robot_payload,
        p.real_payload_sha256 AS robot_sha256,task_payload ORDER BY robot_id'''


COOPERATION_CONTEXT_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (step:WHStep {key:$step_key})
    CALL () {
        OPTIONAL MATCH (ft:WHTaskState {run_id:$run,task_id:$focus_task})
        USING INDEX ft:WHTaskState(run_id,task_id,seq) WHERE ft.seq<=$seq
        RETURN ft.payload AS focus_task_payload
        ORDER BY ft.run_id DESC,ft.task_id DESC,ft.seq DESC LIMIT 1 }
    CALL () {
        OPTIONAL MATCH (g:WHGoodsState {run_id:$run,goods_id:$goods})
        USING INDEX g:WHGoodsState(run_id,goods_id,event_seq)
        WHERE g.event_seq>=0 AND g.seq<=$seq
        RETURN g.location_id AS goods_location,g.event_seq AS goods_event_seq,
            g.sim_time AS goods_sim_time
        ORDER BY g.run_id DESC,g.goods_id DESC,g.event_seq DESC LIMIT 1 }
    CALL () {
        MATCH (g:WHGoodsState {run_id:$run,goods_id:$goods})
        USING INDEX g:WHGoodsState(run_id,goods_id,event_seq)
        WHERE g.event_seq>$goods_after AND g.seq<=$seq
        WITH g ORDER BY g.run_id,g.goods_id,g.event_seq LIMIT $goods_limit
        RETURN collect(g{.event_seq,.seq,.sim_time,.kind,.location_id,.event_key}) AS goods_rows }
    CALL () {
        MATCH (e:WHEvent {run_id:$run,task_id:$focus_task})
        USING INDEX e:WHEvent(run_id,task_id,event_seq)
        WHERE e.event_seq>$coop_after AND e.commit_seq<=$seq AND e.kind IN $kinds
        WITH e ORDER BY e.run_id,e.task_id,e.event_seq LIMIT $coop_limit
        RETURN collect(e{event_seq:e.event_seq,seq:e.commit_seq,payload:e.payload}) AS coop_rows }
    RETURN r.committed_seq AS head,step.checksum AS batch_checksum,focus_task_payload,
        goods_location,goods_event_seq,goods_sim_time,goods_rows,coop_rows'''


def _paging_parameters(goods_limit, cooperation_limit, goods_after, cooperation_after):
    integer(goods_limit, 'goods limit', 1, 1000)
    integer(cooperation_limit, 'cooperation limit', 1, 1000)
    integer(goods_after, 'goods cursor', 0)
    integer(cooperation_after, 'cooperation cursor', 0)
    return {'goods_after': goods_after, 'coop_after': cooperation_after,
            'goods_limit': goods_limit+1, 'coop_limit': cooperation_limit+1,
            'kinds': COOPERATION_KINDS}


def _histories(row, run, seq, goods_id, task_id, goods_limit, cooperation_limit):
    goods, coop = row['goods_rows'], row['coop_rows']
    goods_more, coop_more = len(goods)>goods_limit, len(coop)>cooperation_limit
    goods, coop = goods[:goods_limit], coop[:cooperation_limit]
    current = None if row['goods_event_seq'] is None else {
        'location_id': row['goods_location'], 'event_seq': row['goods_event_seq'],
        'sim_time': row['goods_sim_time']}
    return {
        'goods_history': {'run_id': run, 'goods_id': goods_id, 'seq': seq,
            'graph_committed_seq': row['head'], 'current_as_of': current, 'rows': goods,
            'has_more': goods_more, 'next_after_event_seq': goods[-1]['event_seq'] if goods_more else None},
        'cooperation': {'run_id': run, 'task_id': task_id, 'seq': seq,
            'graph_committed_seq': row['head'], 'rows': [
                {'event_seq': c['event_seq'], 'seq': c['seq'], 'event': _payload(c['payload'])}
                for c in coop], 'has_more': coop_more,
            'next_after_event_seq': coop[-1]['event_seq'] if coop_more else None}}


class QueryService(RealStore):
    def __init__(self, mirror, timeout=15, *, header_cache_size=32):
        super().__init__(mirror, timeout)
        integer(header_cache_size, 'header cache size', 0, 256)
        self.header_cache_size = header_cache_size
        self._headers, self._headers_lock = OrderedDict(), threading.Lock()
        self._local = threading.local()

    @classmethod
    def from_env(cls, timeout=15, *, pool_size=120, header_cache_size=32):
        integer(pool_size, 'connection pool size', 1, 1000)
        # Driver setup is independent of the application's schema-writing path.
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
            auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD']),
            max_connection_pool_size=pool_size, connection_timeout=10,
            connection_acquisition_timeout=30, max_transaction_retry_time=0)
        try:
            driver.verify_connectivity()
            return cls(Neo4jMirror(driver, os.environ.get('NEO4J_DATABASE', 'neo4j')),
                       timeout, header_cache_size=header_cache_size)
        except BaseException:
            driver.close()
            raise

    @property
    def latest_metrics(self):
        return dict(getattr(self._local, 'metrics', {}))

    def _begin(self):
        self._local.metrics = {'query_bolt_ms': 0., 'bolt_statements': 0,
            'metadata_cache_hit': False, 'metadata_wire_chars': 0,
            'decode_assemble_ms': 0., 'json_roundtrip_ms': 0.}

    def _read(self, query, params):
        started = time.perf_counter()
        try:
            return self.mirror.query(query, params, timeout=self.timeout)
        finally:
            self._local.metrics['query_bolt_ms'] += (time.perf_counter()-started)*1000
            self._local.metrics['bolt_statements'] += 1

    def _finish(self, result, assembly_started):
        self._local.metrics['decode_assemble_ms'] = (time.perf_counter()-assembly_started)*1000
        started = time.perf_counter()
        # Keep the baseline's complete finite JSON roundtrip. Returned mutable
        # dictionaries therefore cannot mutate cached parsed metadata/config.
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False)
        output = json.loads(encoded)
        self._local.metrics.update(json_roundtrip_ms=(time.perf_counter()-started)*1000,
            returned_json_chars=len(encoded))
        return output

    def clear_header_cache(self):
        with self._headers_lock:
            self._headers.clear()

    def _known(self, run):
        with self._headers_lock:
            entry = self._headers.get(run)
            if entry is not None:
                self._headers.move_to_end(run)
            return entry

    def _header(self, run, row, known):
        marker = {field: row[field] for field in IDENTITY_FIELDS}
        created_at = row.get('run_created_at')
        version = (run, created_at, *(marker[field] for field in IDENTITY_FIELDS))
        raw_metadata, raw_config = row['metadata_json'], row['config_json']
        self._local.metrics['metadata_wire_chars'] = sum(
            len(value) for value in (raw_metadata, raw_config) if isinstance(value, str))
        if raw_metadata is None and raw_config is None:
            if not created_at or known is None or known['version'] != version:
                raise ValueError('header omitted without exact cached source version')
            self._local.metrics['metadata_cache_hit'] = True
            return known['metadata'], known['config'], marker
        if raw_metadata is None or raw_config is None:
            raise ValueError('incomplete immutable source header')
        metadata, config = _payload(raw_metadata), _payload(raw_config)
        if not isinstance(metadata, dict) or not isinstance(config, dict):
            raise ValueError('invalid immutable source header')
        if self.header_cache_size and isinstance(created_at, str) and created_at:
            entry = {'version': version, 'metadata': metadata, 'config': config,
                'created_at': created_at, 'marker': marker}
            with self._headers_lock:
                self._headers[run] = entry
                self._headers.move_to_end(run)
                while len(self._headers)>self.header_cache_size:
                    self._headers.popitem(last=False)
        return metadata, config, marker

    def full_query(self, run, seq, robot_id, goods_id, focus_task_id, *, mode='ordered',
                   goods_limit=200, cooperation_limit=100, goods_after=0, cooperation_after=0):
        identity(run); identity(goods_id, 'goods'); identity(focus_task_id, 'focus task')
        integer(seq, 'seq'); integer(robot_id, 'robot', 1, 100)
        query = build_full_query(mode)
        paging = _paging_parameters(goods_limit, cooperation_limit, goods_after, cooperation_after)
        self._begin()
        known = self._known(run)
        params = {'run': run, 'seq': seq, 'robot': robot_id,
            'step_key': f'{run}:{seq}', 'state_key': f'{run}:{seq}:{robot_id}',
            'robot_key': f'{run}:{robot_id}', 'goods': goods_id, 'cargo_key': f'{run}:{goods_id}',
            'focus_task': focus_task_id, 'index': VECTOR_INDEX, 'ann_count': 100,
            'known_created_at': known['created_at'] if known else None,
            **{'known_'+field: known['marker'][field] if known else None for field in IDENTITY_FIELDS},
            **paging}
        rows = self._read(query, params)
        if len(rows) != 1:
            raise ValueError('requested same-run committed point absent')
        row = rows[0]
        vector = row['query_vector']
        if not isinstance(vector, (list, tuple)) or len(vector)!=DIMENSIONS or any(
                isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in vector):
            raise ValueError('invalid source query vector')
        similar, statements, retrieval = row['similar'], 1, 'ann100'
        if len(similar)<5:
            similar = self._read(EXPANDED_QUERY,
                {'run': run, 'seq': seq, 'vector': vector, 'index': VECTOR_INDEX})
            statements, retrieval = 2, 'ann1000_after_underfilled100'
        if len(similar)<5:
            similar = self._read(EXACT_QUERY, {'run': run, 'seq': seq, 'vector': vector})
            statements, retrieval = 3, 'exact_after_underfilled1000'
        if len(similar)!=5:
            raise ValueError('eligible real vector result remains incomplete')
        assembly = time.perf_counter()
        metadata, config, marker = self._header(run, row, known)
        result = {'run_id': run, 'seq': seq, 'robot_id': robot_id,
            'graph_committed_seq': row['head'], 'config': config, 'metadata': metadata,
            'real_identity': marker, 'batch_checksum': row['batch_checksum'],
            'robot': _payload(row['robot_payload']), 'robot_sha256': row['robot_sha256'],
            'task': _payload(row['robot_task_payload']), 'focus_task_id': focus_task_id,
            'focus_task': _payload(row['focus_task_payload']), 'history': row['history'],
            'query_vector': vector, 'similar': similar,
            **_histories(row, run, seq, goods_id, focus_task_id, goods_limit, cooperation_limit),
            'query_mode': mode, 'retrieval_path': retrieval, 'bolt_statements': statements}
        return self._finish(result, assembly)

    def robot_context(self, run, seq, robot_ids):
        """Explicit lean API: fixed-cutoff robot/task rows, sorted by robot ID.

        This intentionally does not claim the full-query ANN/history contract.
        All requested robots must be present; no silent partial response.
        """
        identity(run); integer(seq, 'seq')
        if not isinstance(robot_ids, (list, tuple)) or not 1<=len(robot_ids)<=100:
            raise ValueError('one to 100 distinct robot IDs required')
        for robot in robot_ids:
            integer(robot, 'robot', 1, 100)
        if len(set(robot_ids)) != len(robot_ids):
            raise ValueError('duplicate robot IDs')
        self._begin()
        rows = self._read(ROBOT_CONTEXT_QUERY, {'run': run, 'seq': seq,
            'step_key': f'{run}:{seq}', 'robots': list(robot_ids)})
        if len(rows)!=len(robot_ids) or {row['robot_id'] for row in rows}!=set(robot_ids):
            raise ValueError('requested same-run robot points absent or duplicated')
        if len({(row['head'], row['batch_checksum']) for row in rows})!=1:
            raise ValueError('robot context watermarks disagree')
        assembly = time.perf_counter()
        return self._finish({'run_id': run, 'seq': seq,
            'graph_committed_seq': rows[0]['head'], 'batch_checksum': rows[0]['batch_checksum'],
            'robots': [{'robot_id': row['robot_id'], 'robot': _payload(row['robot_payload']),
                'robot_sha256': row['robot_sha256'], 'task': _payload(row['task_payload'])}
                for row in sorted(rows, key=lambda item: item['robot_id'])]}, assembly)

    def cooperation_context(self, run, seq, task_id, goods_id, *, goods_limit=200,
                            cooperation_limit=100, goods_after=0, cooperation_after=0):
        """Explicit lean API: current task plus its custody/cooperation history."""
        identity(run); integer(seq, 'seq'); identity(task_id, 'task'); identity(goods_id, 'goods')
        paging = _paging_parameters(goods_limit, cooperation_limit, goods_after, cooperation_after)
        self._begin()
        rows = self._read(COOPERATION_CONTEXT_QUERY, {'run': run, 'seq': seq,
            'step_key': f'{run}:{seq}', 'focus_task': task_id, 'goods': goods_id, **paging})
        if len(rows)!=1:
            raise ValueError('requested same-run committed cooperation point absent')
        assembly = time.perf_counter()
        row, task = rows[0], _payload(rows[0]['focus_task_payload'])
        if not isinstance(task, dict) or task.get('task_id')!=task_id or task.get('goods_id')!=goods_id:
            raise ValueError('unknown or mismatched task/goods identity')
        return self._finish({'run_id': run, 'seq': seq, 'graph_committed_seq': row['head'],
            'batch_checksum': row['batch_checksum'], 'task_id': task_id, 'goods_id': goods_id,
            'focus_task': task,
            # A terminal page is not proof that preceding pages were included.
            # Shadow decisions can require both cursors == 0 rather than mistake
            # an after-cursor tail with has_more=False for a complete history.
            'request_cursors': {'goods_after': goods_after, 'cooperation_after': cooperation_after},
            **_histories(row, run, seq, goods_id, task_id, goods_limit, cooperation_limit)}, assembly)
