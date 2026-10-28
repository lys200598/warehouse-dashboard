"""Read-only same-run vector/graph/temporal API for actual runtime histories.

No archive resampling, schema changes, omitted fields or cached responses.
Neo4j 5.26-compatible vector procedure and optional ordered range-index reads.
"""
import json
import math
import os
import re

from ..knowledge_graph import Neo4jMirror
from ..wide.queries import COOPERATION_KINDS


VECTOR_INDEX = 'whreal_state_vector_v1'
DIMENSIONS = 32
MODES = ('baseline', 'ordered')


def identity(value, name='identity'):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value):
        raise ValueError('invalid ' + name)
    return value


def integer(value, name, low=1, high=None):
    if isinstance(value, bool) or not isinstance(value, int) or value < low or high is not None and value > high:
        raise ValueError('invalid ' + name)
    return value


def _payload(value):
    return json.loads(value) if isinstance(value, str) else value


def build_query(mode):
    if mode not in MODES:
        raise ValueError('unknown query mode')
    if mode == 'ordered':
        history = '''MATCH (h:WHAgentState {run_id:$run,robot_id:$robot})
            USING INDEX h:WHAgentState(run_id,robot_id,seq)
            WHERE h:WHRealState AND h.seq<=$seq'''
        robot_task = '''OPTIONAL MATCH (ts:WHTaskState {run_id:$run,task_id:p.task_id})
            USING INDEX ts:WHTaskState(run_id,task_id,seq) WHERE ts.seq<=$seq'''
        focus_task = '''OPTIONAL MATCH (ft:WHTaskState {run_id:$run,task_id:$focus_task})
            USING INDEX ft:WHTaskState(run_id,task_id,seq) WHERE ft.seq<=$seq'''
        goods_latest = '''OPTIONAL MATCH (g:WHGoodsState {run_id:$run,goods_id:$goods})
            USING INDEX g:WHGoodsState(run_id,goods_id,event_seq)
            WHERE g.event_seq>=0 AND g.seq<=$seq'''
        goods_history = '''MATCH (g:WHGoodsState {run_id:$run,goods_id:$goods})
            USING INDEX g:WHGoodsState(run_id,goods_id,event_seq)
            WHERE g.event_seq>$goods_after AND g.seq<=$seq'''
        cooperation = '''MATCH (e:WHEvent {run_id:$run,task_id:$focus_task})
            USING INDEX e:WHEvent(run_id,task_id,event_seq)
            WHERE e.event_seq>$coop_after AND e.commit_seq<=$seq AND e.kind IN $kinds'''
    else:
        history = '''MATCH (agent:WHRobot {key:$robot_key})<-[:OF_ROBOT]-(h:WHAgentState)
            WHERE h:WHRealState AND h.run_id=$run AND h.robot_id=$robot AND h.seq<=$seq'''
        robot_task = '''OPTIONAL MATCH (task:WHTask {key:$run+':'+p.task_id})
            OPTIONAL MATCH (ts:WHTaskState)-[:OF_TASK]->(task) WHERE ts.run_id=$run AND ts.seq<=$seq'''
        focus_task = '''OPTIONAL MATCH (focus:WHTask {key:$run+':'+$focus_task})
            OPTIONAL MATCH (ft:WHTaskState)-[:OF_TASK]->(focus) WHERE ft.run_id=$run AND ft.seq<=$seq'''
        goods_latest = '''OPTIONAL MATCH (cargo:WHCargo {key:$cargo_key})
            OPTIONAL MATCH (g:WHGoodsState)-[:OF_CARGO]->(cargo) WHERE g.run_id=$run AND g.seq<=$seq'''
        goods_history = '''MATCH (cargo:WHCargo {key:$cargo_key})<-[:OF_CARGO]-(g:WHGoodsState)
            WHERE g.run_id=$run AND g.seq<=$seq AND g.event_seq>$goods_after'''
        cooperation = '''MATCH (focus:WHTask {key:$run+':'+$focus_task})<-[:ABOUT_TASK]-(e:WHEvent)
            WHERE e.run_id=$run AND e.commit_seq<=$seq AND e.event_seq>$coop_after AND e.kind IN $kinds'''
    query = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
        MATCH (step:WHStep {key:$step_key}), (p:WHAgentState {key:$state_key})
        WHERE p:WHRealState AND p.run_id=$run AND p.seq=$seq AND p.robot_id=$robot
        CALL (p) { ''' + robot_task + '''
            RETURN ts.payload AS robot_task_payload ORDER BY ts.seq DESC LIMIT 1 }
        CALL () { ''' + history + '''
            WITH h ORDER BY h.seq DESC LIMIT 5
            RETURN collect(h{.key,.seq,.robot_id,.payload,.real_payload_sha256}) AS history }
        CALL () { ''' + focus_task + '''
            RETURN ft.payload AS focus_task_payload ORDER BY ft.seq DESC LIMIT 1 }
        CALL () { ''' + goods_latest + '''
            RETURN g.location_id AS goods_location,g.event_seq AS goods_event_seq,g.sim_time AS goods_sim_time
            ORDER BY g.event_seq DESC LIMIT 1 }
        CALL () { ''' + goods_history + '''
            WITH g ORDER BY g.event_seq LIMIT $goods_limit
            RETURN collect(g{.event_seq,.seq,.sim_time,.kind,.location_id,.event_key}) AS goods_rows }
        CALL () { ''' + cooperation + '''
            WITH e ORDER BY e.event_seq LIMIT $coop_limit
            RETURN collect(e{event_seq:e.event_seq,seq:e.commit_seq,payload:e.payload}) AS coop_rows }
        CALL (p) {
            CALL db.index.vector.queryNodes($index,$ann_count,p.real_embedding) YIELD node,score
            WHERE node:WHRealState AND node.run_id=$run AND node.seq<=$seq
            WITH node,score ORDER BY score DESC LIMIT 5
            RETURN collect(node{.key,.run_id,.seq,.robot_id,.real_payload_sha256,
                real_embedding:node.real_embedding,score:score}) AS similar }
        RETURN r.committed_seq AS head,r.metadata_json AS metadata_json,r.config_json AS config_json,
            r.real_scale_version AS real_scale_version,r.real_source_kind AS real_source_kind,
            r.real_source_run AS real_source_run,r.real_dimensions AS real_dimensions,
            r.real_vector_kind AS real_vector_kind,step.checksum AS batch_checksum,
            p.payload AS robot_payload,p.real_payload_sha256 AS robot_sha256,
            p.real_embedding AS query_vector,robot_task_payload,focus_task_payload,history,
            goods_location,goods_event_seq,goods_sim_time,goods_rows,coop_rows,similar'''
    if mode == 'ordered':
        # Neo4j 5.26 needs the fixed equality-prefix properties explicitly in
        # ORDER BY to stream this composite index order instead of Top/Sort.
        # Each prefix is fixed by MATCH, so business ordering is unchanged.
        for short, complete in [
            ('ORDER BY ts.seq DESC', 'ORDER BY ts.run_id DESC,ts.task_id DESC,ts.seq DESC'),
            ('ORDER BY h.seq DESC', 'ORDER BY h.run_id DESC,h.robot_id DESC,h.seq DESC'),
            ('ORDER BY ft.seq DESC', 'ORDER BY ft.run_id DESC,ft.task_id DESC,ft.seq DESC'),
            ('ORDER BY g.event_seq DESC', 'ORDER BY g.run_id DESC,g.goods_id DESC,g.event_seq DESC'),
            ('ORDER BY g.event_seq LIMIT', 'ORDER BY g.run_id,g.goods_id,g.event_seq LIMIT'),
            ('ORDER BY e.event_seq LIMIT', 'ORDER BY e.run_id,e.task_id,e.event_seq LIMIT'),
        ]:
            query = query.replace(short, complete)
    return query


EXPANDED_QUERY = '''CALL db.index.vector.queryNodes($index,1000,$vector) YIELD node,score
    WHERE node:WHRealState AND node.run_id=$run AND node.seq<=$seq
    RETURN node.key AS key,node.run_id AS run_id,node.seq AS seq,node.robot_id AS robot_id,
        node.real_payload_sha256 AS real_payload_sha256,node.real_embedding AS real_embedding,score
    ORDER BY score DESC LIMIT 5'''

EXACT_QUERY = '''MATCH (node:WHAgentState {run_id:$run})
    USING INDEX node:WHAgentState(run_id,seq)
    WHERE node:WHRealState AND node.seq<=$seq AND node.real_embedding IS NOT NULL
    WITH node,vector.similarity.cosine(node.real_embedding,$vector) AS score
    RETURN node.key AS key,node.run_id AS run_id,node.seq AS seq,node.robot_id AS robot_id,
        node.real_payload_sha256 AS real_payload_sha256,node.real_embedding AS real_embedding,score
    ORDER BY score DESC LIMIT 5'''


class RealStore:
    def __init__(self, mirror, timeout=15):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 120:
            raise ValueError('bounded positive timeout required')
        self.mirror, self.timeout = mirror, timeout

    @classmethod
    def from_env(cls, timeout=15):
        # Read benchmark must not invoke the application's schema bootstrap.
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
            auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD']),
            max_connection_pool_size=120, connection_timeout=10,
            connection_acquisition_timeout=30, max_transaction_retry_time=0)
        driver.verify_connectivity()
        return cls(Neo4jMirror(driver, os.environ.get('NEO4J_DATABASE', 'neo4j')), timeout)

    def head(self, run):
        identity(run)
        rows = self.mirror.query('''MATCH (r:WHRun {run_id:$run})
            RETURN r.committed_seq AS seq,r.tick AS tick,r.real_last_seq AS real_last_seq,
                r.real_last_batch_sha256 AS real_last_batch_sha256''', {'run': run}, timeout=self.timeout)
        if len(rows) != 1:
            raise ValueError('unknown real run')
        return rows[0]

    def counts(self, run, prefix):
        identity(run); integer(prefix, 'prefix')
        rows = self.mirror.query('''MATCH (n:WHAgentState {run_id:$run})
            USING INDEX n:WHAgentState(run_id,seq)
            WHERE n:WHRealState AND n.seq<=$seq
            RETURN count(n) AS states,count(n.real_embedding) AS vectors,
                count(n.real_payload_sha256) AS payload_hashes,count(DISTINCT n.robot_id) AS robots,
                min(n.seq) AS first_seq,max(n.seq) AS last_seq,
                min(size(n.real_embedding)) AS min_dimensions,max(size(n.real_embedding)) AS max_dimensions''',
            {'run': run, 'seq': prefix}, timeout=120)
        extras = self.mirror.query('''MATCH (n:WHRealState {run_id:$run}) WHERE n.seq<=$seq
            OPTIONAL MATCH (n)-[d:DERIVED_FROM]->()
            RETURN count(d) AS derived_links,count(CASE WHEN n:WHScaleArchive THEN 1 END) AS archive_states''',
            {'run': run, 'seq': prefix}, timeout=120)
        indexes = self.mirror.query('''SHOW INDEXES YIELD name,state,type,options,labelsOrTypes,properties
            WHERE name=$name RETURN name,state,type,options,labelsOrTypes,properties''', {'name': VECTOR_INDEX}, timeout=30)
        return {**rows[0], **extras[0], 'vector_indexes': indexes}

    def full_query(self, run, seq, robot_id, goods_id, focus_task_id, *, mode='baseline',
                   goods_limit=200, cooperation_limit=100, goods_after=0, cooperation_after=0):
        identity(run); identity(goods_id, 'goods'); identity(focus_task_id, 'focus task')
        integer(seq, 'seq'); integer(robot_id, 'robot', 1, 100)
        integer(goods_limit, 'goods limit', 1, 1000); integer(cooperation_limit, 'cooperation limit', 1, 1000)
        integer(goods_after, 'goods cursor', 0); integer(cooperation_after, 'cooperation cursor', 0)
        query = build_query(mode)
        params = {'run': run, 'seq': seq, 'robot': robot_id,
            'step_key': f'{run}:{seq}', 'state_key': f'{run}:{seq}:{robot_id}',
            'robot_key': f'{run}:{robot_id}', 'goods': goods_id, 'cargo_key': f'{run}:{goods_id}',
            'focus_task': focus_task_id, 'goods_after': goods_after, 'coop_after': cooperation_after,
            'goods_limit': goods_limit+1, 'coop_limit': cooperation_limit+1,
            'index': VECTOR_INDEX, 'ann_count': 100, 'kinds': COOPERATION_KINDS}
        rows = self.mirror.query(query, params, timeout=self.timeout)
        if len(rows) != 1:
            raise ValueError('requested same-run committed point absent')
        row = rows[0]
        vector = row['query_vector']
        if not isinstance(vector, (list, tuple)) or len(vector) != DIMENSIONS or any(
                isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in vector):
            raise ValueError('invalid source query vector')
        similar, statements, retrieval = row['similar'], 1, 'ann100'
        if len(similar) < 5:
            similar = self.mirror.query(EXPANDED_QUERY, {'run': run, 'seq': seq, 'vector': vector,
                                                       'index': VECTOR_INDEX}, timeout=self.timeout)
            statements, retrieval = 2, 'ann1000_after_underfilled100'
        if len(similar) < 5:
            similar = self.mirror.query(EXACT_QUERY, {'run': run, 'seq': seq, 'vector': vector}, timeout=self.timeout)
            statements, retrieval = 3, 'exact_after_underfilled1000'
        if len(similar) != 5:
            raise ValueError('eligible real vector result remains incomplete')
        goods, coop = row['goods_rows'], row['coop_rows']
        goods_more, coop_more = len(goods) > goods_limit, len(coop) > cooperation_limit
        goods, coop = goods[:goods_limit], coop[:cooperation_limit]
        current = None if row['goods_event_seq'] is None else {
            'location_id': row['goods_location'], 'event_seq': row['goods_event_seq'], 'sim_time': row['goods_sim_time']}
        result = {'run_id': run, 'seq': seq, 'robot_id': robot_id, 'graph_committed_seq': row['head'],
            'config': _payload(row['config_json']), 'metadata': _payload(row['metadata_json']),
            'real_identity': {key: row[key] for key in ['real_scale_version', 'real_source_kind',
                'real_source_run', 'real_dimensions', 'real_vector_kind']},
            'batch_checksum': row['batch_checksum'], 'robot': _payload(row['robot_payload']),
            'robot_sha256': row['robot_sha256'], 'task': _payload(row['robot_task_payload']),
            'focus_task_id': focus_task_id, 'focus_task': _payload(row['focus_task_payload']),
            'history': row['history'], 'query_vector': vector, 'similar': similar,
            'goods_history': {'run_id': run, 'goods_id': goods_id, 'seq': seq,
                'graph_committed_seq': row['head'], 'current_as_of': current, 'rows': goods,
                'has_more': goods_more, 'next_after_event_seq': goods[-1]['event_seq'] if goods_more else None},
            'cooperation': {'run_id': run, 'task_id': focus_task_id, 'seq': seq,
                'graph_committed_seq': row['head'], 'rows': [{'event_seq': c['event_seq'], 'seq': c['seq'],
                    'event': _payload(c['payload'])} for c in coop], 'has_more': coop_more,
                'next_after_event_seq': coop[-1]['event_seq'] if coop_more else None},
            'query_mode': mode, 'retrieval_path': retrieval, 'bolt_statements': statements}
        # Full JSON materialization is included in the API call, not just Cypher time.
        return json.loads(json.dumps(result, ensure_ascii=False, allow_nan=False))

    def exact_neighbors(self, run, seq, vector, timeout=60):
        identity(run); integer(seq, 'seq')
        return self.mirror.query(EXACT_QUERY, {'run': run, 'seq': seq, 'vector': vector}, timeout=timeout)

    def close(self):
        self.mirror.close()
