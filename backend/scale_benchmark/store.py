"""Run-scoped additive writes and fully materialized reads; never delete data."""
import json

from backend.knowledge_graph import Neo4jMirror
from backend.wide.queries import WideQueries
from .model import VECTOR_INDEX, DIMENSIONS, identity


BATCHED_QUERY = '''
    MATCH (business:WHRun {run_id:$business}) WHERE business.committed_seq >= $business_seq
    MATCH (robot:WHAgentState {key:$business_key}), (point:WHAgentState {key:$point_key})
    OPTIONAL MATCH (task:WHTask {key:$business+':'+robot.task_id})
    CALL (task) {
        OPTIONAL MATCH (version:WHTaskState)-[:OF_TASK]->(task) WHERE version.seq<=$business_seq
        RETURN version.payload AS task_payload ORDER BY version.seq DESC LIMIT 1
    }
    CALL () {
        MATCH (a:WHAgentState {run_id:$run,robot_id:$robot})
        USING INDEX a:WHAgentState(run_id,robot_id,seq)
        WHERE a.seq<=$seq WITH a ORDER BY a.seq DESC LIMIT 5
        RETURN collect(a{.key,.seq,.robot_id,.source_key,.payload,.source_payload_sha256}) AS history
    }
    CALL (point) {
        CALL db.index.vector.queryNodes($index,100,point.embedding) YIELD node,score
        WHERE node.run_id=$run AND node.seq<=$seq
        WITH node,score ORDER BY score DESC LIMIT 5
        RETURN collect(node{.key,.run_id,.seq,.robot_id,.embedding,score:score}) AS similar
    }
    RETURN business.committed_seq AS business_head,robot.payload AS robot_payload,
        task_payload,history,point.embedding AS query_vector,similar
'''

ANN_EXPANDED_QUERY = '''CALL db.index.vector.queryNodes($index,1000,$vector) YIELD node,score
    WHERE node.run_id=$run AND node.seq<=$seq
    RETURN node.key AS key,node.run_id AS run_id,node.seq AS seq,node.robot_id AS robot_id,
        node.embedding AS embedding,score ORDER BY score DESC LIMIT 5'''

EXACT_ELIGIBLE_QUERY = '''MATCH (node:WHAgentState {run_id:$run})
    USING INDEX node:WHAgentState(run_id,seq)
    WHERE node.seq<=$seq AND node.embedding IS NOT NULL
    WITH node,vector.similarity.cosine(node.embedding,$vector) AS score
    ORDER BY score DESC LIMIT 5
    RETURN node.key AS key,node.run_id AS run_id,node.seq AS seq,node.robot_id AS robot_id,
        node.embedding AS embedding,score'''


class ScaleStore:
    def __init__(self, mirror):
        self.mirror = mirror

    @classmethod
    def from_env(cls):
        return cls(Neo4jMirror.from_env())

    def write(self, query, timeout=120, **parameters):
        from neo4j import Query
        with self.mirror.driver.session(database=self.mirror.database) as session:
            return [dict(row) for row in session.run(Query(query, timeout=timeout), parameters)]

    def head(self, run):
        rows = self.mirror.query('MATCH (r:WHRun {run_id:$run}) RETURN properties(r) AS metadata', {'run':run})
        return rows[0]['metadata'] if rows else None

    def templates(self, business_run):
        rows = self.mirror.query('''MATCH (a:WHAgentState {run_id:$run})
            RETURN a.seq AS seq,a.robot_id AS robot,a.payload AS payload ORDER BY a.seq,a.robot_id LIMIT 200001''',
            {'run':business_run}, timeout=120)
        if not rows or len(rows) > 200000:
            raise ValueError('business source must have 1..200000 states (bounded template materialization)')
        grouped = {}
        for row in rows:
            grouped.setdefault(row['seq'], {})[row['robot']] = row['payload']
        if any(set(group) != set(range(1, 101)) for group in grouped.values()):
            raise ValueError('every business source commit must contain exactly robots 1..100')
        return sorted(grouped), {(seq, robot):payload for seq, group in grouped.items() for robot, payload in group.items()}

    def create_archive(self, run, business, steps, source_hash, resume=False):
        identity(run); identity(business)
        if run == business:
            raise ValueError('archive and real business run must be distinct')
        existing = self.head(run)
        if existing:
            if not resume or any(existing.get(k) != v for k,v in {
                    'scale_version':'warehouse-scale-v2','source_run':business,'archive_steps':steps,
                    'source_hash':source_hash,'dimensions':DIMENSIONS}.items()):
                raise ValueError('existing archive is not an explicitly resumable identical contract')
            return int(existing.get('committed_seq', 0))
        if resume:
            raise ValueError('resume requested but archive absent')
        self.write('''CREATE (r:WHRun {run_id:$run,scale_version:'warehouse-scale-v2',
                scale_kind:'synthetic_resampled_history; not actual new execution',source_run:$business,
                source_hash:$source_hash,archive_steps:$steps,dimensions:$dims,committed_seq:0})
            WITH r UNWIND range(1,100) AS id
            CREATE (a:WHRobot {key:$run+':'+toString(id),run_id:$run,robot_id:id})
            CREATE (a)-[:IN_RUN]->(r)''', run=run, business=business, steps=steps, dims=DIMENSIONS,source_hash=source_hash)
        return 0

    def task_templates(self, business):
        rows=self.mirror.query('''MATCH (t:WHTaskState {run_id:$run})
            RETURN t.seq AS seq,t.task_id AS task_id,t.payload AS payload ORDER BY t.seq LIMIT 400001''',
            {'run':business},timeout=120)
        if len(rows)>400000:
            raise ValueError('task oracle exceeds bounded materialization')
        return rows

    def append(self, run, rows, first, last):
        # A single explicit transaction: all source relations/state properties and
        # commit watermark succeed together. Retrying requires explicit --resume.
        from neo4j import Query
        with self.mirror.driver.session(database=self.mirror.database) as session:
            with session.begin_transaction(timeout=120) as tx:
                current = tx.run('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq',run=run).single()['seq']
                if current != first-1:
                    raise ValueError('archive sequence gap or concurrent writer')
                tx.run('''MATCH (r:WHRun {run_id:$run}) UNWIND range($first,$last) AS seq
                    CREATE (s:WHStep {key:$run+':'+toString(seq),run_id:$run,seq:seq,tick:seq,
                        scale_kind:'synthetic_resampled_history'}) CREATE (s)-[:IN_RUN]->(r)''',
                    run=run,first=first,last=last).consume()
                result = tx.run('''UNWIND $rows AS p
                    MATCH (source:WHAgentState {key:p.source_key}),(robot:WHRobot {key:p.robot_key}),
                          (step:WHStep {key:p.step_key})
                    CREATE (a:WHAgentState:WHScaleArchive) SET a=p
                    CREATE (a)-[:OF_ROBOT]->(robot) CREATE (a)-[:IN_STEP]->(step)
                    CREATE (a)-[:DERIVED_FROM]->(source) RETURN count(a) AS count''',rows=rows).single()
                if result['count'] != len(rows):
                    raise ValueError('archive source match dropped states')
                tx.run('MATCH (r:WHRun {run_id:$run}) SET r.committed_seq=$last,r.tick=$last',run=run,last=last).consume()
                tx.commit()

    def ensure_vector_index(self):
        self.write(f'''CREATE VECTOR INDEX {VECTOR_INDEX} IF NOT EXISTS FOR (a:WHScaleArchive)
            ON (a.embedding) OPTIONS {{indexConfig: {{`vector.dimensions`: {DIMENSIONS},
            `vector.similarity_function`: 'cosine',`vector.quantization.enabled`: false}}}}''')
        self.write('CALL db.awaitIndexes(600)',timeout=660)
        return self.mirror.query('SHOW INDEXES YIELD name,state,type,options WHERE name=$name RETURN name,state,type,options',{'name':VECTOR_INDEX})

    def counts(self, run):
        rows=self.mirror.query('''MATCH (a:WHAgentState {run_id:$run})
            RETURN count(a) AS states,count(a.embedding) AS vectors,
                min(a.seq) AS first_seq,max(a.seq) AS last_seq,count(DISTINCT a.robot_id) AS robots''',{'run':run},timeout=120)
        links=self.mirror.query('''MATCH (a:WHAgentState {run_id:$run})-[:DERIVED_FROM]->(s:WHAgentState)
            RETURN count(*) AS source_links''',{'run':run},timeout=120)
        return {**rows[0],**links[0]}

    def watermark(self, run):
        rows=self.mirror.query('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq,r.tick AS tick',{'run':run})
        return rows[0] if rows else {'seq':0,'tick':None}

    def full_query(self, business_run, business_seq, archive_run, robot, seq, query_mode='baseline'):
        if query_mode=='batched':
            return self._batched_query(business_run,business_seq,archive_run,robot,seq)
        if query_mode!='baseline':
            raise ValueError('unknown query mode')
        business=WideQueries(self.mirror).robot_context(business_run,robot,seq=business_seq)
        history=self.mirror.query('''MATCH (a:WHAgentState {run_id:$run,robot_id:$robot})
            USING INDEX a:WHAgentState(run_id,robot_id,seq)
            WHERE a.seq<=$seq RETURN a.key AS key,a.seq AS seq,a.robot_id AS robot_id,
                a.source_key AS source_key,a.payload AS payload,a.source_payload_sha256 AS source_payload_sha256
            ORDER BY a.seq DESC LIMIT 5''',{'run':archive_run,'robot':robot,'seq':seq},timeout=15)
        point=self.mirror.query('MATCH (a:WHAgentState {key:$key}) RETURN a.embedding AS embedding',
            {'key':f'{archive_run}:{seq}:{robot}'},timeout=15)
        if len(point)!=1:
            raise ValueError('archive query point absent')
        vector=point[0]['embedding']
        similar=self.mirror.query('''CALL db.index.vector.queryNodes($index,100,$vector) YIELD node,score
            WHERE node.run_id=$run AND node.seq<=$seq
            RETURN node.key AS key,node.run_id AS run_id,node.seq AS seq,node.robot_id AS robot_id,
                node.embedding AS embedding,score ORDER BY score DESC LIMIT 5''',
            {'index':VECTOR_INDEX,'vector':vector,'run':archive_run,'seq':seq},timeout=15)
        # Round-trip JSON is part of the complete API materialization cost.
        return json.loads(json.dumps({'business':business,'history':history,'query_vector':vector,'similar':similar}))

    def _batched_query(self,business_run,business_seq,archive_run,robot,seq):
        params={'business':business_run,'business_seq':business_seq,
            'business_key':f'{business_run}:{business_seq}:{robot}',
            'point_key':f'{archive_run}:{seq}:{robot}','run':archive_run,'robot':robot,
            'seq':seq,'index':VECTOR_INDEX}
        def read(query,parameters,path,statements):
            try:
                return self.mirror.query(query,parameters,timeout=15)
            except Exception as exc:
                exc.scale_retrieval_path=path
                exc.scale_bolt_statements=statements
                raise
        rows=read(BATCHED_QUERY,params,'batched_initial',1)
        if len(rows)!=1:
            raise ValueError('business/archive committed query point absent')
        row=rows[0]
        similar=row['similar'];path='ann100';statements=1
        if not similar:
            similar=read(ANN_EXPANDED_QUERY,
                {'index':VECTOR_INDEX,'vector':row['query_vector'],'run':archive_run,'seq':seq},
                'ann1000_after_empty100',2)
            path='ann1000_after_empty100';statements=2
        if not similar:
            similar=read(EXACT_ELIGIBLE_QUERY,
                {'vector':row['query_vector'],'run':archive_run,'seq':seq},
                'exact_eligible_after_empty1000',3)
            path='exact_eligible_after_empty1000';statements=3
        business={'run_id':business_run,'robot_id':robot,'seq':business_seq,
            'graph_committed_seq':row['business_head'],'robot':json.loads(row['robot_payload']),
            'task':json.loads(row['task_payload']) if row['task_payload'] else None}
        result={'business':business,'history':row['history'],'query_vector':row['query_vector'],
            'similar':similar,'retrieval_path':path,'bolt_statements':statements}
        return json.loads(json.dumps(result))

    def close(self):
        self.mirror.close()
