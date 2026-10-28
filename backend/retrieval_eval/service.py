"""Bounded, prefiltered EXACT business-state retrieval over immutable graph facts."""
import heapq
import json
import math
import re
import time

from ..real_scale.store import identity,integer
from ..knowledge import digest
from . import VERSION,business_vector,cosine,family


HEADER = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (s:WHStep {key:$step_key}), (q:WHAgentState {key:$state_key})
    WHERE q:WHRealState AND q.run_id=$run AND q.seq=$seq AND q.robot_id=$robot
    RETURN r.committed_seq AS head,s.checksum AS checksum,r.metadata_json AS metadata,
        q.payload AS query_payload,q.real_payload_sha256 AS query_sha256'''
CANDIDATES = '''MATCH (n:WHAgentState {run_id:$run})
    USING INDEX n:WHAgentState(run_id,seq)
    WHERE n:WHRealState AND n.seq >= $start AND n.seq <= $end
      AND ($exclude_robot=false OR n.robot_id<>$robot)
    RETURN n.key AS key,n.run_id AS run_id,n.seq AS seq,n.robot_id AS robot_id,
        n.x AS x,n.y AS y,n.battery AS battery,n.carrying AS carrying,n.stage AS stage,
        n.real_payload_sha256 AS source_payload_sha256
    LIMIT $budget'''


class CandidateBudgetExceeded(ValueError):
    pass


class BusinessRetriever:
    def __init__(self,mirror,timeout=15):
        if type(timeout) not in (int,float) or not 0<timeout<=120:
            raise ValueError('bounded positive query timeout required')
        self.mirror,self.timeout=mirror,timeout

    def search(self,run,seq,robot_id,*,k=5,window_steps=60,embargo_steps=20,
               max_candidates=10000,exclude_query_robot=True):
        identity(run);integer(seq,'cutoff');integer(robot_id,'robot',1,100)
        integer(k,'k',1,100);integer(window_steps,'window steps',1,10000)
        integer(embargo_steps,'embargo steps',1,10000)
        integer(max_candidates,'candidate budget',1,100000)
        if embargo_steps>window_steps:raise ValueError('embargo exceeds window')
        if type(exclude_query_robot) is not bool:raise ValueError('typed exclude_query_robot required')
        start,end=max(1,seq-window_steps),seq-embargo_steps
        if end<1:raise ValueError('query has no prior committed history beyond embargo')
        begun=time.perf_counter()
        headers=self.mirror.query(HEADER,{'run':run,'seq':seq,'robot':robot_id,
            'step_key':f'{run}:{seq}','state_key':f'{run}:{seq}:{robot_id}'},timeout=self.timeout)
        if len(headers)!=1:raise ValueError('one committed real query state required')
        head=headers[0]
        if type(head['head']) is not int or head['head']<seq:raise ValueError('invalid graph head')
        metadata=json.loads(head['metadata'])
        geometry=metadata['scene']['spec'];width,depth=geometry['width'],geometry['depth']
        query=json.loads(head['query_payload'])
        if type(query.get('robot_id')) is not int or query['robot_id']!=robot_id:
            raise ValueError('query robot differs from requested state')
        if digest(query)!=head['query_sha256'] or not isinstance(head['checksum'],str) or not re.fullmatch('[0-9a-f]{64}',head['checksum']):
            raise ValueError('query source payload/checksum mismatch')
        qvector=business_vector(query,width,depth)
        header_finished=time.perf_counter()
        rows=self.mirror.query(CANDIDATES,{'run':run,'robot':robot_id,'start':start,'end':end,
            'exclude_robot':exclude_query_robot,'budget':max_candidates+1},timeout=self.timeout)
        candidates_finished=time.perf_counter()
        if len(rows)>max_candidates:
            raise CandidateBudgetExceeded('eligible domain exceeds max_candidates; shorten explicit window or raise budget')
        scores=[];seen=set()
        for row in rows:
            if (row.get('run_id')!=run or type(row.get('seq')) is not int or not start<=row['seq']<=end
                or type(row.get('robot_id')) is not int or not 1<=row['robot_id']<=100
                or exclude_query_robot and row['robot_id']==robot_id
                or row.get('key')!=f"{run}:{row['seq']}:{row['robot_id']}" or row['key'] in seen):
                raise ValueError('foreign/future/self/duplicate retrieval candidate')
            if not isinstance(row.get('source_payload_sha256'),str) or not re.fullmatch('[0-9a-f]{64}',row['source_payload_sha256']):
                raise ValueError('retrieval candidate lacks committed source payload identity')
            vector=business_vector(row,width,depth)
            # Both vectors are newly validated and normalized by this module;
            # a scalar dot product is exact cosine without repeating norm/type
            # checks for every candidate. No answer or mutable facts are cached.
            score=max(-1.,min(1.,math.fsum(a*b for a,b in zip(qvector,vector))))
            scores.append({**row,'score':score,'phase_family':family(row['stage'])});seen.add(row['key'])
        selected=heapq.nsmallest(k,scores,key=lambda r:(-r['score'],r['seq'],r['robot_id']))
        return {'run_id':run,'seq':seq,'graph_committed_seq':head['head'],
            'batch_checksum':head['checksum'],'robot_id':robot_id,
            'query_source_payload_sha256':head['query_sha256'],
            'feature_version':VERSION,'method':'prefiltered-exact-cosine','dimensions':len(qvector),
            'feature_config':{'width':width,'depth':depth},
            'domain':{'start_seq':start,'end_seq':end,'window_steps':window_steps,
                'embargo_steps':embargo_steps,'exclude_query_robot':exclude_query_robot,
                'max_candidates':max_candidates,'candidate_count':len(rows),'complete_domain':True},
            'requested_k':k,'returned_k':len(selected),'rows':selected,
            'scope':'advisory structural-state similarity; not ownership authorization or predicted task outcome',
            'stage_ms':{'header_and_query_features':(header_finished-begun)*1000,
                'candidate_graph_read':(candidates_finished-header_finished)*1000,
                'validated_exact_ranking':(time.perf_counter()-candidates_finished)*1000},
            'service_ms':(time.perf_counter()-begun)*1000}
