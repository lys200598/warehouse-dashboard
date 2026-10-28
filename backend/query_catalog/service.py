"""Parameterized, cutoff-bound facts and transparent warehouse decision packs.

Every read observes immutable rows at the requested committed sequence. Exact
cutoffs/checksums and run creation versions bind multi-statement packs; mutable
entity attributes never substitute for historical task or charger state.
"""
from collections import Counter, defaultdict
from copy import deepcopy
import json
import math
import re
import threading
import time

from ..knowledge import digest
from ..real_scale.store import identity, integer
from . import VERSION


CATALOG = {
    'self': 'Specified robot and its task at the committed point',
    'readiness': 'Snapshot identity, source watermark and observed readiness',
    'task': 'Exact task version plus cargo and cooperation provenance',
    'goods': 'Cargo custody and cooperation history for a specified task',
    'energy': 'Battery configuration and 100 physical bay occupancy facts',
    'assistance': 'Nearby available peer facts, recent outcomes and explicit planning limits',
    'progress': 'Task lifecycle events and measured waiting/completion feedback',
    'memory': 'Bounded robot-state memory and actual event window',
    'local_awareness': 'Rack-zone task pressure and current fleet observations',
    'global': 'Step summary, exact task counts by rack zone and fleet/bay load',
    'similar_history': 'Explicit full ANN or business-exact retrieval; advisory only',
}

TASK_POINT_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (step:WHStep {key:$run+':'+toString($seq)})
    CALL () {
        MATCH (t:WHTaskState {run_id:$run,task_id:$task})
        USING INDEX t:WHTaskState(run_id,task_id,seq) WHERE t.seq<=$seq
        WITH t ORDER BY t.run_id DESC,t.task_id DESC,t.seq DESC LIMIT 1
        RETURN t.payload AS task,t.seq AS task_seq }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        step.checksum AS batch_checksum,task,task_seq'''

GOODS_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (step:WHStep {key:$run+':'+toString($seq)})
    CALL () {
        MATCH (g:WHGoodsState {run_id:$run,goods_id:$goods})
        USING INDEX g:WHGoodsState(run_id,goods_id,event_seq) WHERE g.seq<=$seq AND g.event_seq>=0
        WITH g ORDER BY g.run_id,g.goods_id,g.event_seq LIMIT 1001
        RETURN collect(g{.seq,.event_seq,.sim_time,.kind,.location_id,.event_key}) AS history }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        step.checksum AS batch_checksum,history'''


FLEET_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (step:WHStep {key:$run+':'+toString($seq)})
    CALL () {
        UNWIND range(1,100) AS robot
        MATCH (a:WHAgentState {key:$run+':'+toString($seq)+':'+toString(robot)})
        WHERE a:WHRealState AND a.run_id=$run AND a.seq=$seq AND a.robot_id=robot
        WITH a ORDER BY a.robot_id
        RETURN collect(a{.run_id,.seq,.robot_id,.payload,.real_payload_sha256}) AS robots }
    CALL (r) {
        MATCH (r)<-[:IN_RUN]-(bay:WHPhysicalBay)
        MATCH (c:WHChargerState {key:$run+':'+toString($seq)+':'+bay.location_id})
        WHERE c.run_id=$run AND c.seq=$seq AND c.charger_id=bay.location_id
        WITH c ORDER BY c.charger_id
        RETURN collect(c{.run_id,.seq,.charger_id,.physical_bay_id,.capacity,
            .reserved_robot_ids,.occupant_robot_id,.occupant_robot_ids,.charging_robot_id,.occupant_stage}) AS bays }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        r.real_source_run AS source_run,r.metadata_json AS metadata,r.config_json AS config,
        step.checksum AS batch_checksum,step{.seq,.tick,.sim_time,.completed,.running} AS step,robots,bays'''

EVENTS_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (cutoff:WHStep {key:$run+':'+toString($seq)})
    CALL () {
        UNWIND range($start,$seq) AS commit
        MATCH (s:WHStep {key:$run+':'+toString(commit)})<-[:IN_STEP]-(e:WHEvent)
        WHERE e.run_id=$run AND e.commit_seq=commit
          AND ($robot IS NULL OR e.robot_id=$robot)
          AND ($task IS NULL OR e.task_id=$task)
        WITH e ORDER BY e.event_seq LIMIT $limit
        RETURN collect(e{.run_id,.commit_seq,.event_seq,.payload}) AS events }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        cutoff.checksum AS batch_checksum,events'''

MEMORY_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (cutoff:WHStep {key:$run+':'+toString($seq)})
    CALL () {
        MATCH (a:WHAgentState {run_id:$run,robot_id:$robot})
        USING INDEX a:WHAgentState(run_id,robot_id,seq)
        WHERE a:WHRealState AND a.seq >= $start AND a.seq <= $seq
        WITH a ORDER BY a.run_id,a.robot_id,a.seq LIMIT $limit
        RETURN collect(a{.run_id,.seq,.robot_id,.payload,.real_payload_sha256}) AS states }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        cutoff.checksum AS batch_checksum,states'''

# One indexed latest-version seek per task entity, never a scan of all its
# versions followed by collect/head. Entities created after cutoff resolve null.
TASK_COUNTS_QUERY = '''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
    MATCH (cutoff:WHStep {key:$run+':'+toString($seq)})
    CALL (r) {
        MATCH (r)<-[:IN_RUN]-(t:WHTask)
        WITH t ORDER BY t.task_id LIMIT $limit
        RETURN collect(t.task_id) AS task_ids }
    CALL (task_ids) {
        UNWIND task_ids AS task_id
        CALL (task_id) {
            MATCH (v:WHTaskState {run_id:$run,task_id:task_id})
            USING INDEX v:WHTaskState(run_id,task_id,seq) WHERE v.seq <= $seq
            WITH v ORDER BY v.run_id DESC,v.task_id DESC,v.seq DESC LIMIT 1
            RETURN v }
        OPTIONAL MATCH (slot:WHSlot {key:$run+':'+v.slot_id})-[:IN_RACK]->(rack:WHRack)
        WITH coalesce(rack.zone,'unmapped') AS zone,v.status AS status,
            count(*) AS tasks,sum(CASE WHEN v.status<>'completed' AND v.due_second < $clock THEN 1 ELSE 0 END) AS overdue
        RETURN collect({zone:zone,status:status,tasks:tasks,overdue:overdue}) AS counts }
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        cutoff.checksum AS batch_checksum,size(task_ids) AS entity_count,counts'''


class IncompleteContext(ValueError):
    """A complete decision pack was requested but its explicit budget was hit."""


def _json(value):
    result = json.loads(value) if isinstance(value, str) else deepcopy(value)
    if not isinstance(result, dict):
        raise ValueError('JSON object required')
    # Reject nonfinite payloads and preserve strict bool/int distinctions later.
    json.dumps(result, allow_nan=False)
    return result


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('finite '+name+' required')
    return value


def rack_zone(robot, scene):
    """Declared spatial convention: nearest rack centre, not home ownership."""
    racks = scene['racks']
    if not racks:
        raise ValueError('rack geometry absent')
    closest = min(racks, key=lambda r: (math.hypot(robot['x']-r['x']-r['w']/2,
                                                  robot['y']-r['y']-r['d']/2), r['id']))
    return closest['zone']


def event_feedback(events):
    """Observed window feedback; an absent start is reported, never inferred."""
    by_task = defaultdict(list)
    for item in events:
        event = item['event']
        if event.get('task_id'):
            by_task[event['task_id']].append(event)
    feedback = []
    for task, rows in sorted(by_task.items()):
        release = next((e for e in rows if e['kind']=='task_released'), None)
        assigned = None
        picked = None
        attempts = []
        for e in rows:
            if e['kind']=='task_assigned':
                assigned = e
            elif e['kind']=='picked':
                picked = e
                if assigned:
                    attempts.append({'robot_id':e.get('robot_id'),
                        'assignment_to_pickup_seconds':e['sim_time']-assigned['sim_time']})
            elif e['kind'] in ('placed','shipped'):
                feedback.append({'task_id':task,'completion_kind':e['kind'],
                    'completed_at':e['sim_time'],'robot_id':e.get('robot_id'),
                    'release_to_completion_seconds':e['sim_time']-release['sim_time'] if release else None,
                    'pickup_to_completion_seconds':e['sim_time']-picked['sim_time'] if picked else None,
                    'assignment_pickup_observations':deepcopy(attempts),
                    'release_observed_in_window':release is not None,
                    'pickup_observed_in_window':picked is not None})
    return feedback


def validate_cooperation(value,run,seq,task,goods):
    """Read-pack provenance checks; this is not live transfer authorization."""
    if value.get('request_cursors')!={'goods_after':0,'cooperation_after':0}:
        raise ValueError('complete first-page context required')
    for name,parent,key in (('goods_history',goods,'goods_id'),('cooperation',task,'task_id')):
        part=value[name]
        if (part.get('run_id')!=run or type(part.get('seq')) is not int or part['seq']!=seq
                or type(part.get('graph_committed_seq')) is not int
                or part['graph_committed_seq']!=value['graph_committed_seq'] or part.get(key)!=parent):
            raise ValueError('mixed historical context point')
        if part.get('has_more') is not False or part.get('next_after_event_seq') is not None:
            raise IncompleteContext('full custody/cooperation pack exceeds bounded history; no silent tail-only decision')
        previous=0
        for row in part['rows']:
            if (type(row.get('seq')) is not int or not 1<=row['seq']<=seq
                    or type(row.get('event_seq')) is not int or row['event_seq']<=previous):
                raise ValueError('future/unordered historical context row')
            previous=row['event_seq']
            if name=='cooperation':
                event=row['event']
                if event.get('task_id')!=task or type(event.get('event_seq')) is not int or event['event_seq']!=previous:
                    raise ValueError('foreign cooperation event')
    history=value['goods_history'];rows=history['rows'];current=history.get('current_as_of')
    if bool(rows)!=(current is not None):raise ValueError('cargo current state lacks matching history')
    if rows and any(current.get(k)!=rows[-1].get(k) for k in ('event_seq','location_id','sim_time')):
        raise ValueError('cargo history does not reach declared current state')


class QueryCatalogService:
    def __init__(self, store):
        self.store = store
        self._local = threading.local()

    @property
    def _metrics(self):
        return getattr(self._local,'metrics',{})

    @_metrics.setter
    def _metrics(self,value):
        self._local.metrics=value

    @classmethod
    def from_env(cls, timeout=15, *, pool_size=120):
        from ..query_v2.service_tail import TailQueryService
        return cls(TailQueryService.from_env(timeout=timeout, pool_size=pool_size))

    @classmethod
    def describe(cls):
        return {'version':VERSION,'queries':deepcopy(CATALOG),
            'required_common':['query_id','run_id','seq'],
            'optional_common':['robot_id','task_id','goods_id','start_seq','event_limit','history_limit',
                'max_tasks','zone','radius_m','min_soc','limit','variant','window_steps','embargo_steps','max_candidates'],
            'time_semantics':'explicit committed cutoff; window is inclusive commit sequence, not uniform elapsed duration',
            'control_use':'catalog is read-only/advisory; knowledge_runtime separately consumes cooperation_context',
            'mapping_file':'backend/query_catalog/kg56_mapping.json'}

    @property
    def latest_metrics(self):
        return deepcopy(self._metrics)

    def _read(self, query, params):
        self._metrics['statements'] += 1
        return self.store.mirror.query(query, params, timeout=self.store.timeout)

    def _point(self, row, run, seq, token=None):
        if row.get('run_id')!=run or type(row.get('head')) is not int or row['head']<seq:
            raise ValueError('foreign/uncommitted query point')
        checksum = row.get('batch_checksum')
        if not isinstance(checksum,str) or re.fullmatch('[0-9a-f]{64}',checksum) is None:
            raise ValueError('invalid source batch checksum')
        created = row.get('created_at')
        if not isinstance(created,str) or not created:
            raise ValueError('run creation version absent')
        current = (checksum,created)
        if token is not None and token!=current:
            raise ValueError('mixed source batch or recreated run within pack')
        self._metrics['observed_heads'].append(row['head'])
        return current

    def _fleet(self, run, seq):
        rows = self._read(FLEET_QUERY, {'run':run,'seq':seq})
        if len(rows)!=1:
            raise ValueError('same-run fleet point absent')
        row = rows[0]
        token = self._point(row,run,seq)
        if row.get('source_run')!=run:
            raise ValueError('actual-state source identity absent')
        states = row['robots']
        if len(states)!=100 or {s['robot_id'] for s in states}!=set(range(1,101)):
            raise IncompleteContext('100 unique robot states required')
        robots=[]
        for s in states:
            if (s.get('run_id')!=run or type(s.get('seq')) is not int or s['seq']!=seq
                    or type(s.get('robot_id')) is not int):
                raise ValueError('mixed fleet point')
            p=_json(s['payload'])
            if type(p.get('robot_id')) is not int or p['robot_id']!=s['robot_id'] or digest(p)!=s['real_payload_sha256']:
                raise ValueError('robot source payload mismatch')
            for field in ('x','y','battery'):_number(p[field],field)
            if not 0<=p['battery']<=100 or type(p.get('carrying')) is not bool:
                raise ValueError('invalid robot physical facts')
            robots.append(p)
        metadata,config=_json(row['metadata']),_json(row['config'])
        scene=metadata['scene']
        expected={d['id'] for d in scene['docks']}
        bays=row['bays']
        if len(expected)!=100 or len(bays)!=100 or {b['charger_id'] for b in bays}!=expected:
            raise IncompleteContext('100 canonical physical bay states required')
        for b in bays:
            if (b.get('run_id')!=run or type(b.get('seq')) is not int or b['seq']!=seq
                    or b.get('physical_bay_id')!=b['charger_id'] or type(b.get('capacity')) is not int or b['capacity']!=1):
                raise ValueError('mixed/aliased physical bay')
            ids=b.get('occupant_robot_ids') or []
            if len(ids)>1 or any(type(i) is not int or not 1<=i<=100 for i in ids):
                raise ValueError('invalid bay occupancy')
            if b.get('occupant_robot_id')!=(ids[0] if ids else None):
                raise ValueError('conflicting bay occupant aliases')
            charging=b.get('charging_robot_id')
            if charging is not None and (type(charging) is not int or charging not in ids):
                raise ValueError('charging robot is not the physical occupant')
            reserved=b.get('reserved_robot_ids')
            if (not isinstance(reserved,list) or len(set(reserved))!=len(reserved)
                    or any(type(i) is not int or not 1<=i<=100 for i in reserved)):
                raise ValueError('invalid bay reservation facts')
        step=deepcopy(row['step'])
        if type(step.get('seq')) is not int or step['seq']!=seq or type(step.get('sim_time')) is not int:
            raise ValueError('mixed source step')
        integer(step['sim_time'],'source sim_time',0);integer(step.get('completed'),'completed',0)
        return {'robots':sorted(robots,key=lambda r:r['robot_id']),'bays':bays,'metadata':metadata,
                'config':config,'step':step,'token':token,'head':row['head']}

    def _events(self, run, seq, start, limit, token, *, robot=None, task=None):
        rows=self._read(EVENTS_QUERY,{'run':run,'seq':seq,'start':start,'limit':limit+1,'robot':robot,'task':task})
        if len(rows)!=1:raise ValueError('event window source absent')
        self._point(rows[0],run,seq,token)
        values=rows[0]['events']
        if len(values)>limit:raise IncompleteContext('event window exceeds event_limit; narrow start_seq or explicitly raise budget')
        result=[];previous=0
        for row in values:
            if (row['run_id']!=run or type(row['commit_seq']) is not int or not start<=row['commit_seq']<=seq
                    or type(row['event_seq']) is not int or row['event_seq']<=previous):
                raise ValueError('future/foreign/unordered event')
            e=_json(row['payload'])
            if (e.get('event_seq')!=row['event_seq'] or type(e.get('event_seq')) is not int
                    or robot is not None and e.get('robot_id')!=robot
                    or task is not None and e.get('task_id')!=task):raise ValueError('event payload/filter mismatch')
            _number(e.get('sim_time'),'event time')
            previous=row['event_seq'];result.append({'seq':row['commit_seq'],'event_seq':previous,'event':e})
        return result

    def _tasks(self, run, seq, fleet, maximum):
        rows=self._read(TASK_COUNTS_QUERY,{'run':run,'seq':seq,'limit':maximum+1,'clock':fleet['step']['sim_time']})
        if len(rows)!=1:raise ValueError('task summary source absent')
        self._point(rows[0],run,seq,fleet['token'])
        entity_count=integer(rows[0]['entity_count'],'task entity count',0)
        if entity_count>maximum:raise IncompleteContext('task entity budget exceeded; raise max_tasks explicitly')
        counts=rows[0]['counts']
        for row in counts:
            integer(row['tasks'],'task count',0);integer(row['overdue'],'overdue count',0)
            if row['overdue']>row['tasks']:raise ValueError('invalid overdue summary')
        if sum(row['tasks'] for row in counts)>entity_count:
            raise ValueError('duplicate task aggregation')
        completed=sum(row['tasks'] for row in counts if row['status']=='completed')
        if completed!=fleet['step']['completed']:raise IncompleteContext('task summary and committed Step completion count disagree')
        return counts

    def _task(self, run, seq, task):
        rows=self._read(TASK_POINT_QUERY,{'run':run,'seq':seq,'task':task})
        if len(rows)!=1:raise ValueError('task absent at requested cutoff')
        self._point(rows[0],run,seq)
        if type(rows[0]['task_seq']) is not int or not 1<=rows[0]['task_seq']<=seq:
            raise ValueError('future task version')
        value=_json(rows[0]['task'])
        if value.get('task_id')!=task:raise ValueError('foreign task version')
        return value,rows[0]['batch_checksum']

    def execute(self, request):
        if not isinstance(request,dict):raise ValueError('query request object required')
        allowed=set(self.describe()['required_common']+self.describe()['optional_common'])
        if set(request)-allowed:raise ValueError('unknown query parameters: '+','.join(sorted(set(request)-allowed)))
        query=request.get('query_id');run=identity(request.get('run_id'),'run');seq=integer(request.get('seq'),'seq')
        if query not in CATALOG:raise ValueError('unknown query_id')
        robot=integer(request.get('robot_id',1),'robot',1,100)
        start=integer(request.get('start_seq',max(1,seq-60)),'start_seq',1,seq)
        if seq-start>3600:raise ValueError('window limited to 3601 commits; narrow start_seq')
        event_limit=integer(request.get('event_limit',2000),'event_limit',1,10000)
        history_limit=integer(request.get('history_limit',3601),'history_limit',1,10000)
        maximum=integer(request.get('max_tasks',100000),'max_tasks',1,1000000)
        limit=integer(request.get('limit',10),'limit',1,100)
        for field in ('task_id','goods_id','zone'):
            if request.get(field) is not None:identity(request[field],field)
        started=time.perf_counter();self._metrics={'statements':0,'observed_heads':[]}
        try:
            if query in ('self','readiness'):
                value=self.store.robot_context(run,seq,[robot])
                if (value['run_id']!=run or type(value['seq']) is not int or value['seq']!=seq
                        or type(value['graph_committed_seq']) is not int or value['graph_committed_seq']<seq):
                    raise ValueError('mixed self context')
                p=value['robots'][0]
                if (type(p.get('robot_id')) is not int or p['robot_id']!=robot
                        or type(p['robot'].get('robot_id')) is not int or p['robot']['robot_id']!=robot
                        or digest(p['robot'])!=p['robot_sha256']):
                    raise ValueError('self payload identity/hash mismatch')
                result={'context':value,'readiness':{'requested_point_present':True,'future_point_used':False,
                    'known_committed_lag_steps':value['graph_committed_seq']-seq,
                    'ready_for_live_action':False,'note':'historical read; live coordinator must recheck current ownership/reservations'}}
            elif query in ('task','goods'):
                if query=='goods' and request.get('task_id') is None:
                    goods=identity(request.get('goods_id'),'goods')
                    rows=self._read(GOODS_QUERY,{'run':run,'seq':seq,'goods':goods})
                    if len(rows)!=1:raise ValueError('cargo query cutoff absent')
                    self._point(rows[0],run,seq)
                    history=rows[0]['history']
                    if len(history)>1000:raise IncompleteContext('custody history exceeds bounded complete pack')
                    previous=0
                    for item in history:
                        if (type(item['seq']) is not int or not 1<=item['seq']<=seq or type(item['event_seq']) is not int
                                or item['event_seq']<=previous):raise ValueError('future/unordered cargo history')
                        previous=item['event_seq']
                    result={'goods_id':goods,'history':history,'found':bool(history),
                        'current_as_of':history[-1] if history else None,
                        'batch_checksum':rows[0]['batch_checksum'],
                        'cooperation_scope':'supply task_id to request its cooperation context'}
                else:
                    task=identity(request.get('task_id'),'task')
                    current,checksum=self._task(run,seq,task)
                    goods=identity(request.get('goods_id',current['goods_id']),'goods')
                    if goods!=current['goods_id']:raise ValueError('task/cargo mismatch')
                    value=self.store.cooperation_context(run,seq,task,goods,goods_limit=1000,cooperation_limit=1000)
                    if (value.get('run_id')!=run or type(value.get('seq')) is not int or value.get('seq')!=seq
                            or type(value.get('graph_committed_seq')) is not int or value['graph_committed_seq']<seq
                            or value.get('task_id')!=task
                            or value.get('goods_id')!=goods or digest(value.get('focus_task'))!=digest(current)):
                        raise ValueError('mixed task/cargo context')
                    if value.get('batch_checksum')!=checksum:raise ValueError('mixed task/cargo source batch')
                    validate_cooperation(value,run,seq,task,goods)
                    result={'context':value}
            elif query=='similar_history':
                variant=request.get('variant','business-exact')
                if variant=='business-exact':
                    from ..retrieval_eval.service import BusinessRetriever
                    result=BusinessRetriever(self.store.mirror,timeout=self.store.timeout).search(run,seq,robot,
                        k=limit,window_steps=request.get('window_steps',60),embargo_steps=request.get('embargo_steps',20),
                        max_candidates=request.get('max_candidates',10000))
                elif variant=='full-ann':
                    task=identity(request.get('task_id'),'task');goods=identity(request.get('goods_id'),'goods')
                    result=self.store.full_query(run,seq,robot,goods,task)
                else:raise ValueError('unknown similarity variant')
                result={'variant':variant,'context':result,'control_role':'advisory; cannot authorize cargo transfer'}
            else:
                fleet=self._fleet(run,seq);scene=fleet['metadata']['scene'];me=fleet['robots'][robot-1]
                events=[]
                if query in ('assistance','progress','memory','local_awareness','global'):
                    events=self._events(run,seq,start,event_limit,fleet['token'],
                        robot=robot if query=='memory' else None,task=request.get('task_id') if query=='progress' else None)
                    if any(e['event']['sim_time']>fleet['step']['sim_time'] for e in events):
                        raise ValueError('future event effective time')
                result={'point':{'batch_checksum':fleet['token'][0],'run_created_at':fleet['token'][1],
                    'graph_committed_seq':fleet['head'],'sim_time':fleet['step']['sim_time']}}
                if query=='energy':
                    result.update(robot=me,thresholds={k:fleet['config'].get(k) for k in
                        ('energy_profile','task_reserve_soc','emergency_reserve_soc','charge_request_soc','charge_target_soc')},
                        bays=fleet['bays'],empty_physical_bays=sum(not b['occupant_robot_ids'] for b in fleet['bays']),
                        planning_required=['route length and reservation','loaded/empty energy cost','assigned-bay policy'],
                        availability_scope='observed bodies/reservations at cutoff; no route or future charging admission claim')
                elif query=='assistance':
                    radius=_number(request.get('radius_m',200.),'radius_m');min_soc=_number(request.get('min_soc',35.),'min_soc')
                    if not 0<=radius<=10000 or not 0<=min_soc<=100:raise ValueError('invalid peer bounds')
                    peers=[]
                    for p in fleet['robots']:
                        distance=math.hypot(p['x']-me['x'],p['y']-me['y'])
                        if (p['robot_id']!=robot and p['stage']=='idle' and not p['carrying']
                                and p.get('task_id') is None and p['battery']>=min_soc and distance<=radius):
                            peers.append({'robot':p,'distance_lower_bound_m':distance,'rack_zone':rack_zone(p,scene)})
                    peers.sort(key=lambda p:(p['distance_lower_bound_m'],p['robot']['robot_id']))
                    result.update(robot=me,candidates=peers[:limit],eligible_candidate_count=len(peers),
                        candidate_list_truncated=len(peers)>limit,events=events,feedback=event_feedback(events),
                        planning_required=['energy admission including return reserve','route/traffic reservation','live unique ownership'],
                        candidate_scope='idle, empty, minimum SOC and Euclidean radius facts only; not feasible assignments')
                elif query=='progress':
                    result.update(events=events,kind_counts=dict(Counter(e['event']['kind'] for e in events)),feedback=event_feedback(events),
                        filter_task_id=request.get('task_id'),window={'start_seq':start,'end_seq':seq,'complete':True})
                elif query=='memory':
                    rows=self._read(MEMORY_QUERY,{'run':run,'seq':seq,'start':start,'robot':robot,'limit':history_limit+1})
                    if len(rows)!=1:raise ValueError('memory source absent')
                    self._point(rows[0],run,seq,fleet['token']);states=rows[0]['states']
                    if len(states)>history_limit:raise IncompleteContext('memory window exceeds history_limit')
                    history=[];previous=start-1
                    for s in states:
                        p=_json(s['payload'])
                        if (s['run_id']!=run or type(s['robot_id']) is not int or s['robot_id']!=robot or type(s['seq']) is not int
                                or s['seq']!=previous+1 or s['seq']>seq or p['robot_id']!=robot
                                or digest(p)!=s['real_payload_sha256']):raise ValueError('incomplete/foreign memory window')
                        for field in ('x','y','battery','distance'):_number(p[field],field)
                        history.append({'seq':s['seq'],'state':p});previous=s['seq']
                    if previous!=seq:raise IncompleteContext('memory does not reach cutoff')
                    if digest(history[-1]['state'])!=digest(me):raise ValueError('memory latest state differs from fleet point')
                    first,last=history[0]['state'],history[-1]['state']
                    xs=[h['state']['x'] for h in history];ys=[h['state']['y'] for h in history]
                    span=math.hypot(max(xs)-min(xs),max(ys)-min(ys))
                    result.update(robot_id=robot,states=history,events=events,
                        observations={'samples':len(history),'stage_samples':dict(Counter(h['state']['stage'] for h in history)),
                            'delta_x':last['x']-first['x'],'delta_y':last['y']-first['y'],
                            'battery_delta':last['battery']-first['battery'],'distance_delta':last['distance']-first['distance'],
                            'position_span_m':span,'stationary_by_position':span<=1. if len(history)>1 else None,
                            'sample_counts_are_continuous_durations':False})
                elif query in ('local_awareness','global'):
                    counts=self._tasks(run,seq,fleet,maximum)
                    zones=sorted({r['zone'] for r in scene['racks']})
                    zone=request.get('zone',rack_zone(me,scene)) if query=='local_awareness' else None
                    if zone is not None and zone not in zones:raise ValueError('unknown rack zone')
                    selected=[p for p in fleet['robots'] if zone is None or rack_zone(p,scene)==zone]
                    result.update(zone=zone,robot_region_semantics='nearest rack centre at observed coordinates',
                        task_region_semantics='task target slot rack zone',
                        fleet_counts=dict(Counter(p['stage'] for p in selected)),robots=selected,
                        task_counts=[c for c in counts if zone is None or c['zone']==zone],
                        step=fleet['step'],bay_counts={'physical_bays':100,
                            'occupied':sum(bool(b['occupant_robot_ids']) for b in fleet['bays']),
                            'charging':sum(b['charging_robot_id'] is not None for b in fleet['bays'])},
                        recent_event_kind_counts=dict(Counter(e['event']['kind'] for e in events)),
                        event_window_scope='global run window; event zone is not inferred from current task ownership',
                        window={'start_seq':start,'end_seq':seq,'complete':True})
            result={'catalog_version':VERSION,'query_id':query,'run_id':run,'seq':seq,
                'read_only':True,'used_by_live_control':False,'result':result}
            # Include complete JSON encoding/decoding in the API, not driver time only.
            return json.loads(json.dumps(result,ensure_ascii=False,allow_nan=False))
        finally:
            self._metrics['service_ms']=(time.perf_counter()-started)*1000

    def close(self):
        self.store.close()
