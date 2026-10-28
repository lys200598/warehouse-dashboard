"""Explicit Neo4j-point-fenced, journal-derived catalog acceleration."""
from collections import OrderedDict
from copy import deepcopy
import re
import threading
import time

from ..query_catalog.service import QueryCatalogService, FLEET_QUERY, IncompleteContext
from ..real_scale.store import integer
from .summary import ViewNotReady


HEAD_QUERY = '''MATCH (r:WHRun {run_id:$run})
    RETURN r.run_id AS run_id,r.committed_seq AS head,toString(r.created_at) AS created_at,
        r.real_source_run AS source_run,r.metadata_json AS metadata,r.config_json AS config'''
POINT_QUERY = '''MATCH (r:WHRun {run_id:$run})
    OPTIONAL MATCH (p:WHStep {key:$key})
    RETURN r.run_id AS run_id,r.committed_seq AS current_head,toString(r.created_at) AS created_at,
        r.real_source_run AS source_run,r.metadata_json AS metadata,r.config_json AS config,
        p IS NOT NULL AS point_exists,p.key AS point_key,p.run_id AS point_run_id,p.seq AS point_seq,
        p.checksum AS point_checksum,p{.seq,.tick,.sim_time,.completed,.running} AS step'''
# Startup preparation observes a fresh head without a dynamic-key Step join.
LATEST_QUERY = HEAD_QUERY


class AcceleratedCatalog(QueryCatalogService):
    def __init__(self,store,*,summaries=None,cache_fleet=True,cache_size=8,catch_up_on_read=True):
        super().__init__(store)
        if type(cache_fleet) is not bool or type(catch_up_on_read) is not bool:
            raise ValueError('typed catalog acceleration flags required')
        integer(cache_size,'fleet cache size',1,64)
        self.summaries=summaries;self.cache_fleet=cache_fleet;self.cache_size=cache_size
        self.catch_up_on_read=catch_up_on_read
        self._fleet_cache=OrderedDict();self._cache_lock=threading.RLock()

    def _fence(self,run,seq,token=None):
        # Observe fresh H once, then resolve each complete external key in its
        # own consumed auto-commit read. No dynamic expression, two-Step join,
        # retry, sleep or old-watermark fallback is part of this protocol.
        trace={'version':'fixed-key-point-fence-v2','run_id':run,'requested_seq':seq,
               'A_rows':None,'B_rows':None,'C_rows':None,'head_A':None,'head_B':None,'head_C':None,
               'requested_exists':None,'head_exists':None,'probes':[],
               'reused_requested_for_head':False,'outcome':'pending'}
        self._metrics['catalog_fence']=trace
        try:
            started=time.perf_counter()
            try:rows=self._read(HEAD_QUERY,{'run':run})
            finally:trace['A_ms']=(time.perf_counter()-started)*1000
            trace['A_rows']=len(rows)
            if len(rows)!=1:raise ValueError('catalog run head absent')
            first=rows[0];trace['head_A']=first.get('head')
            head=integer(first.get('head'),'graph head')
            if first.get('run_id')!=run or first.get('source_run')!=run:
                raise ValueError('catalog actual source run mismatch')
            if not isinstance(first.get('created_at'),str) or not first['created_at']:
                raise ValueError('run creation version absent')
            if 'metadata' not in first or 'config' not in first:
                raise ValueError('catalog immutable source identity absent')
            if head<seq:raise ValueError('uncommitted query point')
            requested_key=f'{run}:{seq}';head_key=f'{run}:{head}'
            trace.update(requested_key=requested_key,head_key=head_key)

            def probe(key,expected_seq,role,stage):
                observation={'role':role,'key':key,'expected_seq':expected_seq,
                             'rows':None,'current_head':None,'exists':None}
                trace['probes'].append(observation);started=time.perf_counter()
                try:rows=self._read(POINT_QUERY,{'run':run,'key':key})
                finally:
                    elapsed=(time.perf_counter()-started)*1000
                    observation['ms']=elapsed;trace[stage+'_ms']=elapsed
                observation['rows']=len(rows);trace[stage+'_rows']=len(rows)
                if len(rows)!=1:raise ValueError('catalog run absent during '+role+' point resolution')
                row=rows[0];observation.update(current_head=row.get('current_head'),exists=row.get('point_exists'))
                trace['head_'+stage]=row.get('current_head')
                trace['requested_exists' if role=='requested' else 'head_exists']=row.get('point_exists')
                current=integer(row.get('current_head'),'resolution graph head')
                if current<head:raise ValueError('graph head regressed during point resolution')
                for field in ('run_id','created_at','source_run','metadata','config'):
                    if field not in row or row[field]!=first[field]:
                        raise ValueError('catalog source identity changed during point resolution: '+field)
                if row.get('point_exists') is not True:
                    raise ValueError('catalog '+('requested' if role=='requested' else 'budget head')+' point absent')
                if (row.get('point_key')!=key or row.get('point_run_id')!=run
                        or type(row.get('point_seq')) is not int or row['point_seq']!=expected_seq):
                    raise ValueError('catalog Step identity differs from bound '+role+' point')
                checksum=row.get('point_checksum')
                if not isinstance(checksum,str) or re.fullmatch('[0-9a-f]{64}',checksum) is None:
                    raise ValueError('catalog '+role+' point checksum absent/invalid')
                return row

            requested=probe(requested_key,seq,'requested','B')
            if head==seq:
                budget=requested;trace.update(head_exists=True,reused_requested_for_head=True)
            else:budget=probe(head_key,head,'head','C')
            # H2/H3 may advance, but the task budget remains the fresh H seen
            # by A, exactly as for the previous cached-head query contract.
            row={**first,'head':head,'batch_checksum':requested['point_checksum'],
                 'head_checksum':budget['point_checksum'],'step':requested['step']}
            point=self._point(row,run,seq,token)
            trace['outcome']='ok'
            return row,point
        except Exception as exc:
            trace.update(outcome='error',error=type(exc).__name__+': '+str(exc))
            raise

    def _fleet(self,run,seq):
        if not self.cache_fleet:return super()._fleet(run,seq)
        # This fresh graph read is mandatory even for a hot immutable fragment.
        fence,token=self._fence(run,seq)
        key=(run,seq,*token)
        with self._cache_lock:
            cached=self._fleet_cache.get(key)
            if cached is None:
                fleet=super()._fleet(run,seq)
                if fleet['token']!=token:raise ValueError('run recreated while filling fleet cache')
                cached=(fence['metadata'],fence['config'],deepcopy(fleet))
                self._fleet_cache[key]=cached
                while len(self._fleet_cache)>self.cache_size:self._fleet_cache.popitem(last=False)
                self._metrics['fleet_fragment_cache_hit']=False
            else:
                if cached[0]!=fence['metadata'] or cached[1]!=fence['config']:
                    raise ValueError('immutable graph metadata/config changed under cached point')
                self._fleet_cache.move_to_end(key)
                self._metrics['fleet_fragment_cache_hit']=True
            # All consumers treat this private body as immutable. The base
            # execute() performs a full JSON roundtrip before any public return,
            # so clients cannot mutate the private shared fragment.
            fleet=dict(cached[2]);fleet['head']=fence['head']
            if fleet['step']!=fence['step']:raise ValueError('immutable Step changed under cached point')
            fleet['_fresh_head_checksum']=fence['head_checksum']
            return fleet

    def prepare_run(self,run):
        """Explicit startup warmup; caller reports this outside API readiness."""
        if self.summaries is None:return None
        rows=self.store.mirror.query(LATEST_QUERY,{'run':run},timeout=self.store.timeout)
        if len(rows)!=1:raise ViewNotReady('graph run unavailable for summary warmup')
        row=rows[0];head=integer(row['head'],'graph head')
        if row['run_id']!=run:raise ValueError('foreign graph run')
        repository=self.summaries.repository(run,row['created_at'])
        result=repository.catch_up(head)
        return {**result,'run_id':run,'created_version':row['created_at'],'phase':'explicit startup warmup'}

    def _tasks(self,run,seq,fleet,maximum):
        if self.summaries is None:return super()._tasks(run,seq,fleet,maximum)
        started=time.perf_counter()
        if '_fresh_head_checksum' in fleet:
            fence={'head':fleet['head'],'head_checksum':fleet['_fresh_head_checksum']};token=fleet['token']
        else:
            fence,token=self._fence(run,seq,fleet['token'])
        self._metrics['summary_guard_ms']=(time.perf_counter()-started)*1000
        repository=self.summaries.repository(run,token[1])
        if self.catch_up_on_read and repository.status()['head']<fence['head']:
            started=time.perf_counter();result=repository.catch_up(fence['head'])
            self._metrics['summary_catch_up_ms']=(time.perf_counter()-started)*1000
            self._metrics['summary_consumed_batches']=result['consumed']
        try:
            started=time.perf_counter()
            value,entities=repository.read(run,seq,fleet,fence['head'],fence['head_checksum'])
            self._metrics['summary_read_ms']=(time.perf_counter()-started)*1000
        except ViewNotReady as error:
            raise IncompleteContext(str(error)) from error
        if entities>maximum:raise IncompleteContext('task entity budget exceeded; raise max_tasks explicitly')
        counts=value['counts']
        if (sum(c['tasks'] for c in counts)!=value['task_entity_count']
                or any(type(c['tasks']) is not int or type(c['overdue']) is not int
                       or not 0<=c['overdue']<=c['tasks'] for c in counts)):
            raise ValueError('derived task summary invalid')
        self._metrics.update(task_summary_source='original-journal-derived-exact-point',
            summary_entity_budget_head=fence['head'],summary_entity_count=entities)
        return deepcopy(counts)
