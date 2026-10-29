"""Opt-in bounded exact event-count reuse, guarded by fresh Neo4j point fences.

Cache fills run the original ordered, bounded payload validation. Hits reuse
only immutable counts, never a stale head, live task ownership or event tail.
"""
from collections import Counter, OrderedDict
from copy import deepcopy
import json
import threading
import time

from .catalog import AcceleratedCatalog
from ..query_catalog.service import IncompleteContext, _json
from ..real_scale.store import integer


class EventCountCatalog(AcceleratedCatalog):
    def __init__(self,store,*,event_count_cache_size=32,event_count_cache_bytes=4*1024*1024,**kwargs):
        integer(event_count_cache_size,'event count cache entries',1,256)
        integer(event_count_cache_bytes,'event count cache bytes',1,64*1024*1024)
        super().__init__(store,**kwargs)
        self.event_count_cache_size=event_count_cache_size
        self.event_count_cache_bytes=event_count_cache_bytes
        self._event_counts=OrderedDict()
        self._event_counts_bytes=0
        self._event_counts_lock=threading.RLock()
        # Fixed stripes bound single-flight state even under many distinct keys.
        self._event_count_stripes=[threading.Lock() for _ in range(16)]

    def _event_kind_counts(self,run,seq,start,limit,fleet,*,robot=None,task=None):
        begun=time.perf_counter()
        # Keep a fresh head/creation/source/checksum proof on every hit, including
        # when fleet fragment caching is disabled. No old-head fallback/retry.
        fence,token=self._fence(run,seq,fleet['token'])
        if (fence['step']!=fleet['step'] or _json(fence['metadata'])!=fleet['metadata']
                or _json(fence['config'])!=fleet['config']):
            raise ValueError('event-count source identity changed within pack')
        self._metrics['event_count_guard_ms']=(time.perf_counter()-begun)*1000
        key=(run,seq,start,*token,robot,task)

        def lookup():
            with self._event_counts_lock:
                item=self._event_counts.get(key)
                if item is not None:self._event_counts.move_to_end(key)
                return item

        item=lookup()
        hit=item is not None
        if item is None:
            with self._event_count_stripes[hash(key)%len(self._event_count_stripes)]:
                item=lookup()
                hit=item is not None
                if item is None:
                    values=self._events(run,seq,start,limit,token,robot=robot,task=task)
                    if any(e['event']['sim_time']>fleet['step']['sim_time'] for e in values):
                        raise ValueError('future event effective time')
                    counts=dict(Counter(e['event']['kind'] for e in values))
                    encoded=json.dumps(counts,ensure_ascii=False,allow_nan=False).encode('utf-8')
                    item={'counts':counts,'events':len(values),
                          'max_sim_time':max((e['event']['sim_time'] for e in values),default=None),
                          'metadata':deepcopy(fence['metadata']),'config':deepcopy(fence['config']),
                          'step':deepcopy(fence['step'])}
                    # Bound serialized key/counts AND identity payloads, which can
                    # include a large scene. Large entries remain uncached.
                    size=len(encoded)+len(json.dumps({k:v for k,v in item.items() if k!='counts'},
                        ensure_ascii=False,allow_nan=False).encode('utf-8'))+len(repr(key).encode('utf-8'))
                    if size<=self.event_count_cache_bytes:
                        item['bytes']=size
                        with self._event_counts_lock:
                            self._event_counts[key]=item
                            self._event_counts_bytes+=size
                            while (len(self._event_counts)>self.event_count_cache_size
                                   or self._event_counts_bytes>self.event_count_cache_bytes):
                                _,old=self._event_counts.popitem(last=False)
                                self._event_counts_bytes-=old['bytes']
                    else:self._metrics['event_count_cache_skip']='entry_exceeds_byte_budget'
        if (item['metadata']!=fence['metadata'] or item['config']!=fence['config']
                or item['step']!=fence['step']):
            raise ValueError('immutable event-count source identity changed')
        if item['events']>limit:
            raise IncompleteContext('event window exceeds event_limit; narrow start_seq or explicitly raise budget')
        if item['max_sim_time'] is not None and item['max_sim_time']>fleet['step']['sim_time']:
            raise ValueError('future event effective time')
        self._metrics.update(event_count_cache_hit=hit,event_count_source='original-validated-immutable-window',
                             event_count_events=item['events'])
        # Caller receives a private shallow copy; the base public return still
        # performs the original complete JSON roundtrip.
        return dict(item['counts'])
