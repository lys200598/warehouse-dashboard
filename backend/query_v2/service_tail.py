"""Opt-in tail candidate: lazy ANN fallbacks in one read and cold-header reuse.

The candidate counts stay EXACTLY 100 -> 1000 -> exact, with the same run/cutoff
filters and full response fields. Guarded UNION branches avoid running fallback
work when the prior stage is complete. No response cache or graph writes.
"""
from collections import OrderedDict
import json
import threading

from .service import IDENTITY_FIELDS, build_full_query
from ..real_scale.store import EXPANDED_QUERY
from .service_fragments import FragmentQueryService


_NODE_PROJECTION = '''node{.key,.run_id,.seq,.robot_id,.real_payload_sha256,
                real_embedding:node.real_embedding,score:score}'''

_FUSED_VECTOR = '''        WITH *,p.real_embedding AS tail_query_vector
        CALL (tail_query_vector) {
            CALL db.index.vector.queryNodes($index,$ann_count,tail_query_vector) YIELD node,score
            WHERE node:WHRealState AND node.run_id=$run AND node.seq<=$seq
            WITH node,score ORDER BY score DESC LIMIT 5
            RETURN collect('''+_NODE_PROJECTION+''') AS initial_similar }
        CALL (tail_query_vector,initial_similar) {
            WITH tail_query_vector,initial_similar WHERE size(initial_similar)>=5
            RETURN initial_similar AS expanded_similar,0 AS expanded_count
            UNION
            WITH tail_query_vector,initial_similar WHERE size(initial_similar)<5
            CALL (tail_query_vector) {
                CALL db.index.vector.queryNodes($index,1000,tail_query_vector) YIELD node,score
                WHERE node:WHRealState AND node.run_id=$run AND node.seq<=$seq
                WITH node,score ORDER BY score DESC LIMIT 5
                RETURN collect('''+_NODE_PROJECTION+''') AS expanded }
            RETURN expanded AS expanded_similar,size(expanded) AS expanded_count }
        WITH *,expanded_similar AS similar,size(initial_similar) AS tail_ann100_eligible,
            expanded_count AS tail_ann1000_eligible,
            CASE WHEN size(initial_similar)>=5 THEN 'ann100'
                 WHEN size(expanded_similar)>=5 THEN 'ann1000_after_underfilled100'
                 ELSE 'exact_after_underfilled1000' END AS tail_retrieval_path
'''


def build_tail_query(mode='ordered'):
    query = build_full_query(mode)
    start = query.index('        CALL (p) {\n            CALL db.index.vector.queryNodes')
    end = query.index('        RETURN r.committed_seq AS head', start)
    query = query[:start]+_FUSED_VECTOR+query[end:]
    return query.replace('goods_rows,coop_rows,similar',
        'goods_rows,coop_rows,similar,tail_retrieval_path,tail_ann100_eligible,tail_ann1000_eligible')


_FUSED_QUERIES = {build_full_query(mode): build_tail_query(mode) for mode in ('baseline', 'ordered')}


class TailQueryService(FragmentQueryService):
    def __init__(self, mirror, timeout=15, *, header_cache_size=32,
                 fuse_vectors=True, singleflight_headers=True):
        if type(fuse_vectors) is not bool or type(singleflight_headers) is not bool:
            raise ValueError('tail feature flags must be actual booleans')
        super().__init__(mirror, timeout, header_cache_size=header_cache_size)
        self.fuse_vectors, self.singleflight_headers = fuse_vectors, singleflight_headers
        self._raw_headers, self._raw_header_lock = OrderedDict(), threading.Lock()

    def _begin(self):
        super()._begin()
        self._local.tail_retrieval_path = None
        self._local.tail_pending_expanded = None
        self._local.metrics.update(ann_fused=False, ann100_eligible=None,
            ann1000_eligible=None, header_parse_reused=False)

    def clear_header_cache(self):
        super().clear_header_cache()
        with self._raw_header_lock:
            self._raw_headers.clear()

    def _read(self, query, params):
        if query==EXPANDED_QUERY and self._local.tail_pending_expanded is not None:
            # The fused first statement already performed exactly this ANN1000
            # fallback. Pass its intermediate rows to the inherited controller
            # WITHOUT issuing it again. Only per-invocation intermediate rows
            # are reused; the exact fallback still uses the original parameter
            # query, which profiled faster than a correlated exact subquery.
            expanded = self._local.tail_pending_expanded
            self._local.tail_pending_expanded = None
            return expanded
        fused = self.fuse_vectors and query in _FUSED_QUERIES
        rows = super()._read(_FUSED_QUERIES[query] if fused else query, params)
        if fused and len(rows)==1:
            row = rows[0]
            path = row.pop('tail_retrieval_path')
            if path not in {'ann100', 'ann1000_after_underfilled100', 'exact_after_underfilled1000'}:
                raise ValueError('unknown fused ANN retrieval path')
            if path=='exact_after_underfilled1000':
                if len(row['similar'])>=5:
                    raise ValueError('inconsistent fused exact fallback result')
                self._local.tail_pending_expanded = row['similar']
            elif len(row['similar'])!=5:
                raise ValueError('fused eligible real vector result remains incomplete')
            self._local.tail_retrieval_path = path
            self._local.metrics.update(ann_fused=True,
                ann100_eligible=row.pop('tail_ann100_eligible'),
                ann1000_eligible=row.pop('tail_ann1000_eligible'))
        return rows

    def _header(self, run, row, known):
        raw_metadata, raw_config, created = row['metadata_json'], row['config_json'], row.get('run_created_at')
        eligible = (self.singleflight_headers and self.header_cache_size and
            isinstance(created, str) and created and
            isinstance(raw_metadata, str) and isinstance(raw_config, str))
        if not eligible:
            return super()._header(run, row, known)
        marker = {field: row[field] for field in IDENTITY_FIELDS}
        version_tuple = (run, created, *(marker[field] for field in IDENTITY_FIELDS))
        version = json.dumps(version_tuple, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        # Cold concurrent requests can all start before the first header is
        # cached. Compare the COMPLETE original JSON strings, then share their
        # already decoded private objects; a changed string is never concealed.
        with self._raw_header_lock:
            entry = self._raw_headers.get(version)
            if entry and entry['raw_metadata']==raw_metadata and entry['raw_config']==raw_config:
                local_row = {**row, 'metadata_json': None, 'config_json': None}
                result = super()._header(run, local_row, entry['known'])
                self._raw_headers.move_to_end(version)
                self._local.metrics.update(header_parse_reused=True, metadata_cache_hit=False,
                    metadata_wire_chars=len(raw_metadata)+len(raw_config))
                return result
            result = super()._header(run, row, known)
            metadata, config, marker = result
            entry = {'run': run, 'raw_metadata': raw_metadata, 'raw_config': raw_config,
                'known': {'version': version_tuple, 'created_at': created, 'marker': marker,
                          'metadata': metadata, 'config': config}}
            for old in [key for key, item in self._raw_headers.items() if item['run']==run and key!=version]:
                del self._raw_headers[old]
            self._raw_headers[version] = entry
            self._raw_headers.move_to_end(version)
            while len(self._raw_headers)>self.header_cache_size:
                self._raw_headers.popitem(last=False)
            return result

    def _finish(self, result, assembly_started):
        path = getattr(self._local, 'tail_retrieval_path', None)
        if path is not None and 'similar' in result:
            result['retrieval_path'] = path
            result['bolt_statements'] = self._local.metrics['bolt_statements']
        return super()._finish(result, assembly_started)
