"""Small opt-in candidate: reuse immutable header JSON encoding, not responses.

All dynamic graph reads, ANN, history, fields and the final COMPLETE JSON decode
remain unchanged. Only metadata/config JSON encoding is reused across requests
at an exact immutable run version. The frozen service and benchmark are intact.
"""
from collections import OrderedDict
import json
import threading
import time

from .service import IDENTITY_FIELDS, QueryService


class FragmentQueryService(QueryService):
    def __init__(self, mirror, timeout=15, *, header_cache_size=32):
        super().__init__(mirror, timeout, header_cache_size=header_cache_size)
        self._fragment_headers = OrderedDict()
        self._fragment_lock = threading.Lock()

    def _begin(self):
        super()._begin()
        self._local.fragment_header = None
        self._local.metrics.update(fragment_cache_hit=False, fragment_encoding_ms=0.,
            fragment_static_json_chars=0, fragment_encoding_enabled=False)

    def clear_header_cache(self):
        super().clear_header_cache()
        with self._fragment_lock:
            self._fragment_headers.clear()

    def _header(self, run, row, known):
        metadata, config, marker = super()._header(run, row, known)
        created = row.get('run_created_at')
        if not self.header_cache_size or not isinstance(created, str) or not created:
            # Explicitly disabling header caching disables fragments too. A run
            # without an immutable creation version cannot safely reuse either.
            return metadata, config, marker
        started = time.perf_counter()
        version = json.dumps([run, created, *(marker[field] for field in IDENTITY_FIELDS)],
                             ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        with self._fragment_lock:
            entry = self._fragment_headers.get(version)
            # Requiring the SAME private source objects avoids silently reusing
            # a fragment after a cold header refresh created new parsed objects.
            # Caller-visible results never expose these private cached objects.
            hit = (entry is not None and entry['metadata'] is metadata and entry['config'] is config)
            if not hit:
                encoded_metadata = json.dumps(metadata, ensure_ascii=False,
                    allow_nan=False, separators=(',', ':'))
                encoded_config = json.dumps(config, ensure_ascii=False,
                    allow_nan=False, separators=(',', ':'))
                entry = {'run': run, 'metadata': metadata, 'config': config,
                    'metadata_json': encoded_metadata, 'config_json': encoded_config}
                # Version replacement invalidates previous fragments of this run.
                for old in [key for key, value in self._fragment_headers.items()
                            if value['run']==run and key!=version]:
                    del self._fragment_headers[old]
                self._fragment_headers[version] = entry
            self._fragment_headers.move_to_end(version)
            while len(self._fragment_headers)>self.header_cache_size:
                self._fragment_headers.popitem(last=False)
        self._local.fragment_header = entry
        self._local.metrics.update(fragment_cache_hit=hit,
            fragment_encoding_ms=(time.perf_counter()-started)*1000,
            fragment_static_json_chars=len(entry['metadata_json'])+len(entry['config_json']),
            fragment_encoding_enabled=True)
        return metadata, config, marker

    def _finish(self, result, assembly_started):
        entry = getattr(self._local, 'fragment_header', None)
        if (entry is None or result.get('metadata') is not entry['metadata'] or
                result.get('config') is not entry['config']):
            # Lean business methods do not return a header, and disabled/unknown
            # versions retain the original full encoder without special cases.
            return super()._finish(result, assembly_started)
        self._local.metrics['decode_assemble_ms'] = (time.perf_counter()-assembly_started)*1000
        started = time.perf_counter()
        dynamic = {key: value for key, value in result.items() if key not in {'metadata', 'config'}}
        encoded_dynamic = json.dumps(dynamic, ensure_ascii=False, allow_nan=False)
        separator = ', ' if dynamic else ''
        encoded = (encoded_dynamic[:-1]+separator+'"config": '+entry['config_json']+
                   ', "metadata": '+entry['metadata_json']+'}')
        # This is one complete response string, not a lazy proxy or omitted
        # fields. Every returned nested object is newly decoded and isolated.
        output = json.loads(encoded)
        self._local.metrics.update(json_roundtrip_ms=(time.perf_counter()-started)*1000,
                                  returned_json_chars=len(encoded))
        return output
