"""Tool-only task ground-truth reuse; every full response still uses old check.

The original JournalTruth validates the entire source/oracle checksum prefix.
Only immutable decoded task ground truth is cached here, never query responses
or their validation outcome. No Neo4j connections or measured-path hooks.
"""
from functools import lru_cache
from types import MappingProxyType

from ops.verify_catalog_responses import JournalTruth


class CachedJournalTruth(JournalTruth):
    @lru_cache(maxsize=2)
    def all_tasks(self,seq):
        return tuple(MappingProxyType(task) for task in super().all_tasks(seq))

    def close(self):
        self.all_tasks.cache_clear()
        super().close()
