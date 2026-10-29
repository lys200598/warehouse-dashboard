from copy import deepcopy
from unittest.mock import patch
import unittest

from ops.query_ab_validation import CachedJournalTruth
from ops.verify_catalog_responses import JournalTruth,ResponseVerifier
from backend.query_catalog.service import QueryCatalogService
from tests.test_catalog_verifier import Store,Source,RUN,SEQ


class TruthCacheTests(unittest.TestCase):
    def test_task_cache_is_bounded_and_top_level_read_only(self):
        truth=CachedJournalTruth.__new__(CachedJournalTruth)
        with patch.object(JournalTruth,'all_tasks',return_value=[{'task_id':'x','status':'waiting'}]) as original:
            first=truth.all_tasks(1);self.assertEqual(first,truth.all_tasks(1))
            self.assertEqual(original.call_count,1)
            with self.assertRaises(TypeError):first[0]['status']='completed'
            truth.all_tasks(2);truth.all_tasks(3);truth.all_tasks(1)
            self.assertEqual(original.call_count,4)
        truth.all_tasks.cache_clear()

    def test_full_response_comparison_still_detects_tamper_on_cache_hit(self):
        store=Store();source=Source(store);truth=CachedJournalTruth.__new__(CachedJournalTruth)
        original_tasks=source.all_tasks
        with patch.object(JournalTruth,'all_tasks',side_effect=lambda seq:original_tasks(seq)) as original:
            source.all_tasks=truth.all_tasks
            verifier=ResponseVerifier(source)
            request={'query_id':'global','run_id':RUN,'seq':SEQ,'robot_id':1}
            response=QueryCatalogService(store).execute(request)
            self.assertEqual(verifier.check(request,response),[])
            self.assertEqual(verifier.check(request,response),[])
            self.assertEqual(original.call_count,1)
            for mutate in (
                lambda result:result['task_counts'][0].update(tasks=12345),
                lambda result:result['recent_event_kind_counts'].update(picked=12345),
                lambda result:result['point'].update(batch_checksum='b'*64),
                lambda result:result['robots'][0].update(battery=0.),
            ):
                bad=deepcopy(response);mutate(bad['result'])
                self.assertTrue(verifier.check(request,bad))
            with self.assertRaises(ValueError):verifier.check({**request,'robot_id':True},response)
            self.assertEqual(original.call_count,1)
        truth.all_tasks.cache_clear()


if __name__=='__main__':unittest.main()
