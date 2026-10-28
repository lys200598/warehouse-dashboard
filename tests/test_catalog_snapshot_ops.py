from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location('catalog_counts_probe',
    Path(__file__).resolve().parents[1] / 'ops/probe_catalog_task_counts.py')
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class ProbeSupportTests(unittest.TestCase):
    def test_complete_source_aggregation(self):
        rows = [{'slot_id': 'R01-C1', 'status': 'waiting', 'due_second': 9},
                {'slot_id': 'R01-C2', 'status': 'waiting', 'due_second': 10},
                {'slot_id': 'R01-C3', 'status': 'completed', 'due_second': 1},
                {'slot_id': 'R02-C1', 'status': 'assigned', 'due_second': 3}]
        before = deepcopy(rows)
        result = MOD.source_counts(rows, [{'id': 'R01', 'zone': 'A'}, {'id': 'R02', 'zone': 'B'}], 10)
        self.assertEqual(result, [{'zone': 'A', 'status': 'completed', 'tasks': 1, 'overdue': 0},
                                  {'zone': 'A', 'status': 'waiting', 'tasks': 2, 'overdue': 1},
                                  {'zone': 'B', 'status': 'assigned', 'tasks': 1, 'overdue': 1}])
        self.assertEqual(rows, before)

    def test_unmapped_slot_is_preserved(self):
        self.assertEqual(MOD.source_counts([{'slot_id': 'MISSING-X', 'status': 'waiting', 'due_second': 0}], [], 1),
                         [{'zone': 'unmapped', 'status': 'waiting', 'tasks': 1, 'overdue': 1}])

    def test_empty_source(self):
        self.assertEqual(MOD.source_counts([], [], 99), [])

    def test_count_order_is_not_semantics(self):
        rows = [{'zone': 'B', 'status': 'waiting'}, {'zone': 'A', 'status': 'completed'}]
        self.assertEqual(MOD.ordered_counts(rows)[0]['zone'], 'A')
        self.assertEqual(rows[0]['zone'], 'B')

    def test_per_task_prototype_changes_only_preaggregation(self):
        self.assertEqual(MOD.PREAGGREGATE_QUERY.replace(MOD._PREAGGREGATED_SLOT_JOIN, MOD._ORIGINAL_SLOT_JOIN),
                         MOD.TASK_COUNTS_QUERY)
        self.assertIn('WHERE v.seq <= $seq', MOD.PREAGGREGATE_QUERY)

    def test_stream_membership_and_cutoff_remain_explicit(self):
        self.assertIn('v.seq <= $seq', MOD.STREAM_QUERY)
        self.assertIn("MATCH (t:WHTask {key:$run+':'+task_id})-[:IN_RUN]->(r)", MOD.STREAM_QUERY)
        self.assertIn('head(collect(v))', MOD.STREAM_QUERY)


if __name__ == '__main__':
    unittest.main()
