from collections import Counter
from types import SimpleNamespace
import unittest

from backend.scheduler_diagnostics.trace import pool_order,target_gates


class DiagnosticContractTests(unittest.TestCase):
    def test_pool_preserves_exact_policy_order_and_does_not_mutate(self):
        ready=[dict(task_id='b',due_second=500,ready_at=10),dict(task_id='a',due_second=500,ready_at=10)]
        sim=SimpleNamespace(blocked={},elapsed=20)
        self.assertEqual([t['task_id'] for t in pool_order(sim,ready)],['a','b'])
        self.assertEqual([t['task_id'] for t in ready],['b','a'])

    def test_backoff_flag_outweighs_deadline_as_in_actual_code(self):
        ready=[dict(task_id='urgent',due_second=1,ready_at=0),dict(task_id='other',due_second=100,ready_at=0)]
        sim=SimpleNamespace(blocked={'urgent':{'retry_after':31}},elapsed=30)
        self.assertEqual(pool_order(sim,ready)[0]['task_id'],'other')
        sim.elapsed=31
        self.assertEqual(pool_order(sim,ready)[0]['task_id'],'urgent')

    def test_gate_observer_distinguishes_port_from_slot(self):
        sim=SimpleNamespace(slots={'slot':{'occupied':False,'reservedBy':None}},inside=864,ports=Counter({'IN-1':6}))
        task={'target_id':'slot','source_id':'IN-1','status':'waiting'}
        value=target_gates(sim,task)
        self.assertTrue(value['port_full'])
        self.assertFalse(value['target_occupied'])
        self.assertFalse(value['capacity_full'])
        self.assertEqual(sim.ports['IN-1'],6)


if __name__=='__main__':unittest.main()
