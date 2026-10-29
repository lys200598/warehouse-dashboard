"""Verdict logic of the joint acceptance protocol (synthetic rounds, no live service)."""
import unittest

from ops.joint_acceptance import round_record, verdict


def side(run='r1', durable=10, status='running', nodes=1_200_000):
    return {'run_id': run, 'durable_seq': durable, 'status': status, 'nodes': nodes}


def summary(mean=120., top=400., over=0, count=1000, errors=0):
    return {'status': 'measured', 'run_complete': True, 'groups': {'mixed': {'complete_http_json': {'count': count, 'mean_ms': mean, 'p99_ms': top - 10,
                                                        'max_ms': top, 'over_500ms': over}}},
            'outcomes': {'ok': count, **({'timeout': errors} if errors else {})}}


def rounds(n=3, **kw):
    out = []
    for i in range(1, n + 1):
        before = side(durable=10 * i, nodes=kw.get('nodes', 1_200_000), run=kw.get('run', 'r1'))
        after = side(durable=10 * i + kw.get('advance', 4), run=kw.get('run_after', kw.get('run', 'r1')))
        out.append(round_record(i, before, after, summary(**kw.get('summary', {}))))
    return out


def judge(rs):
    return verdict(rs, min_rounds=3, min_nodes=1_000_000, threshold_ms=500)


class JointAcceptanceVerdictTests(unittest.TestCase):
    def test_three_qualified_rounds_under_mean_threshold_pass(self):
        v = judge(rounds())
        self.assertTrue(v['qualified']); self.assertTrue(v['pass'])
        self.assertTrue(v['additional_all_requests_under_threshold'])

    def test_mean_pass_and_strict_tail_are_reported_separately(self):
        v = judge(rounds(summary={'top': 713., 'over': 24}))
        self.assertTrue(v['pass'])
        self.assertFalse(v['additional_all_requests_under_threshold'])

    def test_mean_over_threshold_fails(self):
        v = judge(rounds(summary={'mean': 620.}))
        self.assertTrue(v['qualified']); self.assertFalse(v['pass'])

    def test_not_qualified_without_scale_writes_rounds_or_same_run(self):
        self.assertIn('below', ' '.join(judge(rounds(nodes=244_666))['not_qualified_reasons']))
        self.assertIn('writer', ' '.join(judge(rounds(advance=0))['not_qualified_reasons']))
        self.assertIn('rounds', ' '.join(judge(rounds(n=2))['not_qualified_reasons']))
        self.assertIn('run changed', ' '.join(judge(rounds(run_after='r2'))['not_qualified_reasons']))
        self.assertIn('not verified', ' '.join(judge(rounds(nodes=None))['not_qualified_reasons']))
        for rs in (rounds(nodes=244_666), rounds(advance=0), rounds(n=2)):
            self.assertFalse(judge(rs)['pass'])

    def test_failed_requests_disqualify(self):
        v = judge(rounds(summary={'errors': 3}))
        self.assertFalse(v['qualified'])

    def test_incomplete_measurement_disqualifies(self):
        rs = rounds()
        rs[1]['run_complete'] = False
        self.assertIn('did not complete', ' '.join(judge(rs)['not_qualified_reasons']))


if __name__ == '__main__':
    unittest.main()
