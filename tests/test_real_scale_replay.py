"""Local SQLite/transaction doubles only; never starts Neo4j or a simulation."""
from argparse import Namespace
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import zlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.knowledge import JournalKnowledgeStore, digest, canonical
from backend.real_scale import VERSION, SOURCE_KIND, VECTOR_KIND
from backend.real_scale.replay import Source, SCOPE, COUNTERS, _Result, connect, run, parser
from tests.test_real_scale_oracle import RUN, checkpoint, events, make_journal, mutate_batch


ROOT = Path(__file__).resolve().parents[1]/'data/local-check'


class Clock:
    def __init__(self):
        self.value, self.sleeps, self.on_sleep = 0., [], None

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.value += seconds
        if self.on_sleep:
            callback, self.on_sleep = self.on_sleep, None
            callback()


class Mirror:
    def __init__(self, clock):
        self.runs, self.clock, self.calls = {}, clock, []
        self.last_counters = None
        self.lose_ack, self.orphans = False, False

    def committed_point(self, run):
        value = self.runs.get(run)
        return None if value is None else {'seq': value['seq'], 'checksum': value['checksum']}

    def apply_many(self, batches):
        self.calls.append(deepcopy(batches))
        run = batches[0]['run_id']
        current = self.runs.setdefault(run, {'seq': 0, 'checksum': None, 'steps': {},
            'metadata': json.dumps(batches[0]['metadata']), 'config': json.dumps(batches[0]['config'])})
        for batch in batches:
            assert batch['run_id'] == run and batch['seq'] == current['seq']+1
            current['seq'], current['checksum'] = batch['seq'], digest(batch)
            current['steps'][batch['seq']] = {'seq': batch['seq'], 'key': f'{run}:{batch["seq"]}', 'checksum': digest(batch)}
        self.clock.value += .05
        self.last_counters = {key: (201*len(batches) if key=='nodes_created' else 0) for key in COUNTERS}
        if self.lose_ack:
            self.lose_ack = False
            raise RuntimeError('deliberately lost ACK after actual commit')

    def query(self, query, params, timeout=15):
        if 'RETURN steps,states' in query:
            return [dict(steps=int(self.orphans), states=int(self.orphans))]
        value = self.runs[params['run']]
        if 'r.metadata_json AS metadata' in query:
            return [dict(metadata=value['metadata'], config=value['config'], version=VERSION,
                source_kind=SOURCE_KIND, source_run=params['run'], vector_kind=VECTOR_KIND,
                dimensions=32, seq=value['seq'], checksum=value['checksum'])]
        if 's.seq AS seq' in query:
            return [deepcopy(value['steps'][seq]) for seq in sorted(value['steps']) if seq<=params['seq']]
        raise AssertionError(query)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        ROOT.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT)
        self.path = Path(self.temp.name)
        self.journal = self.path/'journal.sqlite'
        make_journal(self.journal)
        self.clock = Clock()
        self.mirror = Mirror(self.clock)
        self.index = 0

    def tearDown(self):
        self.temp.cleanup()

    def args(self, **kwargs):
        self.index += 1
        values = dict(journal=self.journal, run_id=RUN, output_dir=self.path/f'evidence-{self.index}',
            batch_size=20, stop_after_seq=None, follow=False, resume=False,
            ensure_schema=False, max_wall_seconds=10., interval_seconds=0.)
        values.update(kwargs)
        return Namespace(**values)

    def execute(self, args):
        return run(args, self.mirror, clock=self.clock, sleep=self.clock.sleep)

    def seed(self, through):
        source = Source(self.journal, RUN)
        try:
            self.mirror.apply_many(source.read(through, 50))
        finally:
            source.close()

    def test_bulk_preserves_exact_batches_source_bytes_and_other_runs(self):
        original = hashlib.sha256(self.journal.read_bytes()).hexdigest()
        self.mirror.runs['unrelated'] = {'do_not_touch': True}
        args = self.args(batch_size=3)
        report = self.execute(args)
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(report['scope'], SCOPE)
        self.assertEqual([len(v) for v in self.mirror.calls], [3, 1])
        self.assertEqual(report['replayed_commits'], 4)
        self.assertEqual(report['graph_head_at_end']['seq'], 4)
        self.assertEqual(self.mirror.runs['unrelated'], {'do_not_touch': True})
        self.assertEqual(hashlib.sha256(self.journal.read_bytes()).hexdigest(), original)
        with sqlite3.connect(self.journal) as db:
            self.assertEqual(db.execute('SELECT mirrored_seq FROM heads').fetchone()[0], 0)
        db.close()
        source = Source(self.journal, RUN)
        try:
            self.assertEqual([b for call in self.mirror.calls for b in call], source.read(4, 50))
            self.assertFalse(source.db.in_transaction)
        finally:
            source.close()

    def test_raw_contains_exact_identity_counters_times_and_scope(self):
        args = self.args(stop_after_seq=3, batch_size=2)
        report = self.execute(args)
        self.assertEqual(report['status'], 'complete')
        rows = [json.loads(line) for line in (args.output_dir/'transactions.jsonl').read_text().splitlines()]
        self.assertEqual([(r['first_seq'], r['last_seq']) for r in rows], [(1, 2), (3, 3)])
        for row in rows:
            self.assertEqual(row['scope'], SCOPE)
            self.assertEqual(row['source_seq'], row['graph_after_seq'])
            self.assertEqual(row['batch_checksum'], self.mirror.runs[RUN]['steps'][row['last_seq']]['checksum'])
            self.assertGreaterEqual(row['end_ns'], row['start_ns'])
            self.assertEqual(row['actual_agent_states_in_source_batches'], 100*row['batch_size'])
            self.assertGreater(row['actual_neo4j_counters']['nodes_created'], 0)
            self.assertFalse(report['live_robot_simulation'])
        self.assertEqual(rows[-1]['source_backlog_commits'], 1)
        self.assertFalse(report['query_latency_measured'])

    def test_existing_graph_requires_resume_and_new_output(self):
        self.seed(2)
        calls = len(self.mirror.calls)
        denied = self.execute(self.args())
        self.assertEqual(denied['status'], 'failed')
        self.assertIn('--resume', denied['error'])
        self.assertEqual(len(self.mirror.calls), calls)
        args = self.args(resume=True)
        restored = self.execute(args)
        self.assertEqual(restored['status'], 'complete')
        self.assertEqual([b['seq'] for b in self.mirror.calls[-1]], [3, 4])
        with self.assertRaises(FileExistsError):
            self.execute(args)
        again = self.execute(self.args(resume=True))
        self.assertEqual(again['status'], 'complete')
        self.assertEqual(again['transactions'], 0)

    def test_source_sha_gap_wrong_run_identity_and_event_namespace_fail_before_write(self):
        changes = [lambda path: mutate_batch(path, 2, lambda b: b['robots'][0].update(x=99), rehash=False),
            lambda path: self._sql(path, 'DELETE FROM commits WHERE seq=2'),
            lambda path: mutate_batch(path, 2, lambda b: b.update(run_id='foreign-run')),
            lambda path: mutate_batch(path, 2, lambda b: b['metadata'].update(scene_sha256='foreign')),
            lambda path: mutate_batch(path, 2, lambda b: b['events'][0].update(event_id='foreign:event:2'))]
        for index, edit in enumerate(changes):
            other = self.path/f'bad-{index}.sqlite'; make_journal(other); edit(other)
            result = self.execute(self.args(journal=other))
            self.assertEqual(result['status'], 'failed', result)
            self.assertEqual(self.mirror.calls, [])

    def _sql(self, path, sql):
        with sqlite3.connect(path) as db:
            db.execute(sql)
        db.close()

    def test_existing_prefix_gaps_and_middle_sha_conflicts_are_not_hidden_by_head(self):
        for kind in ('gap', 'sha', 'metadata', 'head'):
            self.mirror = Mirror(self.clock); self.seed(3)
            value = self.mirror.runs[RUN]
            if kind=='gap': del value['steps'][2]
            elif kind=='sha': value['steps'][2]['checksum'] = 'f'*64
            elif kind=='metadata': value['metadata'] = '{}'
            else: value['checksum'] = 'f'*64
            result = self.execute(self.args(resume=True))
            self.assertEqual(result['status'], 'failed', kind)
            self.assertEqual(len(self.mirror.calls), 1)

    def test_orphan_graph_and_missing_source_run_are_rejected(self):
        self.mirror.orphans = True
        self.assertEqual(self.execute(self.args())['status'], 'failed')
        self.mirror.orphans = False
        self.assertEqual(self.execute(self.args(run_id='missing'))['status'], 'failed')
        self.assertEqual(self.mirror.calls, [])

    def test_lost_ack_records_actual_graph_head_and_resume_uses_it(self):
        self.mirror.lose_ack = True
        args = self.args(batch_size=2)
        failed = self.execute(args)
        self.assertEqual(failed['status'], 'failed')
        self.assertEqual(failed['graph_head_at_end']['seq'], 2)
        row = json.loads((args.output_dir/'transactions.jsonl').read_text().splitlines()[0])
        self.assertFalse(row['ok'])
        self.assertEqual(row['graph_head_after_error']['seq'], 2)
        report = self.execute(self.args(resume=True, batch_size=2))
        self.assertEqual(report['status'], 'complete')
        self.assertEqual([b['seq'] for b in self.mirror.calls[-1]], [3, 4])

    def test_follow_observes_new_commits_without_holding_sqlite_read_transaction(self):
        small = self.path/'growing.sqlite'; make_journal(small, through=2)
        def append():
            store = JournalKnowledgeStore(small)
            try:
                store.append(checkpoint(3), events(3)); store.append(checkpoint(4), events(4))
            finally:
                store.close()
        self.clock.on_sleep = append
        result = self.execute(self.args(journal=small, follow=True, stop_after_seq=4, batch_size=2))
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['source_head_at_start']['seq'], 2)
        self.assertEqual(result['source_head_at_end']['seq'], 4)
        self.assertEqual([len(v) for v in self.mirror.calls], [2, 2])

    def test_follow_wall_bound_and_paced_tail_do_not_skip_or_resample(self):
        result = self.execute(self.args(follow=True, max_wall_seconds=.5))
        self.assertEqual(result['status'], 'wall-time-limit')
        self.assertGreaterEqual(result['wall_seconds'], .5)
        self.assertEqual(result['errors'], 0)
        self.mirror = Mirror(self.clock)
        args = self.args(batch_size=1, interval_seconds=1)
        result = self.execute(args)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['replay_mode'], 'paced_tail')
        self.assertEqual([batch[0]['seq'] for batch in self.mirror.calls], [1, 2, 3, 4])
        rows = [json.loads(v) for v in (args.output_dir/'transactions.jsonl').read_text().splitlines()]
        self.assertEqual([rows[i+1]['scheduled_ns']-rows[i]['scheduled_ns'] for i in range(3)], [1000000000]*3)
        self.assertTrue(all(row['batch_size']==1 for row in rows))

    def test_absolute_pacer_preparation_cost_does_not_accumulate_across_24_commits(self):
        store = JournalKnowledgeStore(self.journal)
        try:
            for seq in range(5, 25): store.append(checkpoint(seq), [])
        finally:
            store.close()
        clock = self.clock
        class PrepMirror(Mirror):
            def __init__(self, clock):
                super().__init__(clock); self.starts = []
            def committed_point(self, run):
                clock.value += .03
                return super().committed_point(run)
            def apply_many(self, batches):
                self.starts.append(clock())
                return super().apply_many(batches)
        self.mirror = PrepMirror(clock)
        old_head, old_read = Source.head, Source.read
        def head(source):
            clock.value += .05
            return old_head(source)
        def read(source, *args):
            clock.value += .04
            return old_read(source, *args)
        with patch.object(Source, 'head', head), patch.object(Source, 'read', read):
            report = self.execute(self.args(batch_size=1, interval_seconds=1, max_wall_seconds=40))
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(len(self.mirror.starts), 24)
        for index, start in enumerate(self.mirror.starts):
            self.assertAlmostEqual(start-self.mirror.starts[0], float(index), places=8)
        self.assertEqual([b[0]['seq'] for b in self.mirror.calls], list(range(1, 25)))

    def test_slow_pacer_catches_up_without_skipping_or_changing_nominal_deadlines(self):
        clock = self.clock
        class SlowMirror(Mirror):
            def apply_many(self, batches):
                super().apply_many(batches)
                clock.value += .75
        self.mirror = SlowMirror(clock)
        args = self.args(batch_size=1, interval_seconds=.5)
        report = self.execute(args)
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(clock.sleeps, [])
        self.assertEqual([b[0]['seq'] for b in self.mirror.calls], [1, 2, 3, 4])
        rows = [json.loads(v) for v in (args.output_dir/'transactions.jsonl').read_text().splitlines()]
        self.assertEqual([rows[i+1]['scheduled_ns']-rows[i]['scheduled_ns'] for i in range(3)], [500000000]*3)

    def test_unavailable_stop_and_invalid_bounds_do_not_write(self):
        result = self.execute(self.args(stop_after_seq=9))
        self.assertEqual(result['status'], 'failed')
        for change in [dict(batch_size=51), dict(batch_size=0), dict(max_wall_seconds=0),
                       dict(interval_seconds=-.1, batch_size=1), dict(interval_seconds=1, batch_size=2)]:
            with self.assertRaises(ValueError): self.execute(self.args(**change))
        self.assertEqual(self.mirror.calls, [])
        parsed = parser().parse_args(['--journal', 'j', '--run-id', RUN, '--output-dir', 'o'])
        self.assertEqual(parsed.batch_size, 20)
        self.assertFalse(parsed.ensure_schema)

    def test_schema_bootstrap_is_opt_in_and_write_counters_are_not_double_counted(self):
        class Driver:
            def verify_connectivity(self): pass
            def close(self): pass
        module = SimpleNamespace(GraphDatabase=SimpleNamespace(driver=lambda *a, **kw: Driver()))
        with patch.dict('sys.modules', {'neo4j': module}), patch.dict('os.environ', {
                'NEO4J_URI': 'bolt://test', 'NEO4J_PASSWORD': 'test-only'}), \
             patch('backend.real_scale.replay.ReplayMirror.ensure_schema') as schema:
            mirror = connect(); schema.assert_not_called(); mirror.close()
            mirror = connect(ensure_schema=True); schema.assert_called_once(); mirror.close()
        summary = SimpleNamespace(counters=SimpleNamespace(**{key: 2 for key in COUNTERS}))
        result = _Result(SimpleNamespace(consume=lambda: summary), {key: 0 for key in COUNTERS})
        result.consume(); result.consume()
        self.assertEqual(result.counters, {key: 2 for key in COUNTERS})

    def _replace_head(self, path, cp, *, keep_checksum=False):
        raw = canonical(cp)
        with sqlite3.connect(path) as db:
            if keep_checksum:
                db.execute('UPDATE heads SET checkpoint=? WHERE run_id=?', (zlib.compress(raw), RUN))
            else:
                db.execute('UPDATE heads SET checksum=?,checkpoint=? WHERE run_id=?',
                           (hashlib.sha256(raw).hexdigest(), zlib.compress(raw), RUN))
        db.close()

    def test_head_cache_reuses_only_identical_verified_blob_and_allows_mirror_watermark(self):
        with patch('backend.real_scale.replay.zlib.decompress', wraps=zlib.decompress) as decode:
            source = Source(self.journal, RUN)
            try:
                initial = decode.call_count
                self.assertEqual(initial, 2)  # one full CP, one first batch
                source.head(); source.head()
                self.assertEqual(decode.call_count, initial)
                self._sql(self.journal, 'UPDATE heads SET mirrored_seq=2')
                self.assertEqual(source.head()['source_mirrored_seq'], 2)
                self.assertEqual(decode.call_count, initial)
                store = JournalKnowledgeStore(self.journal)
                try: store.append(checkpoint(5), [])
                finally: store.close()
                self.assertEqual(source.head()['seq'], 5)
                self.assertGreater(decode.call_count, initial)
                latest = decode.call_count
                source.head()
                self.assertEqual(decode.call_count, latest)
            finally:
                source.close()

    def test_same_head_changed_valid_checksum_or_changed_blob_is_rejected(self):
        for keep_checksum in (False, True):
            path = self.path/f'head-{keep_checksum}.sqlite'; make_journal(path)
            source = Source(path, RUN)
            try:
                changed = checkpoint(4); changed['speed'] = 2.
                self._replace_head(path, changed, keep_checksum=keep_checksum)
                with self.assertRaisesRegex(ValueError, 'same durable sequence'):
                    source.head()
            finally:
                source.close()

    def test_nonobject_metadata_has_clear_value_error(self):
        cp = checkpoint(4); cp['run_metadata'] = None
        self._replace_head(self.journal, cp)
        with self.assertRaisesRegex(ValueError, 'metadata must be an object'):
            Source(self.journal, RUN)


if __name__ == '__main__':
    unittest.main()
