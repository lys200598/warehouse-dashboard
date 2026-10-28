"""Independent miniature source-prefix fixtures; no backend or database service."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zlib

SPEC = importlib.util.spec_from_file_location('source_size', Path(__file__).resolve().parents[1] / 'ops/source_size_at_queries.py')
size = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(size)


def fixture(root):
    path = root / 'journal.sqlite'
    metadata = {'scene': {'docks': [{}] * 100, 'homes': [{}] * 100, 'stations': [{}] * 16}}
    config = {'chargers': 100}
    cp = {'run_id': 'r', 'knowledge_seq': 3, 'event_sequence': 2, 'run_metadata': metadata, 'config': config}
    rows = []
    for seq in range(1, 4):
        value = {'run_id': 'r', 'seq': seq, 'sim_time': seq * 300, 'metadata': metadata, 'config': config,
                 'robots': [{'robot_id': i} for i in range(1, 101)], 'tasks': [], 'slots': [], 'racks': [], 'events': []}
        if seq == 1:
            value.update(tasks=[{'task_id': 't1', 'goods_id': 'g1'}], slots=[{'slot_id': 's1', 'cargo_id': 'g1'}],
                         racks=[{'id': 'rack1', 'zone': 'zone1'}],
                         events=[{'event_seq': 1, 'event_id': 'r:event:1', 'kind': 'initial_stock', 'goods_id': 'g1', 'location_id': 's1'}])
        if seq == 2:
            value.update(tasks=[{'task_id': 't1', 'goods_id': 'g1'}], slots=[{'slot_id': 's1', 'cargo_id': None}],
                         events=[{'event_seq': 2, 'event_id': 'r:event:2', 'kind': 'shipped', 'goods_id': 'g1', 'location_id': 'SHIPPED'}])
        rows.append(value)
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB)')
    db.execute('CREATE TABLE heads(run_id TEXT,seq INTEGER,checksum TEXT,checkpoint BLOB,mirrored_seq INTEGER)')
    for value in rows:
        db.execute('INSERT INTO commits VALUES(?,?,?,?)', ('r', value['seq'], size.digest(value), zlib.compress(size.canonical(value))))
    db.execute('INSERT INTO heads VALUES(?,?,?,?,?)', ('r', 3, size.digest(cp), zlib.compress(size.canonical(cp)), 3))
    db.commit(); db.close()
    return path, rows


def write_requests(root, source, points=(1, 3)):
    requests = root / 'requests.jsonl'; responses = root / 'responses.jsonl'
    indexes, values = [], []
    for number, seq in enumerate(points, 1):
        result = {'run_id': 'r', 'seq': seq, 'graph_committed_seq': 3, 'batch_checksum': source['sizes'][seq]['batch_checksum']}
        indexes.append({'request_id': 'q' + str(number), 'round': number, 'run_id': 'r', 'seq': seq, 'ok': True,
                        'response_sha256': size.digest(result), 'request': {'run_id': 'r', 'seq': seq}})
        values.append({'request_id': 'q' + str(number), 'result': result})
    requests.write_text(''.join(json.dumps(row) + '\n' for row in indexes))
    responses.write_text(''.join(json.dumps(row) + '\n' for row in values))
    return requests, responses


def comparison(root, source):
    value = {'passed': True, 'source_statistics': {'required': {
        'run_id': 'r', 'journal_sha256': source['journal_sha256'], 'prefix': source['prefix'],
        'prefix_sha256': source['prefix_sha256'], 'expected_distinct_run_nodes': source['final_source_nodes'],
        'expected_graph_label_counts': source['expected_graph_label_counts']}},
        'graph_statistics': {'run_id': 'r', 'head': 3, 'distinct_nodes': source['final_source_nodes'],
                             'label_counts': source['expected_graph_label_counts']}}
    path = root / 'comparison.json'; path.write_text(json.dumps(value)); return path


class SourceSizeTests(unittest.TestCase):
    def test_prefixes_do_not_accumulate_static_entities_or_duplicate_labels(self):
        with tempfile.TemporaryDirectory() as folder:
            path, _ = fixture(Path(folder)); result = size.source_prefixes(path)
            self.assertEqual([p['source_nodes'] for p in result['sizes'].values()], [629, 834, 1035])
            self.assertEqual(result['expected_graph_label_counts']['WHRealState'], 300)
            self.assertEqual(result['expected_graph_label_counts']['WHAgentState'], 300)
            self.assertEqual(result['final_source_nodes'], 1035)
            self.assertIsNone(result['first_million_node_prefix'])

    def test_first_crossing_is_bound_to_its_own_source_prefix(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root)
            with patch.object(size, 'MILLION', 1000):
                source = size.source_prefixes(path)
                requests, responses = write_requests(root, source)
                result = size.summarize_requests(source, requests, responses)
            self.assertEqual(source['first_million_node_prefix']['seq'], 3)
            self.assertEqual(result['million_node_requests'], 1)
            self.assertEqual(result['rounds'][0]['minimum_source_nodes'], 629)
            self.assertEqual(result['rounds'][0]['million_node_requests'], 0)
            self.assertEqual(result['rounds'][1]['minimum_source_nodes'], 1035)

    def test_observed_head_does_not_replace_selected_cutoff(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, responses = write_requests(root, source, (1,))
            result = size.summarize_requests(source, requests, responses)
            row = result['request_prefix_sizes'][0]
            self.assertEqual(row['source_nodes'], 629)
            self.assertEqual(row['observed_head_source_nodes'], 1035)

    def test_full_audit_and_original_inputs_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, responses = write_requests(root, source); compared = comparison(root, source)
            before = size.file_sha(path)
            result = size.audit(path, requests, compared, responses)
            self.assertTrue(result['passed']); self.assertEqual(size.file_sha(path), before)

    def test_foreign_request_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, _ = write_requests(root, source)
            records = list(size.read_lines(requests)); records[0][1]['run_id'] = 'other'
            requests.write_text(''.join(json.dumps(row) + '\n' for _, row in records))
            with self.assertRaisesRegex(ValueError, 'another run'):
                size.summarize_requests(source, requests)

    def test_future_cutoff_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, _ = write_requests(root, source)
            records = list(size.read_lines(requests)); records[0][1]['seq'] = 4
            requests.write_text(''.join(json.dumps(row) + '\n' for _, row in records))
            with self.assertRaisesRegex(ValueError, 'request cutoff'):
                size.summarize_requests(source, requests)

    def test_batch_sha_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path, _ = fixture(Path(folder)); db = sqlite3.connect(path)
            db.execute("UPDATE commits SET checksum='corrupt' WHERE seq=2"); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'batch SHA'):
                size.source_prefixes(path)

    def test_missing_commit_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path, _ = fixture(Path(folder)); db = sqlite3.connect(path)
            db.execute('DELETE FROM commits WHERE seq=2'); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'sequence gap'):
                size.source_prefixes(path)

    def test_incomplete_actual_robot_batch_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path, batches = fixture(Path(folder)); value = deepcopy(batches[1]); value['robots'].pop()
            db = sqlite3.connect(path)
            db.execute('UPDATE commits SET checksum=?,batch=? WHERE seq=2', (size.digest(value), zlib.compress(size.canonical(value))))
            db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, '100 actual robots'):
                size.source_prefixes(path)

    def test_response_batch_source_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, responses = write_requests(root, source, (1,))
            index = next(size.read_lines(requests))[1]; value = next(size.read_lines(responses))[1]
            value['result']['batch_checksum'] = 'wrong'; index['response_sha256'] = size.digest(value['result'])
            requests.write_text(json.dumps(index) + '\n'); responses.write_text(json.dumps(value) + '\n')
            with self.assertRaisesRegex(ValueError, 'batch checksum'):
                size.summarize_requests(source, requests, responses)

    def test_final_graph_mismatch_fails_instead_of_rewriting_history(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, responses = write_requests(root, source); compared = comparison(root, source)
            value = json.loads(compared.read_text()); value['graph_statistics']['distinct_nodes'] += 300
            compared.write_text(json.dumps(value)); result = size.audit(path, requests, compared, responses)
            self.assertFalse(result['passed']); self.assertIn('final_graph_distinct_nodes_match', result['failed_checks'])

    def test_duplicate_full_response_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); path, _ = fixture(root); source = size.source_prefixes(path)
            requests, responses = write_requests(root, source, (1,)); responses.write_text(responses.read_text() * 2)
            with self.assertRaisesRegex(ValueError, 'duplicate full response'):
                size.summarize_requests(source, requests, responses)

    def test_duplicate_json_field_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate JSON'):
            size.loads('{"seq":1,"seq":2}')


if __name__ == '__main__':
    unittest.main()
