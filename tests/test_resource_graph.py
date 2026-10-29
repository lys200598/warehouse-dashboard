"""Offline contracts and exact historical custody checks; no Neo4j substitute."""
from copy import deepcopy
import json
import unittest

from backend.resource_graph import (build_package, validate_forward_batches, apply_resources,
    RackInventoryService, SCHEMA_QUERIES, VERSION, RACK_INVENTORY_QUERY)
from backend.resource_graph.writer import (RESOURCE_HEAD, ADVANCE_RESOURCE_HEAD,
    BOOTSTRAP_RACKS, LINK_RACK_SLOTS, BOOTSTRAP_INITIAL)


def first_batch():
    scene = {'spec': {'version': 'fixture', 'levels': 1, 'faces': 2, 'baysPerFace': 1,
                     'width': 40, 'depth': 40}, 'capacity': 4,
        'racks': [{'id': 'R01', 'x': 5, 'y': 5, 'w': 2, 'd': 1, 'capacity': 2, 'zone': 'A'},
                  {'id': 'R02', 'x': 15, 'y': 5, 'w': 2, 'd': 1, 'capacity': 2, 'zone': 'B'}],
        'homes': [], 'docks': [], 'stations': [{'id': 'IN-1', 'kind': 'inbound', 'x': 0, 'y': 0},
                                               {'id': 'OUT-1', 'kind': 'outbound', 'x': 30, 'y': 0}]}
    slots = [{'slot_id': f'R{rack:02}-L1-C1-F{face}', 'x': rack * 5., 'y': face * 2.,
              'occupied': rack == 1 and face == 1, 'cargo_id': 'G1' if rack == 1 and face == 1 else None}
             for rack in (1, 2) for face in (1, 2)]
    return {'run_id': 'fixture-run', 'seq': 1, 'metadata': {'scene': scene,
        'scene_sha256': 'a' * 64, 'tasks_sha256': 'b' * 64,
        'initial_inventory_sha256': 'c' * 64, 'manifest_sha256': 'd' * 64},
        'slots': slots, 'events': [{'kind': 'initial_stock', 'goods_id': 'G1',
                                  'location_id': 'R01-L1-C1-F1'}],
        'tasks': [{'task_id': 'T1', 'task_type': 'OUTBOUND', 'goods_id': 'G1',
                  'source_id': 'R01-L1-C1-F1', 'target_id': 'OUT-1',
                  'release_second': 0, 'due_second': 100},
                 {'task_id': 'T2', 'task_type': 'INBOUND', 'goods_id': 'G2',
                  'source_id': 'IN-1', 'target_id': 'R02-L1-C1-F1',
                  'release_second': 100, 'due_second': 200}]}


class Result:
    def __init__(self, row=None): self.row = row
    def single(self): return self.row
    def consume(self): return None


class Tx:
    def __init__(self, header=None):
        self.header = header or {'resource_seq': None, 'version': None, 'source_sha256': None,
                                 'warehouse_id': None, 'dataset_id': None}
        self.calls = []
    def run(self, query, **parameters):
        self.calls.append((query, deepcopy(parameters)))
        return Result(self.header if query == RESOURCE_HEAD else None)


class ResourceGraphModelTests(unittest.TestCase):
    def test_all_scoped_rack_references_resolve_and_business_id_is_preserved(self):
        p = build_package(first_batch()); ids = {r['id'] for r in p['racks']}
        self.assertTrue(all(r['id'].startswith('warehouse:sha256:') for r in p['racks']))
        self.assertEqual({r['rack_id'] for r in p['racks']}, {'R01', 'R02'})
        self.assertTrue(all(l['rack_node_id'] in ids for l in p['locations'] if l['kind'] == 'slot'))

    def test_dataset_identity_distinguishes_input_hashes_on_same_date(self):
        a = first_batch(); b = deepcopy(a); b['metadata']['tasks_sha256'] = 'e' * 64
        self.assertNotEqual(build_package(a)['dataset']['id'], build_package(b)['dataset']['id'])
        # One physical layout must not acquire the latest dataset's hashes.
        self.assertEqual(build_package(a)['warehouse'], build_package(b)['warehouse'])

    def test_unreleased_tasks_are_not_required_and_do_not_change_file_identity(self):
        a = first_batch(); b = deepcopy(a); b['tasks'] = []
        self.assertEqual(build_package(a)['dataset']['id'], build_package(b)['dataset']['id'])
        self.assertEqual(build_package(b)['tasks'], [])
        c = deepcopy(a); c['metadata']['initial_inventory_sha256'] = 'e' * 64
        self.assertNotEqual(build_package(a)['dataset']['id'], build_package(c)['dataset']['id'])

    def test_same_input_is_shared_across_runs_but_new_coordinates_are_separate(self):
        a = first_batch(); b = deepcopy(a); b['run_id'] = 'new-run'
        self.assertEqual(build_package(a)['warehouse']['id'], build_package(b)['warehouse']['id'])
        self.assertEqual(build_package(a)['dataset']['id'], build_package(b)['dataset']['id'])
        b['metadata']['scene']['racks'][0]['x'] += 1
        self.assertNotEqual(build_package(a)['warehouse']['id'], build_package(b)['warehouse']['id'])

    def test_capacity_and_full_slot_declarations_are_mandatory(self):
        for mutate in (lambda b: b['slots'].pop(),
                       lambda b: b['slots'].append(deepcopy(b['slots'][0])),
                       lambda b: b['metadata']['scene'].update(capacity=9),
                       lambda b: b['metadata']['scene']['racks'][0].update(capacity=8)):
            b = first_batch(); mutate(b)
            with self.assertRaises(ValueError): build_package(b)

    def test_foreign_task_endpoints_and_initial_stock_are_rejected(self):
        a = first_batch(); a['tasks'][0]['target_id'] = 'foreign'
        with self.assertRaises(ValueError): build_package(a)
        a = first_batch(); a['events'][0]['location_id'] = 'ROBOT-1'
        with self.assertRaises(ValueError): build_package(a)

    def test_seq_gap_source_mutation_and_mixed_run_are_rejected_before_parent_write(self):
        a = first_batch(); b = deepcopy(a); b['seq'] = 2
        self.assertIsNotNone(validate_forward_batches([a, b]))
        b['run_id'] = 'foreign'
        with self.assertRaises(ValueError): validate_forward_batches([a, b])
        b = deepcopy(a); b['seq'] = 3
        with self.assertRaises(ValueError): validate_forward_batches([a, b])
        b['seq'] = 2; b['metadata']['manifest_sha256'] = 'f' * 64
        with self.assertRaises(ValueError): validate_forward_batches([a, b])


class ResourceGraphWriterTests(unittest.TestCase):
    def test_bootstrap_uses_same_transaction_and_complete_resource_links(self):
        a = first_batch(); tx = Tx(); apply_resources(tx, [a])
        racks = next(p['rows'] for q, p in tx.calls if q == BOOTSTRAP_RACKS)
        links = next(p['rows'] for q, p in tx.calls if q == LINK_RACK_SLOTS)
        self.assertEqual(len(links), 4)
        self.assertTrue(all(l['rack_node_id'] in {r['id'] for r in racks} for l in links))
        final = tx.calls[-1]; self.assertEqual(final[0], ADVANCE_RESOURCE_HEAD)
        self.assertEqual(final[1]['seq'], 1); self.assertEqual(final[1]['version'], VERSION)
        self.assertIn(BOOTSTRAP_INITIAL, [q for q, _ in tx.calls])

    def test_later_commit_only_advances_resource_watermark_no_rebuild(self):
        a = first_batch(); initial = Tx(); apply_resources(initial, [a]); args = initial.calls[-1][1]
        tx = Tx({'resource_seq': 1, 'version': VERSION, 'source_sha256': args['source_sha256'],
                 'warehouse_id': args['warehouse_id'], 'dataset_id': args['dataset_id']})
        a['seq'] = 2; a['slots'] = []; a['tasks'] = []; a['events'] = []
        apply_resources(tx, [a])
        self.assertEqual([q for q, _ in tx.calls], [RESOURCE_HEAD, ADVANCE_RESOURCE_HEAD])

    def test_existing_run_cannot_be_silently_bootstrapped_from_delta(self):
        a = first_batch(); a['seq'] = 2
        with self.assertRaisesRegex(ValueError, 'seq1'): apply_resources(Tx(), [a])

    def test_later_task_publication_projects_goods_and_tasks_without_resource_rebuild(self):
        from backend.resource_graph.writer import BOOTSTRAP_GOODS, BOOTSTRAP_TASKS
        a = first_batch(); initial = Tx(); apply_resources(initial, [a]); args = initial.calls[-1][1]
        tx = Tx({'resource_seq': 1, 'version': VERSION, 'source_sha256': args['source_sha256'],
                 'warehouse_id': args['warehouse_id'], 'dataset_id': args['dataset_id']})
        a['seq'] = 2; a['slots'] = []; a['events'] = []
        apply_resources(tx, [a])
        queries = [q for q, _ in tx.calls]
        self.assertIn(BOOTSTRAP_GOODS, queries); self.assertIn(BOOTSTRAP_TASKS, queries)
        self.assertNotIn(BOOTSTRAP_RACKS, queries); self.assertNotIn(BOOTSTRAP_INITIAL, queries)

    def test_write_failure_propagates_to_parent_transaction(self):
        class Failure(Tx):
            def run(self, query, **parameters):
                if query == LINK_RACK_SLOTS: raise RuntimeError('transaction failure')
                return super().run(query, **parameters)
        tx = Failure()
        with self.assertRaises(RuntimeError): apply_resources(tx, [first_batch()])
        self.assertNotIn(ADVANCE_RESOURCE_HEAD, [q for q, _ in tx.calls])

    def test_schema_declares_scoped_resources_and_historical_lookup_indexes(self):
        self.assertTrue(all('IF NOT EXISTS' in q for q in SCHEMA_QUERIES))
        self.assertTrue(any('WHSlotState' in q and 'run_id,n.slot_id,n.seq' in q for q in SCHEMA_QUERIES))


def response(seq=1):
    values = []
    for face in (1, 2):
        occupied = seq == 1 and face == 1
        payload = {'slot_id': f'R01-L1-C1-F{face}', 'occupied': occupied,
                   'cargo_id': 'G1' if occupied else None}
        values.append({'location_id': payload['slot_id'], 'level': 1, 'face': face, 'bay': 1,
            'slot_seq': seq, 'slot_payload': json.dumps(payload), 'occupied': occupied,
            'goods_id': payload['cargo_id'], 'custody_location': payload['slot_id'] if occupied else None,
            'custody_seq': seq if occupied else None, 'custody_event_seq': 1 if occupied else None,
            'custody_time': 0 if occupied else None, 'custody_kind': 'initial_stock' if occupied else None})
    return {'run_id': 'fixture-run', 'head': 3, 'resource_head': 3, 'dataset_id': 'dataset:x',
        'warehouse_id': 'warehouse:x', 'rack_id': 'R01', 'capacity': 2,
        'batch_checksum': 'a' * 64, 'sim_time': seq - 1, 'slots': values}


class Mirror:
    def __init__(self, row): self.row, self.calls = row, []
    def query(self, query, params, timeout):
        self.calls.append((query, params, timeout)); return [deepcopy(self.row)]


class RackInventoryTests(unittest.TestCase):
    def test_initial_rack_has_goods_but_after_pickup_does_not_reuse_initial_at(self):
        initial = RackInventoryService(Mirror(response(1))).inventory('fixture-run', 1, 'R01')
        later = RackInventoryService(Mirror(response(2))).inventory('fixture-run', 2, 'R01')
        self.assertEqual(initial['occupied'], 1); self.assertEqual(initial['empty'], 1)
        self.assertEqual(later['occupied'], 0); self.assertEqual(later['empty'], 2)
        self.assertNotIn('INITIAL_AT', RACK_INVENTORY_QUERY)
        self.assertIn('v.seq <= $seq', RACK_INVENTORY_QUERY)
        self.assertIn('g.seq <= $seq', RACK_INVENTORY_QUERY)

    def test_exact_run_seq_and_rack_are_parameters_and_read_has_no_writes(self):
        mirror = Mirror(response()); value = RackInventoryService(mirror).inventory('fixture-run', 1, 'R01')
        self.assertEqual(mirror.calls[0][1]['run'], 'fixture-run')
        self.assertEqual(mirror.calls[0][1]['seq'], 1)
        self.assertEqual(value['batch_checksum'], 'a' * 64)
        for word in ('MERGE ', 'SET ', 'DELETE ', 'CREATE '): self.assertNotIn(word, RACK_INVENTORY_QUERY)

    def test_future_foreign_missing_mapping_and_custody_disagreement_fail_closed(self):
        mutations = [lambda r: r.update(resource_head=0), lambda r: r.update(run_id='foreign'),
            lambda r: r['slots'].pop(), lambda r: r['slots'][0].update(slot_seq=4),
            lambda r: r['slots'][0].update(custody_location='ROBOT-3'),
            lambda r: r['slots'][0].update(slot_payload='{}')]
        for mutate in mutations:
            row = response(); mutate(row)
            with self.subTest(row=row), self.assertRaises(ValueError):
                RackInventoryService(Mirror(row)).inventory('fixture-run', 1, 'R01')

    def test_budget_is_explicit_and_empty_filter_retains_complete_counts(self):
        result = RackInventoryService(Mirror(response())).inventory('fixture-run', 1, 'R01', include_empty=False)
        self.assertEqual(len(result['slots']), 1); self.assertEqual(result['capacity'], 2)
        with self.assertRaises(ValueError): RackInventoryService(Mirror(response())).inventory('fixture-run', 1, 'R01', limit=1)

    def test_cutoff_and_options_are_strict_typed(self):
        service = RackInventoryService(Mirror(response()))
        for seq in (True, 0, -1, 1.2):
            with self.assertRaises(ValueError): service.inventory('fixture-run', seq, 'R01')
        with self.assertRaises(ValueError): service.inventory('fixture-run', 1, 'R01', include_empty=1)


class ActualFirstCommitContractTests(unittest.TestCase):
    def test_real_v5_source_initial_commit_does_not_reveal_unreleased_tasks(self):
        from pathlib import Path
        from backend.real_scale.writer import RealSimulation
        from backend.knowledge import make_batch
        root = Path(__file__).resolve().parents[1]
        sim = RealSimulation(root/'data/layouts/warehouse100-wide-v2.json',
            root/'data/examples/warehouse-v5', 'resource-actual-first-commit')
        self.assertEqual(len(sim.tasks), 19)
        sim.knowledge_seq = 1
        batch = make_batch(sim.checkpoint(), sim.pending_events)
        package = build_package(batch)
        self.assertEqual(len(package['racks']), 36)
        self.assertEqual(len([l for l in package['locations'] if l['kind'] == 'slot']), 1728)
        self.assertEqual(len(package['inventory']), 864)
        self.assertEqual(package['tasks'], [])
        self.assertEqual(sim.elapsed, 0)


if __name__ == '__main__': unittest.main()
