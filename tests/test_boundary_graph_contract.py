"""Immutable logical plans, cutoff-scoped actual ports and strict source audit."""
from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from backend.boundary_contract import VERSION, validate_task, validate_binding_history
from backend.resource_graph.model import build_package, materialize_task_records
from backend.resource_graph.writer import apply_resources, LABEL_BOUNDARY_GROUPS
from backend.wide.graph import WideMirror, _locations
from test_resource_graph import first_batch, Tx

ROOT = Path(__file__).resolve().parents[1]


def fixture():
    batch = first_batch(); batch['sim_time'] = batch['tick'] = 0
    batch['metadata']['boundary_routing'] = {'version':VERSION,
        'logical_inbound':'INBOUND', 'logical_outbound':'OUTBOUND'}
    for task in batch['tasks']:
        task.update(plan_source_id=task['source_id'], plan_target_id=task['target_id'],
            resolved_source_id=task['source_id'], resolved_target_id=task['target_id'],
            boundary_bindings={}, source_bound_at=None, target_bound_at=None,
            status='waiting', release_second=0)
    out, incoming = batch['tasks']
    out.update(plan_target_id='OUTBOUND', target_bound_at=0)
    out['boundary_bindings']['target'] = {'logical_id':'OUTBOUND','actual_id':'OUT-1',
        'at':0,'phase':'release','event_seq':1,'event_id':'fixture-run:event:1'}
    incoming.update(plan_source_id='INBOUND', source_id='INBOUND', resolved_source_id=None)
    batch['events'] = [{'kind':'boundary_port_bound','task_id':'T1','endpoint':'target',
        'planned_location_id':'OUTBOUND','actual_location_id':'OUT-1',
        'logical_id':'OUTBOUND','actual_id':'OUT-1','binding_phase':'release',
        'event_seq':1,'event_id':'fixture-run:event:1','sim_time':0},
        {'kind':'task_released','task_id':'T1','event_seq':2,'sim_time':0}]
    return batch


class BoundaryGraphTests(unittest.TestCase):
    def test_static_plan_and_logical_group_have_no_fake_coordinates(self):
        batch=fixture();package=build_package(batch)
        groups=[p for p in package['locations'] if p['kind']=='logical-boundary']
        self.assertEqual({p['location_id'] for p in groups},{'INBOUND','OUTBOUND'})
        self.assertTrue(all('x' not in p and 'y' not in p and not p['physical'] for p in groups))
        self.assertEqual([(p['source_id'],p['target_id']) for p in package['tasks']],
            [('R01-L1-C1-F1','OUTBOUND'),('INBOUND','R02-L1-C1-F1')])

    def test_same_plan_across_runs_never_inherits_the_last_actual_out_port(self):
        a=fixture();a['metadata']['scene']['stations'].append({'id':'OUT-2','kind':'outbound','x':35,'y':0})
        b=deepcopy(a);b['run_id']='second-run';task=b['tasks'][0]
        task.update(target_id='OUT-2', resolved_target_id='OUT-2')
        task['boundary_bindings']['target']['actual_id']='OUT-2'
        pa,pb=build_package(a),build_package(b)
        self.assertEqual(pa['dataset']['id'],pb['dataset']['id'])
        self.assertEqual(pa['tasks'],pb['tasks'])

    def test_incremental_projection_keeps_plan_after_actual_in_port_is_bound(self):
        batch=fixture();package=build_package(batch);task=batch['tasks'][1]
        task.update(source_id='IN-1',resolved_source_id='IN-1',source_bound_at=0,
            received_at=0,status='accepted')
        task['boundary_bindings']['source']={'logical_id':'INBOUND','actual_id':'IN-1',
            'at':0,'phase':'admission','event_seq':3,'event_id':'fixture-run:event:3'}
        batch['seq']=2
        p=materialize_task_records(batch,package['warehouse']['id'],package['dataset']['id'])
        self.assertEqual(p['tasks'][1]['source_id'],'INBOUND')
        self.assertTrue(p['tasks'][1]['source_node_id'].endswith(':location:INBOUND'))

    def test_logical_task_requires_explicit_declaration_and_immutable_plan(self):
        batch=fixture();task=batch['tasks'][1]
        with self.assertRaises(ValueError):validate_task(task,{'scene':batch['metadata']['scene']})
        task.pop('plan_source_id')
        with self.assertRaises(ValueError):validate_task(task,batch['metadata'])

    def test_wrong_role_future_time_or_mutated_binding_are_rejected(self):
        batch=fixture()
        for mutate in (lambda t:t.update(target_id='IN-1',resolved_target_id='IN-1'),
                       lambda t:t.update(target_bound_at=1),
                       lambda t:t['boundary_bindings']['target'].update(actual_id='OUT-2')):
            task=deepcopy(batch['tasks'][0]);mutate(task)
            with self.assertRaises(ValueError):validate_task(task,batch['metadata'],cutoff=0)

    def test_published_outbound_cannot_remain_unbound(self):
        batch=fixture();task=batch['tasks'][0]
        task.update(target_id='OUTBOUND',resolved_target_id=None,target_bound_at=None,boundary_bindings={})
        with self.assertRaises(ValueError):validate_task(task,batch['metadata'])

    def test_source_binding_has_exactly_one_before_lifecycle_event(self):
        batch=fixture();tasks={t['task_id']:t for t in batch['tasks']}
        self.assertEqual(validate_binding_history(tasks,batch['events'],batch['metadata']),
            {'bindings':1,'logical_tasks':2})
        for mutate in (lambda e:e.append(deepcopy(e[0])),lambda e:e.pop(0),
                       lambda e:e[0].update(event_seq=3),
                       lambda e:e[0].update(goods_id='invented')):
            events=deepcopy(batch['events']);mutate(events)
            with self.assertRaises(ValueError):validate_binding_history(tasks,events,batch['metadata'])

    def test_declared_bootstrap_labels_groups_and_legacy_bootstrap_is_unchanged(self):
        tx=Tx();apply_resources(tx,[fixture()])
        self.assertIn(LABEL_BOUNDARY_GROUPS,[q for q,_ in tx.calls])
        tx=Tx();apply_resources(tx,[first_batch()])
        self.assertNotIn(LABEL_BOUNDARY_GROUPS,[q for q,_ in tx.calls])

    def test_run_locations_are_logical_descriptions_not_custody(self):
        scene=json.loads((ROOT/'data/layouts/expansion-candidates/balanced/layout.json').read_text(encoding='utf-8'))
        rows=_locations('run',scene,[],fixture()['metadata'])
        groups=[r for r in rows if r['kind']=='logical-boundary']
        self.assertEqual(len(groups),2)
        self.assertTrue(all('x' not in r and 'y' not in r for r in groups))

    def test_writer_plan_edges_and_actual_version_edges_are_separate(self):
        batch=fixture();batch['metadata']['scene']=json.loads((ROOT/'data/layouts/expansion-candidates/balanced/layout.json').read_text(encoding='utf-8'))
        batch['metadata']['domain']='warehouse-wide-inbound-outbound'
        batch.update(config={'chargers':100},robots=[],slots=[])
        tx=Tx()
        with patch('backend.wide.graph.Neo4jMirror._apply'):
            WideMirror._apply(tx,[batch])
        old=next(p['rows'] for q,p in tx.calls if 'FROM_LOCATION' in q)
        self.assertEqual(old[1]['source'],'fixture-run:INBOUND')
        actual=next(p['rows'] for q,p in tx.calls if 'EXECUTION_TARGET' in q)
        self.assertEqual(actual[0]['location_key'],'fixture-run:OUT-1')
        self.assertFalse(any(r['location_key'].endswith(':INBOUND') for q,p in tx.calls
            if 'EXECUTION_SOURCE' in q for r in p['rows']))

    def test_fake_logical_custody_fails_before_parent_write(self):
        batch=fixture();batch['metadata']['scene']=json.loads((ROOT/'data/layouts/expansion-candidates/balanced/layout.json').read_text(encoding='utf-8'))
        batch['metadata']['domain']='warehouse-wide-inbound-outbound'
        batch.update(config={'chargers':100},robots=[],slots=[])
        batch['events'].append({'kind':'received','goods_id':'G2','location_id':'INBOUND'})
        with patch('backend.wide.graph.Neo4jMirror._apply') as parent:
            with self.assertRaises(ValueError):WideMirror._apply(Tx(),[batch])
            parent.assert_not_called()


if __name__=='__main__':unittest.main()
