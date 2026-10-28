"""Declared logical boundaries bind once, under unchanged physical admission."""
from collections import Counter
from copy import deepcopy
import csv
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from backend.day1.data import FIELDS, sha
from backend.wide.data import migrate_dataset, validate_dataset
from backend.wide.free_ports import (DECLARATION, bind, choose_inbound,
    endpoints_valid, outbound_loads, release)
from backend.scheduling_v5.sim import CandidateSimulation, Config
from ops.convert_logical_boundaries import convert

ROOT=Path(__file__).resolve().parents[1]
SCENE=ROOT/'data/layouts/warehouse100-wide-v2.json'
EXAMPLE=ROOT/'data/examples/warehouse-v5'


class FreePortTests(unittest.TestCase):
    def setUp(self):
        parent=ROOT/'data/local-check/free-port-tests';parent.mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temp.cleanup)
        self.data=Path(self.temp.name)/'logical'
        convert(SCENE,EXAMPLE,self.data)

    def sim(self, **kwargs):
        return CandidateSimulation(SCENE,self.data,'free-test',
            Config(port_selection_policy='balanced',charging_policy='critical-only',**kwargs),
            policy='port-guard',planner='original-sipp')

    def test_converter_preserves_exact_non_boundary_fields_and_initial_bytes(self):
        with (EXAMPLE/'tasks.csv').open(newline='',encoding='utf-8') as f:a=list(csv.DictReader(f))
        with (self.data/'tasks.csv').open(newline='',encoding='utf-8') as f:
            reader=csv.DictReader(f);self.assertEqual(reader.fieldnames,FIELDS);b=list(reader)
        self.assertEqual(len(a),len(b))
        for old,new in zip(a,b):
            field='source_id' if old['task_type']=='INBOUND' else 'target_id'
            self.assertEqual(new[field],old['task_type'])
            self.assertEqual({k:v for k,v in old.items() if k!=field},{k:v for k,v in new.items() if k!=field})
        self.assertEqual((self.data/'initial_inventory.csv').read_bytes(),(EXAMPLE/'initial_inventory.csv').read_bytes())
        self.assertTrue(validate_dataset(SCENE,self.data)['valid'])
        with self.assertRaises(FileExistsError):convert(SCENE,EXAMPLE,self.data)

    def test_logical_input_requires_both_declaration_and_runtime_optin(self):
        with self.assertRaisesRegex(ValueError,'require port_selection_policy'):
            CandidateSimulation(SCENE,self.data,'fixed',Config())
        manifest=json.loads((self.data/'manifest.json').read_text());manifest.pop('boundary_routing')
        (self.data/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'invalid inbound|invalid outbound'):
            validate_dataset(SCENE,self.data)
        with self.assertRaises(ValueError):Config(port_selection_policy='anywhere')

    def test_unreleased_stays_unbound_and_published_outbound_binds_once(self):
        sim=self.sim();self.assertEqual(sim.checkpoint()['tasks'],{})
        self.assertFalse(any(e['kind']=='boundary_port_bound' for e in sim.pending_events))
        outbound=next(t for t in sim.tasks.values() if t['task_type']=='OUTBOUND')
        sim.elapsed=sim.tick=outbound['release_second'];sim._release()
        self.assertTrue(outbound['target_id'].startswith('OUT-'))
        chosen=outbound['target_id'];before=deepcopy(outbound)
        sim._release();self.assertEqual(outbound,before)
        self.assertEqual(outbound['plan_target_id'],'OUTBOUND')
        self.assertEqual(outbound['resolved_target_id'],chosen)
        self.assertEqual(outbound['boundary_bindings']['target']['at'],sim.elapsed)
        with self.assertRaisesRegex(AssertionError,'exactly once'):
            bind(sim,outbound,'target','OUT-2','release')
        events=[e for e in sim.pending_events if e['task_id']==outbound['task_id']]
        self.assertLess(next(i for i,e in enumerate(events) if e['kind']=='boundary_port_bound'),
                        next(i for i,e in enumerate(events) if e['kind']=='task_released'))

    def test_full_ports_do_not_admit_or_bind_and_no_logical_holder_is_invented(self):
        sim=self.sim();task=next(t for t in sim.tasks.values() if t['task_type']=='INBOUND')
        sim.elapsed=sim.tick=task['release_second']
        sim.ports.update({f'IN-{i}':6 for i in range(1,9)})
        sim._release()
        self.assertEqual(task['source_id'],'INBOUND');self.assertEqual(task['status'],'waiting')
        self.assertNotIn(task['goods_id'],sim.goods)
        self.assertIsNone(task['resolved_source_id']);self.assertEqual(task['boundary_bindings'],{})
        sim.ports['IN-7']=5;sim._release()
        self.assertEqual(task['source_id'],'IN-7');self.assertEqual(task['status'],'accepted')
        self.assertEqual(sim.goods[task['goods_id']],'IN-7');self.assertEqual(sim.ports['IN-7'],6)
        self.assertEqual(task['source_bound_at'],task['received_at'])
        self.assertFalse(any('goods_id' in e or 'location_id' in e for e in sim.pending_events if e['kind']=='boundary_port_bound'))

    def test_fixed_rack_and_port_bindings_survive_restore_and_cannot_be_changed(self):
        sim=self.sim()
        first_in=min(t['release_second'] for t in sim.tasks.values() if t['task_type']=='INBOUND')
        for _ in range(first_in+20):sim.step()
        state=sim.checkpoint();other=self.sim();other.restore(state)
        self.assertEqual(other.tasks,sim.tasks);self.assertEqual(other.goods,sim.goods)
        for _ in range(20):sim.step();other.step()
        self.assertEqual(other.tasks,sim.tasks);self.assertEqual(other.robots,sim.robots)
        task=next(t for t in other.tasks.values() if t.get('source_bound_at') is not None)
        task['source_id']='IN-8' if task['source_id']!='IN-8' else 'IN-7'
        with self.assertRaises(AssertionError):other.assert_inventory()

    def test_complete_actual_sample_keeps_cargo_and_station_capacity(self):
        sim=self.sim()
        while sim.elapsed<6000 and (sim.cursor<len(sim.tasks) or sim.metrics['completed']<len(sim.tasks)):
            sim.step()
        self.assertEqual(sim.metrics['completed'],19)
        sim.assert_inventory()
        for task in sim.tasks.values():
            self.assertEqual(task['plan_source_id'],'INBOUND' if task['task_type']=='INBOUND' else task['source_id'])
            self.assertEqual(task['plan_target_id'],'OUTBOUND' if task['task_type']=='OUTBOUND' else task['target_id'])
            self.assertIn(task['resolved_source_id'],sim.nav.locations)
            self.assertIn(task['resolved_target_id'],sim.nav.locations)
        binds=[e for e in sim.pending_events if e['kind']=='boundary_port_bound']
        self.assertEqual(len(binds),19)
        self.assertEqual(len({e['task_id'] for e in binds}),19)
        self.assertNotIn('INBOUND',sim.goods.values())

    def test_endpoint_gate_only_permits_markers_before_binding_boundary(self):
        inbound={'task_type':'INBOUND','status':'waiting','source_id':'INBOUND','target_id':'S',
                 'plan_source_id':'INBOUND','resolved_source_id':None}
        self.assertTrue(endpoints_valid(inbound,{'IN-1'},{'OUT-1'},{'S':{}}))
        self.assertFalse(endpoints_valid({**inbound,'status':'accepted'},{'IN-1'},{'OUT-1'},{'S':{}}))
        out={'task_type':'OUTBOUND','status':'scheduled','source_id':'S','target_id':'OUTBOUND',
             'plan_target_id':'OUTBOUND','resolved_target_id':None}
        self.assertTrue(endpoints_valid(out,{'IN-1'},{'OUT-1'},{'S':{}}))
        self.assertFalse(endpoints_valid({**out,'status':'waiting'},{'IN-1'},{'OUT-1'},{'S':{}}))

    def test_logical_map_migration_preserves_deadlines_and_cannot_invent_reference_endpoint(self):
        target=Path(self.temp.name)/'migrated'
        new_scene=ROOT/'data/layouts/expansion-candidates/balanced/layout.json'
        migrate_dataset(SCENE,new_scene,self.data,target)
        self.assertEqual((target/'tasks.csv').read_bytes(),(self.data/'tasks.csv').read_bytes())
        self.assertTrue(validate_dataset(new_scene,target)['valid'])
        rejected=Path(self.temp.name)/'reject-reference'
        with self.assertRaisesRegex(ValueError,'must preserve deadlines'):
            migrate_dataset(SCENE,new_scene,self.data,rejected,deadline_policy='explicit-reference')
        self.assertFalse(rejected.exists())

    def test_outbound_choice_load_does_not_read_unpublished_endpoint(self):
        class Future(dict):
            def __getitem__(self,key):
                if key!='status':raise AssertionError('read future routing plan')
                return 'scheduled'
        sim=SimpleNamespace(tasks={'future':Future(),
            'active':{'status':'assigned','task_type':'OUTBOUND','target_id':'OUT-2'},
            'done':{'status':'completed'}})
        self.assertEqual(outbound_loads(sim),{'OUT-2':1})

    def test_idle_release_never_scans_complete_history(self):
        class Unread:
            def values(self):raise AssertionError('idle tick scanned task history')
        sim=SimpleNamespace(tasks=Unread(),cursor=0,release_order=[],
                            _inbound_admission_order=lambda:[])
        release(sim)

    def test_full_ports_skip_robot_scoring_without_changing_capacity(self):
        sim=SimpleNamespace(scene={'stations':[{'id':'IN-1','kind':'inbound'}]},
                            ports={'IN-1':6})
        # No robots/config are supplied: none should be read when every port is full.
        self.assertIsNone(choose_inbound(sim,{}))


if __name__=='__main__':unittest.main()
