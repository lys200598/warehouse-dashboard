"""Synthetic audit-format fixtures; not physical or latency qualification."""
from copy import deepcopy
import csv
import json
from pathlib import Path
import sqlite3
import unittest
import zlib

from backend.boundary_contract import VERSION
from backend.checkpoint import read_checkpoint, write_checkpoint
import test_meeting_capacity_summary as fixture_module

summary=fixture_module.summary


class BoundaryCapacityAuditTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixture_module.CapacitySummaryTests()
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.case=self.fixture.plan[1];self.out=Path(self.case['output'])
        self.data=Path(self.case['dataset'])
        self.logical_case()

    def logical_case(self):
        declaration={'version':VERSION,'logical_inbound':'INBOUND','logical_outbound':'OUTBOUND'}
        scene={'spec':{'levels':1,'faces':2,'baysPerFace':1},'racks':[{'id':'R01'}],
            'stations':[{'id':'IN-1','kind':'inbound'},{'id':'OUT-1','kind':'outbound'},{'id':'OUT-2','kind':'outbound'}]}
        Path(self.case['scene']).write_text(json.dumps(scene),encoding='utf-8')
        with (self.data/'tasks.csv').open(newline='',encoding='utf-8') as f:rows=list(csv.DictReader(f))
        rows[0]['target_id']='OUTBOUND'
        with (self.data/'tasks.csv').open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
        manifest=json.loads((self.data/'manifest.json').read_text())
        manifest.update(boundary_routing=declaration);manifest['files']['tasks.csv']=summary.file_sha(self.data/'tasks.csv')
        (self.data/'manifest.json').write_text(json.dumps(manifest))
        identity={'tasks_sha256':summary.file_sha(self.data/'tasks.csv'),
            'initial_inventory_sha256':summary.file_sha(self.data/'initial_inventory.csv'),
            'manifest_sha256':summary.file_sha(self.data/'manifest.json'),'scene_sha256':summary.file_sha(self.case['scene'])}
        self.case.update(tasks_sha256=identity['tasks_sha256']);self.fixture.save_plan()
        cp=read_checkpoint(self.out/'checkpoint.json');run=cp['run_id'];cp['event_sequence']=3
        cp['run_metadata']={**identity,'scene':scene,'boundary_routing':declaration}
        task=cp['tasks']['T1'];task.update(plan_source_id=task['source_id'],plan_target_id='OUTBOUND',
            resolved_source_id=task['source_id'],resolved_target_id='OUT-1',target_bound_at=0,
            boundary_bindings={'target':{'logical_id':'OUTBOUND','actual_id':'OUT-1','at':0,'phase':'release',
                'event_seq':1,'event_id':run+':event:1'}})
        write_checkpoint(self.out/'checkpoint.json',cp)
        result=json.loads((self.out/'results.json').read_text());result['input_identity']=identity
        (self.out/'results.json').write_text(json.dumps(result))
        events=[{'event_id':run+':event:1','event_seq':1,'kind':'boundary_port_bound','task_id':'T1',
            'endpoint':'target','planned_location_id':'OUTBOUND','actual_location_id':'OUT-1',
            'logical_id':'OUTBOUND','actual_id':'OUT-1','binding_phase':'release','sim_time':0},
            {'event_id':run+':event:2','event_seq':2,'kind':'task_released','task_id':'T1','sim_time':0},
            {'event_id':run+':event:3','event_seq':3,'kind':'shipped','task_id':'T1','goods_id':'G1',
             'location_id':'SHIPPED','robot_id':1,'sim_time':10}]
        self.batch={'run_id':run,'seq':1,'sim_time':10,'events':events,'tasks':[deepcopy(task)]}
        self.save_journal()

    def save_journal(self):
        with sqlite3.connect(self.out/'journal.sqlite') as db:
            db.execute('DELETE FROM commits')
            b=self.batch;db.execute('INSERT INTO commits VALUES (?,?,?,?)',
                (b['run_id'],b['seq'],summary.digest(b),zlib.compress(summary.canonical(b))))

    def report(self):return self.fixture.report()['runs'][1]

    def test_declared_plan_is_compared_exactly_and_actual_binding_is_verified(self):
        r=self.report();self.assertTrue(r['completed_run'],r['errors'])
        self.assertEqual(r['boundary_routing_audit'],{'bindings':1,'logical_tasks':1})

    def test_stripping_binding_event_cannot_make_the_source_look_fixed(self):
        self.batch['events'][0].update(kind='unrelated')
        self.save_journal();r=self.report();self.assertFalse(r['completed_run'])
        self.assertTrue(any('unique source event' in e for e in r['errors']),r['errors'])

    def test_foreign_or_changed_plan_is_not_ignored_when_actual_is_valid(self):
        cp=read_checkpoint(self.out/'checkpoint.json');cp['tasks']['T1']['plan_target_id']='OUT-2'
        write_checkpoint(self.out/'checkpoint.json',cp);r=self.report();self.assertFalse(r['completed_run'])

    def test_history_cannot_rewrite_the_selected_port_without_changing_final_csv(self):
        old=deepcopy(self.batch['tasks'][0]);old.update(target_id='OUT-2',resolved_target_id='OUT-2')
        old['boundary_bindings']['target']['actual_id']='OUT-2'
        self.batch['tasks'].insert(0,old);self.save_journal()
        r=self.report();self.assertFalse(r['completed_run'])
        self.assertTrue(any('rebound' in e for e in r['errors']),r['errors'])

    def test_future_binding_event_reference_is_not_hidden_by_valid_checkpoint_hash(self):
        self.batch['tasks'][0]['boundary_bindings']['target']['event_seq']=4
        self.batch['tasks'][0]['boundary_bindings']['target']['event_id']=self.batch['run_id']+':event:4'
        self.save_journal();r=self.report();self.assertFalse(r['completed_run'])
        self.assertTrue(any('future source event' in e for e in r['errors']),r['errors'])

    def test_early_history_cannot_loosen_derived_deadline_then_restore_final_value(self):
        self.batch['tasks'][0]['due_second']+=1
        self.save_journal();r=self.report();self.assertFalse(r['completed_run'])
        self.assertTrue(any('clock differs' in e for e in r['errors']),r['errors'])

    def test_logical_csv_cannot_drop_manifest_declaration(self):
        cp=read_checkpoint(self.out/'checkpoint.json')
        manifest=json.loads((self.data/'manifest.json').read_text());manifest.pop('boundary_routing')
        (self.data/'manifest.json').write_text(json.dumps(manifest))
        cp['run_metadata']['manifest_sha256']=summary.file_sha(self.data/'manifest.json')
        write_checkpoint(self.out/'checkpoint.json',cp)
        result=json.loads((self.out/'results.json').read_text())
        result['input_identity']['manifest_sha256']=cp['run_metadata']['manifest_sha256']
        (self.out/'results.json').write_text(json.dumps(result))
        r=self.report();self.assertFalse(r['completed_run'])
        self.assertTrue(any('lacks its manifest' in e for e in r['errors']),r['errors'])

    def test_balanced_specific_csv_needs_no_logical_input_declaration(self):
        with (self.data/'tasks.csv').open(newline='',encoding='utf-8') as f:rows=list(csv.DictReader(f))
        rows[0]['target_id']='OUT-1'
        with (self.data/'tasks.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
        manifest=json.loads((self.data/'manifest.json').read_text());manifest.pop('boundary_routing')
        manifest['files']['tasks.csv']=summary.file_sha(self.data/'tasks.csv')
        (self.data/'manifest.json').write_text(json.dumps(manifest))
        self.case['tasks_sha256']=manifest['files']['tasks.csv'];self.fixture.save_plan()
        cp=read_checkpoint(self.out/'checkpoint.json');cp['event_sequence']=2
        cp['run_metadata'].update(tasks_sha256=self.case['tasks_sha256'],manifest_sha256=summary.file_sha(self.data/'manifest.json'))
        task=cp['tasks']['T1'];task.update(plan_target_id='OUT-1',target_bound_at=None,boundary_bindings={})
        write_checkpoint(self.out/'checkpoint.json',cp)
        result=json.loads((self.out/'results.json').read_text());result['input_identity']={k:cp['run_metadata'][k] for k in result['input_identity']}
        (self.out/'results.json').write_text(json.dumps(result))
        self.batch['tasks']=[deepcopy(task)];self.batch['events']=self.batch['events'][1:]
        for seq,event in enumerate(self.batch['events'],1):event.update(event_seq=seq,event_id=cp['run_id']+f':event:{seq}')
        self.save_journal();r=self.report();self.assertTrue(r['completed_run'],r['errors'])
        self.assertEqual(r['boundary_routing_audit'],{'bindings':0,'logical_tasks':0})


if __name__=='__main__':unittest.main()
