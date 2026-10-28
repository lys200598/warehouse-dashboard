import csv
from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest

from backend.day1.data import sha, load_scene as load_old_scene
from backend.scheduling_v3.scenarios import generate_scenario as generate_old
from backend.wide.data import load_scene, migrate_dataset, read_dataset, validate_dataset
from backend.wide.scenarios import generate_scenario


class WideDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[1]
        cls.scene = cls.repo/'data/layouts/warehouse100-wide-v2.json'
        cls.old_scene = cls.repo/'data/layouts/warehouse100-v1.json'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='wide-data-',dir=self.repo/'tests')
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def variant(self, change):
        scene = json.loads(self.scene.read_text(encoding='utf-8'))
        change(scene)
        path = self.root/'variant.json'
        path.write_text(json.dumps(scene),encoding='utf-8')
        return path

    def test_coordinates_canonical_capacity_and_normalized_aliases(self):
        before = sha(self.scene)
        scene,slots = load_scene(self.scene)
        self.assertEqual((scene['spec']['width'],scene['spec']['depth']),(90,76))
        self.assertEqual(len(slots),1728)
        old,old_slots=load_old_scene(self.old_scene)
        self.assertEqual(set(slots)|{s['id'] for s in scene['stations']},
                         set(old_slots)|{s['id'] for s in old['stations']})
        self.assertAlmostEqual(slots['R01-L1-C1-F1']['x'],18.9)
        self.assertAlmostEqual(slots['R01-L1-C8-F1']['x'],27.3)
        self.assertAlmostEqual(slots['R01-L1-C1-F1']['y'],13.3)
        self.assertEqual(scene['physical_bay_count'],100)
        self.assertEqual(len(scene['canonical_bay_ids']),100)
        self.assertEqual(scene['location_aliases']['P001'],'C-A01')
        home,dock=scene['homes'][0],scene['docks'][0]
        self.assertEqual(home['kind'],'home')
        self.assertEqual(home['point'],dock['point'])
        self.assertEqual(home['gate'],dock['gate'])
        self.assertFalse(home['staticCollision'])
        self.assertEqual(sha(self.scene),before)

    def test_invalid_alias_capacity_and_slot_geometry_rejected(self):
        changes=[lambda s:s['homes'][0].update(alias_of='MISSING'),
                 lambda s:s['homes'][0].update(point={'x':3.,'y':3.}),
                 lambda s:s['docks'][0].update(capacity=2),
                 lambda s:s['docks'][0].update(capacity=True),
                 lambda s:s['spec'].update(slotPitch=.6),
                 lambda s:s['spec'].update(robotRadius=.1),
                 lambda s:s['docks'].pop(),
                 lambda s:s['stations'][0].update(id='MISSING')]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(ValueError):load_scene(self.variant(change))

    def test_each_wide_scenario_has_valid_seven_field_input(self):
        for scenario in ('uniform','peak','hotspot','continuous'):
            with self.subTest(scenario=scenario):
                output=self.root/scenario
                manifest,report=generate_scenario(self.scene,output,scenario=scenario,duration_seconds=1800,
                                                  slot_interval_seconds=600,active_slots=16)
                self.assertTrue(report['valid'])
                self.assertFalse(report['runtime_feasibility_claim'])
                self.assertEqual(report['initial'],864)
                self.assertEqual(manifest['scene_sha256'],sha(self.scene))
                self.assertEqual(manifest['capacity'],1728)
                validate_dataset(self.scene,output)
                with (output/'tasks.csv').open(encoding='utf-8',newline='') as f:
                    self.assertEqual(next(csv.reader(f)),['task_id','task_type','goods_id','release_time','due_time','source_id','target_id'])

    def test_multi_day_dependencies_and_fixed_seed_are_reproducible(self):
        a,b=self.root/'a',self.root/'b'
        ma,ra=generate_scenario(self.scene,a,active_slots=12,duration_seconds=172800,slot_interval_seconds=21600,seed=123)
        mb,rb=generate_scenario(self.scene,b,active_slots=12,duration_seconds=172800,slot_interval_seconds=21600,seed=123)
        self.assertEqual(ma,mb)
        self.assertEqual(ra,rb)
        self.assertEqual(ma['files'],mb['files'])
        self.assertEqual(len(ra['daily_tasks']),2)
        self.assertTrue(all(ra['daily_tasks']))
        self.assertEqual(ra['planned_final_inventory'],864)

    def test_preserve_migration_keeps_both_csv_bytes_and_source_identity(self):
        source=self.root/'source'
        generate_old(self.old_scene,source,active_slots=16,duration_seconds=3600,slot_interval_seconds=1200)
        original={p.name:sha(p) for p in source.iterdir()}
        output=self.root/'migration'
        manifest,report=migrate_dataset(self.old_scene,self.scene,source,output)
        self.assertTrue(report['valid'])
        self.assertEqual(manifest['migration']['deadline_policy'],'preserve')
        self.assertEqual(manifest['migration']['due_time_changes'],0)
        for filename in ('tasks.csv','initial_inventory.csv'):
            self.assertEqual((source/filename).read_bytes(),(output/filename).read_bytes())
        self.assertEqual(manifest['migration']['source_manifest_sha256'],original['manifest.json'])
        self.assertEqual(manifest['scene_sha256'],sha(self.scene))
        self.assertNotEqual(manifest['scene_sha256'],manifest['migration']['source_scene_sha256'])
        self.assertEqual(original,{p.name:sha(p) for p in source.iterdir()})
        self.assertFalse(manifest['migration']['uses_measured_completion_times'])

    def test_hotspot_migration_does_not_label_old_coordinates_as_new(self):
        source=self.root/'source'
        generate_old(self.old_scene,source,scenario='hotspot',active_slots=16,duration_seconds=1800,slot_interval_seconds=600)
        original=json.loads((source/'manifest.json').read_text(encoding='utf-8'))
        manifest,_=migrate_dataset(self.old_scene,self.scene,source,self.root/'migrated')
        self.assertIsNone(manifest['hotspot_centre'])
        self.assertTrue(manifest['migration']['spatial_reclassification_required'])
        self.assertEqual(manifest['migration']['source_spatial_metadata']['hotspot_centre'],original['hotspot_centre'])
        self.assertEqual(manifest['hotspot_slot_ids'],original['hotspot_slot_ids'])

    def test_explicit_deadline_reference_is_declared_not_measured(self):
        source=self.root/'source'
        generate_old(self.old_scene,source,active_slots=16,duration_seconds=3600,slot_interval_seconds=1200)
        output=self.root/'migration'
        manifest,report=migrate_dataset(self.old_scene,self.scene,source,output,deadline_policy='explicit-reference')
        _,old_tasks,_=read_dataset(self.old_scene,source)
        _,new_tasks,_=read_dataset(self.scene,output)
        self.assertEqual([t['task_id'] for t in old_tasks],[t['task_id'] for t in new_tasks])
        self.assertEqual([t['goods_id'] for t in old_tasks],[t['goods_id'] for t in new_tasks])
        self.assertEqual([t['release_time'] for t in old_tasks],[t['release_time'] for t in new_tasks])
        self.assertGreater(manifest['migration']['due_time_changes'],0)
        self.assertFalse(manifest['migration']['uses_measured_completion_times'])
        self.assertTrue(all(datetime.fromisoformat(t['due_time'])>datetime.fromisoformat(t['release_time']) for t in new_tasks))

    def test_no_output_or_original_dataset_overwrite(self):
        source=self.root/'source'
        generate_old(self.old_scene,source,active_slots=4,duration_seconds=1800,slot_interval_seconds=600)
        with self.assertRaises(FileExistsError):generate_scenario(self.scene,source,active_slots=4)
        with self.assertRaises(FileExistsError):migrate_dataset(self.old_scene,self.scene,source,source)
        with self.assertRaises(ValueError):migrate_dataset(self.old_scene,self.scene,source,source/'nested')
        with self.assertRaises(ValueError):migrate_dataset(self.old_scene,self.scene,source,self.root/'unused',deadline_policy='measured')

    def test_bad_hash_duplicate_task_and_manifest_counts_fail_preflight(self):
        output=self.root/'dataset'
        generate_scenario(self.scene,output,active_slots=8,duration_seconds=1800,slot_interval_seconds=600)
        manifest=json.loads((output/'manifest.json').read_text(encoding='utf-8'))
        manifest['tasks']+=1
        (output/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaises(ValueError):validate_dataset(self.scene,output)
        manifest['tasks']-=1
        (output/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
        with (output/'tasks.csv').open(encoding='utf-8',newline='') as f:rows=list(csv.DictReader(f))
        rows[1]['task_id']=rows[0]['task_id']
        with (output/'tasks.csv').open('w',encoding='utf-8',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
        with self.assertRaisesRegex(ValueError,'hash mismatch'):validate_dataset(self.scene,output)
        manifest['files']['tasks.csv']=sha(output/'tasks.csv')
        (output/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'duplicate task'):validate_dataset(self.scene,output)


if __name__=='__main__':unittest.main()
