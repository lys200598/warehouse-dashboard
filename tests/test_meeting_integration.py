"""Opt-in rack resources keep source commits, controls and query cutoffs aligned."""
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
import tempfile

from backend.resource_graph_mirror import ResourceMirror
from backend.query_catalog.service import QueryCatalogService
from backend.warehouse.__main__ import parser
from backend.knowledge_runtime.sim import KnowledgeSimulation

ROOT=Path(__file__).resolve().parents[1]


class MeetingIntegrationTests(TestCase):
    def test_resource_hook_is_validated_before_parent_and_uses_same_tx(self):
        tx=SimpleNamespace(run=lambda *a,**k:SimpleNamespace(single=lambda:None))
        events=[];batch={'run_id':'test','seq':1};prepared=object()
        with patch('backend.resource_graph_mirror.validate_forward_batches',side_effect=lambda f:(events.append(('validate',f)),prepared)[1]), \
             patch('backend.resource_graph_mirror.RealMirror._apply',side_effect=lambda t,b:events.append(('parent',t))), \
             patch('backend.resource_graph_mirror.apply_resources',side_effect=lambda t,b,**k:events.append(('resource',t,k['prepared']))):
            ResourceMirror._apply(tx,[batch])
        self.assertEqual(events,[('validate',[batch]),('parent',tx),('resource',tx,prepared)])

    def test_invalid_resource_package_never_calls_parent_writer(self):
        tx=SimpleNamespace(run=lambda *a,**k:SimpleNamespace(single=lambda:None))
        with patch('backend.resource_graph_mirror.validate_forward_batches',side_effect=ValueError('bad input')), \
             patch('backend.resource_graph_mirror.RealMirror._apply') as parent:
            with self.assertRaises(ValueError):ResourceMirror._apply(tx,[{'run_id':'r','seq':1}])
            parent.assert_not_called()

    def test_replay_still_uses_parent_checksum_validation_without_projection(self):
        tx=SimpleNamespace(run=lambda *a,**k:SimpleNamespace(single=lambda:{'seq':3}))
        with patch('backend.resource_graph_mirror.RealMirror._apply') as parent, \
             patch('backend.resource_graph_mirror.apply_resources') as projection:
            ResourceMirror._apply(tx,[{'run_id':'r','seq':3}]);parent.assert_called_once();projection.assert_not_called()

    def test_rack_catalog_retains_explicit_cutoff_and_does_not_use_top5_slot_budget(self):
        store=SimpleNamespace(mirror=object(),timeout=4)
        with patch('backend.resource_graph.RackInventoryService') as cls:
            cls.return_value.inventory.return_value={'capacity':48,'occupied':2}
            result=QueryCatalogService(store).execute({'query_id':'rack_inventory','run_id':'run-A','seq':9,'rack_id':'R01'})
            cls.return_value.inventory.assert_called_once_with('run-A',9,'R01',include_empty=True)
            self.assertEqual(result['result']['capacity'],48);self.assertEqual(result['seq'],9)

    def test_cli_flag_is_opt_in_and_does_not_change_default(self):
        args=['run','--dataset','data/example','--output','out','--run-id','r']
        self.assertFalse(parser().parse_args(args).resource_graph)
        self.assertTrue(parser().parse_args(args+['--resource-graph']).resource_graph)

    def test_projection_identity_cannot_be_silently_removed_or_added_on_restore(self):
        args=(ROOT/'data/layouts/warehouse100-wide-v2.json',ROOT/'data/examples/warehouse-v5','identity-test')
        plain=KnowledgeSimulation(*args,knowledge_mode='rules')
        projected=KnowledgeSimulation(*args,knowledge_mode='rules',resource_graph=True)
        with self.assertRaises(ValueError):plain.restore(projected.checkpoint())
        with self.assertRaises(ValueError):projected.restore(plain.checkpoint())

    def test_scene_point_switches_with_published_run_not_an_old_layout(self):
        from ops.warehouse_live_runtime import WarehouseLiveRuntime
        from backend.wide.data import migrate_dataset, sha
        parent=ROOT/'data/local-check/meeting-scene-tests';parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as folder:
            p=Path(folder);old=ROOT/'data/layouts/warehouse100-wide-v2.json'
            new=ROOT/'data/layouts/expansion-candidates/medium/layout.json'
            data=ROOT/'data/examples/warehouse-v5'
            migrate_dataset(old,new,data,p/'migrated')
            runtime=WarehouseLiveRuntime(source_root=ROOT,output_root=p/'runs',datasets={
                'old':{'scene':old,'dataset':data},'medium':{'scene':new,'dataset':p/'migrated'}},dataset_id='old')
            try:
                before=runtime.scene_point();self.assertEqual(before['scene']['spec']['width'],90)
                runtime.command('new_run',{'dataset_id':'medium'},expected_run_id=before['run_id'])
                after=runtime.scene_point();self.assertEqual(after['scene']['spec']['width'],120)
                self.assertNotEqual(before['run_id'],after['run_id'])
                self.assertEqual(after['run_id'],runtime.latest()['runId'])
                self.assertEqual(after['scene_sha256'],sha(new))
            finally:runtime.shutdown()
