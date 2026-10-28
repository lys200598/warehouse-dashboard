"""Platform routing is explicit and keeps the existing engine's safety gates."""
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from backend.warehouse.__main__ import main,parser,ROOT


class PlatformCLITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (ROOT/'data/local-check').mkdir(parents=True,exist_ok=True)

    def invoke(self,args):
        stream=StringIO()
        with redirect_stdout(stream):code=main(args)
        return code,json.loads(stream.getvalue())

    def test_status_and_defaults_are_explicit(self):
        code,value=self.invoke(['status'])
        self.assertEqual(code,0);self.assertEqual(value['default_mode'],'required')
        self.assertIsNone(value['duration_cap']);self.assertEqual(value['default_knowledge_backend'],'neo4j')
        args=parser().parse_args(['run','--dataset','a','--output','b','--run-id','c'])
        self.assertEqual(args.knowledge_mode,'required');self.assertEqual(args.commit_seconds,5)
        self.assertTrue(Path(args.scene).is_absolute())

    def test_rules_is_deliberate_and_coarse_sampling_explicit(self):
        args=parser().parse_args(['run','--dataset','a','--output','b','--run-id','c',
            '--mode','rules','--knowledge-backend','journal','--commit-seconds','300','--telemetry-jsonl','t.jsonl'])
        self.assertEqual(args.knowledge_mode,'rules');self.assertEqual(args.commit_seconds,300)
        self.assertEqual(args.telemetry_jsonl,'t.jsonl')

    def test_identifiers_match_query_contract(self):
        from backend.wide.data import _identifier
        self.assertEqual(_identifier('x'*128,'id'),'x'*128)
        for bad in ('x'*129,'',' white ',True):
            with self.assertRaises(ValueError):_identifier(bad,'id')

    def test_critical_boundary_is_published_at_coarse_cadence(self):
        from tests.test_knowledge_runtime import RuntimeKnowledgeTests
        fixture=RuntimeKnowledgeTests();sim=fixture.make()
        owner=fixture.carrying(sim)
        self.assertTrue(sim.request_assistance(owner))
        at=sim.assists[owner]['handoff_at']
        while sim.elapsed<at-1:
            self.assertFalse(sim.knowledge_boundary_due());sim.step()
        self.assertTrue(sim.knowledge_boundary_due())
        sim.step();self.assertEqual(sim.metrics['handoffs'],1)
        self.assertFalse(sim.knowledge_boundary_due())

    def test_real_sample_validation(self):
        code,value=self.invoke(['validate','--dataset',str(ROOT/'data/examples/warehouse-v5')])
        self.assertEqual(code,0);self.assertTrue(value['valid'])

    def test_generate_eight_days_and_revalidate_no_reset(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'data/local-check') as directory:
            folder=Path(directory)/'data'
            code,value=self.invoke(['generate','--kind','multiday','--days','8','--tasks-per-day','16',
                '--active-slots','16','--output',str(folder)])
            self.assertEqual(code,0);self.assertTrue(value['validation']['valid'])
            self.assertEqual(value['manifest']['days'],8)
            _,again=self.invoke(['validate','--dataset',str(folder)])
            self.assertEqual(again['tasks'],value['manifest']['tasks'])
            with self.assertRaises(FileExistsError):main(['generate','--output',str(folder)])

    def test_missing_frames_fail_before_opening_server(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'data/local-check') as directory:
            with self.assertRaises(SystemExit) as error:main(['serve','--output',directory])
            self.assertEqual(error.exception.code,2)

    def test_run_routes_same_factory_config_and_auditor(self):
        with patch('backend.knowledge_runtime.runner.run',return_value=0) as run:
            self.assertEqual(main(['run','--dataset','a','--output','b','--run-id','c']),0)
            args=run.call_args.args[0];kw=run.call_args.kwargs
            self.assertEqual(args.knowledge_mode,'required')
            self.assertEqual(kw['simulation_class'].keywords['knowledge_mode'],'required')
            self.assertEqual(kw['graph_auditor'].__name__,'audit_real')


if __name__=='__main__':unittest.main()
