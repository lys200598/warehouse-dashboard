from argparse import Namespace
import json
from pathlib import Path
import tempfile
import unittest

from backend.scale_benchmark.model import archive_state,fingerprint,source_sequence
from backend.scale_benchmark.profile import run


class ProfileToolTests(unittest.TestCase):
    def test_thread_profiles_and_complete_oracle_kept_diagnostic_only(self):
        templates={(seq,robot):json.dumps({'robot_id':robot,'x':seq,'task_id':None})
            for seq in [1,2] for robot in range(1,101)}
        source_hash=fingerprint([[seq,robot,json.loads(payload)] for (seq,robot),payload in sorted(templates.items())])
        class Store:
            def head(self,run):
                return ({'committed_seq':2} if run=='business' else
                    {'source_hash':source_hash,'committed_seq':10000,'archive_steps':10000})
            def templates(self,run):return [1,2],templates
            def task_templates(self,run):return []
            def full_query(self,business,business_seq,archive,robot,seq,query_mode):
                history=[archive_state(archive,s,robot,business,source_sequence(s,10000,[1,2]),
                    templates[(source_sequence(s,10000,[1,2]),robot)]) for s in range(seq,max(0,seq-5),-1)]
                point=history[0]
                return {'business':{'run_id':business,'seq':business_seq,'robot':json.loads(templates[(business_seq,robot)]),'task':None},
                    'history':history,'query_vector':point['embedding'],'similar':[{**point,'score':1}],
                    'retrieval_path':'ann100'}
        parent=Path(__file__).resolve().parents[1]/'data/local-check'
        parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=parent) as directory:
            args=Namespace(business_run='business',archive_run='archive',business_output=None,
                output_dir=Path(directory),server_profile=False,clients=3,samples=2,max_wall_seconds=60)
            run(args,Store())
            report=json.loads((Path(directory)/'report.json').read_text(encoding='utf-8'))
            self.assertIn('diagnostic-only',report['scope'])
            self.assertEqual(report['actual_requests'],6)
            self.assertEqual(report['requests_summary']['failures'],0)
            self.assertGreater(report['client_process_cpu_seconds'],0)
            self.assertTrue((Path(directory)/'workers.pstats').is_file())
            self.assertTrue((Path(directory)/'workers-top100.txt').is_file())


if __name__=='__main__':unittest.main()
