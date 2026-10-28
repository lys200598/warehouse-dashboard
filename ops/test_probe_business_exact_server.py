"""Offline boundary tests for the ops-only server exact prototype."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import unittest

P=Path(__file__).with_name('probe_business_exact_server.py')
spec=importlib.util.spec_from_file_location('business_server_probe',P)
probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
from backend.knowledge import digest
from backend.retrieval_eval import business_vector,cosine
from backend.retrieval_eval.service import HEADER,CandidateBudgetExceeded


Q={'robot_id':1,'x':10.,'y':20.,'battery':24.,'carrying':True,'stage':'to_target'}


class Mirror:
    def __init__(self):
        self.header={'head':100,'checksum':'a'*64,'metadata':json.dumps({'scene':{'spec':{'width':90,'depth':76}}}),
            'query_payload':json.dumps(Q),'query_sha256':digest(Q)}
        row={'key':'run:70:2','run_id':'run','seq':70,'robot_id':2,'x':11.,'y':21.,'battery':25.,
            'carrying':True,'stage':'to_target','source_payload_sha256':'b'*64,'phase':1}
        row['score']=cosine(business_vector(Q),business_vector(row))
        self.result={'candidate_count':1,'invalid_count':0,'rows':[row]};self.calls=[]
    def query(self,q,params,timeout):
        self.calls.append((q,params));return [deepcopy(self.header if q==HEADER else self.result)]


class ServerPrototypeTests(unittest.TestCase):
    def test_source_domain_and_exact_score(self):
        m=Mirror();r=probe.server_search(m,'run',100,1)
        self.assertEqual(r['domain']['candidate_count'],1)
        self.assertEqual(r['domain']['start_seq'],40);self.assertEqual(r['domain']['end_seq'],80)
        self.assertEqual(r['rows'][0]['robot_id'],2)
        self.assertEqual(m.calls[-1][1]['budget'],10001)
    def test_zero_domain_and_budget_are_explicit(self):
        m=Mirror();m.result={'candidate_count':0,'invalid_count':0,'rows':[]}
        self.assertEqual(probe.server_search(m,'run',100,1)['rows'],[])
        m.result={'candidate_count':2,'invalid_count':0,'rows':[]}
        with self.assertRaises(CandidateBudgetExceeded):probe.server_search(m,'run',100,1,maximum=1)
    def test_invalid_domain_partial_result_source_and_score_fail(self):
        for patch in ({'invalid_count':1},{'candidate_count':2}):
            m=Mirror();m.result.update(patch)
            with self.subTest(patch=patch),self.assertRaises(ValueError):probe.server_search(m,'run',100,1)
        for patch in ({'run_id':'foreign'},{'seq':90},{'robot_id':1},{'robot_id':True},
                      {'source_payload_sha256':None},{'score':0.}):
            m=Mirror();m.result['rows'][0].update(patch)
            with self.subTest(patch=patch),self.assertRaises(ValueError):probe.server_search(m,'run',100,1)
    def test_invalid_query_clock_and_cypher_shape(self):
        m=Mirror();m.header.update(query_payload=json.dumps({**Q,'robot_id':True}),query_sha256=digest({**Q,'robot_id':True}))
        with self.assertRaises(ValueError):probe.server_search(m,'run',100,1)
        with self.assertRaises(ValueError):probe.server_search(Mirror(),'run',10,1)
        self.assertIn('LIMIT $budget',probe.SERVER_QUERY)
        self.assertIn('candidate_count<=$maximum',probe.SERVER_QUERY)
        self.assertIn('ORDER BY score DESC,n.seq,n.robot_id LIMIT $k',probe.SERVER_QUERY)
        self.assertNotIn('APOC',probe.SERVER_QUERY.upper())
        self.assertNotIn('SET ',probe.SERVER_QUERY)


if __name__=='__main__':unittest.main()
