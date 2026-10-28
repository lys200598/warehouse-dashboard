from copy import deepcopy
import json
import math
from pathlib import Path
import tempfile
import unittest

from backend.knowledge import digest
from backend.retrieval_eval import business_vector,business_relevant,family,cosine
from backend.retrieval_eval.service import BusinessRetriever,CandidateBudgetExceeded,HEADER,CANDIDATES


STATE={'robot_id':1,'x':10.,'y':20.,'battery':24.,'carrying':True,'stage':'to_target',
       'task_id':'TASK-1','cargo_id':'GOODS-1'}


class Mirror:
    def __init__(self):
        self.calls=[]
        self.header={'head':100,'checksum':'c'*64,'metadata':json.dumps({'scene':{'spec':{'width':90,'depth':76}}}),
            'query_payload':json.dumps(STATE),'query_sha256':digest(STATE)}
        self.rows=[{'key':'run:70:2','run_id':'run','seq':70,'robot_id':2,
            'x':11.,'y':20.,'battery':25.,'carrying':True,'stage':'to_target','source_payload_sha256':'a'*64},
            {'key':'run:71:3','run_id':'run','seq':71,'robot_id':3,
            'x':70.,'y':60.,'battery':99.,'carrying':False,'stage':'charging','source_payload_sha256':'b'*64}]
    def query(self,query,params,timeout):
        self.calls.append((query,params,timeout))
        return [deepcopy(self.header)] if query==HEADER else deepcopy(self.rows)


class FeaturesTests(unittest.TestCase):
    def test_identity_clock_and_ids_do_not_change_business_similarity(self):
        other={**STATE,'robot_id':99,'task_id':'OTHER','cargo_id':'OTHER','elapsed':999999,'event_seq':2000}
        self.assertEqual(business_vector(STATE),business_vector(other))
        self.assertEqual(len(business_vector(STATE)),17)
        self.assertAlmostEqual(sum(v*v for v in business_vector(STATE)),1.)
    def test_phase_is_categorical_and_relevance_is_independent_rule(self):
        self.assertEqual(family('to_target'),family('to_source'))
        good={**STATE,'x':11,'battery':30,'stage':'to_source'}
        bad={**STATE,'battery':80}
        self.assertTrue(business_relevant(STATE,good));self.assertFalse(business_relevant(STATE,bad))
        self.assertGreater(cosine(business_vector(STATE),business_vector(good)),
                           cosine(business_vector(STATE),business_vector(bad)))
    def test_invalid_physics_and_types_rejected(self):
        for patch in ({'x':91},{'battery':101},{'x':math.nan},{'carrying':1},{'stage':''},{'battery':False}):
            with self.subTest(patch=patch),self.assertRaises(ValueError):business_vector({**STATE,**patch})
        with self.assertRaises(ValueError):cosine([0,0],[1,1])
        with self.assertRaises(ValueError):cosine([1],[1,2])


class RetrievalServiceTests(unittest.TestCase):
    def test_real_query_domain_exact_order_and_provenance(self):
        mirror=Mirror();result=BusinessRetriever(mirror).search('run',100,1)
        self.assertEqual([r['robot_id'] for r in result['rows']],[2,3])
        self.assertEqual(result['domain']['start_seq'],40);self.assertEqual(result['domain']['end_seq'],80)
        self.assertEqual(result['domain']['candidate_count'],2)
        self.assertTrue(result['domain']['complete_domain']);self.assertEqual(result['returned_k'],2)
        self.assertEqual(mirror.calls[1][1]['budget'],10001)
        self.assertIn('USING INDEX n:WHAgentState(run_id,seq)',CANDIDATES)
        self.assertNotIn('queryNodes',CANDIDATES)
    def test_zero_matches_not_filled_with_future_or_self(self):
        mirror=Mirror();mirror.rows=[]
        result=BusinessRetriever(mirror).search('run',100,1)
        self.assertEqual(result['rows'],[]);self.assertEqual(result['domain']['candidate_count'],0)
    def test_budget_fails_instead_of_returning_truncated_exact(self):
        with self.assertRaises(CandidateBudgetExceeded):BusinessRetriever(Mirror()).search('run',100,1,max_candidates=1)
    def test_foreign_future_self_duplicate_or_untyped_candidates_fail(self):
        patches=[{'run_id':'other'},{'seq':81},{'seq':39},{'robot_id':1},{'robot_id':True},
                 {'key':'wrong'},{'source_payload_sha256':'x'}, {'battery':float('inf')}]
        for patch in patches:
            mirror=Mirror();mirror.rows=[{**mirror.rows[0],**patch}]
            with self.subTest(patch=patch),self.assertRaises(ValueError):BusinessRetriever(mirror).search('run',100,1)
        mirror=Mirror();mirror.rows=[mirror.rows[0],mirror.rows[0]]
        with self.assertRaises(ValueError):BusinessRetriever(mirror).search('run',100,1)
    def test_bad_head_query_source_and_early_clock_fail(self):
        for patch in ({'head':99},{'head':True},{'query_sha256':'b'*64},{'checksum':None},
                      {'query_payload':json.dumps({**STATE,'robot_id':2})}):
            mirror=Mirror();mirror.header.update(patch)
            with self.subTest(patch=patch),self.assertRaises(ValueError):BusinessRetriever(mirror).search('run',100,1)
        with self.assertRaises(ValueError):BusinessRetriever(Mirror()).search('run',10,1)
    def test_option_budgets_and_boolean_types_are_strict(self):
        for options in ({'k':True},{'window_steps':0},{'embargo_steps':0},{'max_candidates':100001},
                        {'exclude_query_robot':1},{'window_steps':10,'embargo_steps':20}):
            with self.subTest(options=options),self.assertRaises(ValueError):BusinessRetriever(Mirror()).search('run',100,1,**options)
    def test_explicit_self_inclusion_still_has_history_embargo(self):
        mirror=Mirror();mirror.rows[0].update(key='run:70:1',robot_id=1)
        result=BusinessRetriever(mirror).search('run',100,1,exclude_query_robot=False)
        self.assertEqual(result['rows'][0]['robot_id'],1)
        self.assertEqual(result['domain']['end_seq'],80)


class ExactRankingTests(unittest.TestCase):
    def test_selection_spans_end_and_deduplicates_low_soc_episode(self):
        try:import numpy as np
        except ImportError:self.skipTest('optional numpy evaluator is unavailable')
        from backend.retrieval_eval.evaluate import select_queries
        seqs=np.repeat(np.arange(1,1001),2);robots=np.tile([1,2],1000)
        physics=np.zeros((2000,4));physics[:,2]=90
        physics[1200:1210:2,2]=24;physics[1200:1210:2,3]=1
        selected=select_queries(seqs,robots,physics,12,20)
        self.assertEqual(len(set(selected)),12)
        self.assertEqual(seqs[selected[-1]],1000)
        self.assertEqual(sum(physics[i,3]==1 for i in selected),1)

    def test_numpy_matches_independent_scalar_and_tie_order(self):
        try:import numpy as np
        except ImportError:self.skipTest('optional numpy evaluator is unavailable')
        from backend.retrieval_eval.evaluate import rank_numpy,rank_scalar
        rng=np.random.default_rng(42)
        matrix=rng.random((300,17));matrix/=np.linalg.norm(matrix,axis=1)[:,None]
        seqs=np.repeat(np.arange(1,101),3);robots=np.tile([1,2,3],100)
        for query in (0,18,299):
            valid=np.flatnonzero((seqs<seqs[query])&(robots!=robots[query]))
            if len(valid)==0:continue
            left=rank_numpy(matrix,matrix[query],valid,5,seqs,robots)
            right=rank_scalar(matrix,matrix[query],valid,5,seqs,robots)
            self.assertEqual([i for i,s in left],[i for i,s in right])
            for a,b in zip(left,right):self.assertAlmostEqual(a[1],b[1],places=12)
        matrix=np.ones((6,3))/math.sqrt(3);seqs=np.array([3,2,1,3,2,1]);robots=np.array([2,2,2,1,1,1])
        self.assertEqual([i for i,s in rank_numpy(matrix,matrix[0],np.arange(6),3,seqs,robots)],[5,2,4])


if __name__=='__main__':unittest.main()
