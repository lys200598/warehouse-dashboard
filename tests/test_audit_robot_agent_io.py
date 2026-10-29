"""Negative qualification tests, independent of a running graph/database."""
from copy import deepcopy
from types import SimpleNamespace
import unittest

from backend.knowledge import digest
from ops.audit_robot_agent_io import (packet_integrity,packet_kind,trace_verdict,
    client_packet_coverage,check_canonical_context,stats,expected_local_events,closed_verdict)


def packet():
    body={'run_id':'same-run','robot_id':1,'seq':1,'sim_time':0,'committed_seq':1,
          'state':{'robot_id':1},'events':[]}
    res={'ok':True,'run_id':'same-run','robot_id':1,'seq':1,'payload_sha256':digest(body),
         'backend':'neo4j','metrics':{'queue_and_commit_ms':0.}}
    http={'kind':'agent_http_client','request_id':'agent-1-1-write','robot_id':1,'pid':123,
          'path':'/api/agent/observation','status':200,'error':None,'body_complete':True,
          'start_ns':1,'headers_ns':2,'body_end_ns':3,'parse_end_ns':4,'end_ns':5,'duration_ms':.000004}
    return {'request_id':http['request_id'],'robot_id':1,'pid':123,'path':http['path'],
            'request':body,'response':res,'http':http,'request_sha256':digest(body),
            'response_sha256':digest(res),'response_body_sha256':digest(res),'observer_after_http':True}


class AgentAuditNegativeTests(unittest.TestCase):
    def test_valid_packet_passes(self):
        self.assertEqual(packet_integrity(packet(),'same-run',{1:123},digest),'write')

    def test_unknown_endpoint_not_treated_as_query(self):
        value=packet();value['path']=value['http']['path']='/api/control'
        with self.assertRaisesRegex(ValueError,'unknown agent endpoint'): packet_kind(value)

    def test_tampered_request_or_response_fails(self):
        for section in ('request','response'):
            value=packet();value[section]['robot_id']=2
            with self.assertRaises(ValueError):packet_integrity(value,'same-run',{1:123},digest)

    def test_foreign_run_and_wrong_worker_pid_fail(self):
        value=packet();value['request']['run_id']='other-run';value['request_sha256']=digest(value['request'])
        with self.assertRaisesRegex(ValueError,'binding'):packet_integrity(value,'same-run',{1:123},digest)
        value=packet();value['http']['pid']=999
        with self.assertRaisesRegex(ValueError,'binding'):packet_integrity(value,'same-run',{1:123},digest)

    def test_failed_http_kept_and_rejected(self):
        value=packet();value['http'].update(status=503,error='HTTPError')
        with self.assertRaisesRegex(ValueError,'HTTP failure'):packet_integrity(value,'same-run',{1:123},digest)

    def test_missing_full_response_cannot_pass_coverage(self):
        value=packet();clients={value['request_id']:value['http']}
        with self.assertRaisesRegex(ValueError,'coverage differs'):client_packet_coverage(clients,{})
        client_packet_coverage(clients,{value['request_id']:value})
        value['http']=deepcopy(value['http']);value['http']['status']=503
        with self.assertRaisesRegex(ValueError,'differs'):client_packet_coverage(clients,{value['request_id']:value})

    def test_verifier_returned_differences_are_not_ignored(self):
        bad=SimpleNamespace(check=lambda request,response:['$.robot.state: tampered'])
        with self.assertRaisesRegex(ValueError,'canonical context differences'):check_canonical_context(bad,{}, {})
        check_canonical_context(SimpleNamespace(check=lambda request,response:[]),{}, {})

    def test_overflow_transport_drop_and_missing_status_fail_trace(self):
        api=[{'request_id':'x','path':'/api/agent/query','start_ns':1,'end_ns':2,
              'status':200,'body_send_completed':True}]
        good={'complete':True,'drained':True,'dropped':0,'transport_rejections':0}
        self.assertTrue(trace_verdict(api,{'x'},good)[0])
        for key,value in [('complete',False),('drained',False),('dropped',1),
                          ('transport_rejections',1),('prior_trace_incomplete',True)]:
            bad={**good,key:value}
            self.assertFalse(trace_verdict(api,{'x'},bad)[0],key)
        self.assertFalse(trace_verdict(api,{'x'},None)[0])
        self.assertFalse(trace_verdict(api,{'x','missing'},good)[0])

    def test_duplicate_or_failed_server_interval_fails(self):
        row={'request_id':'x','path':'/api/agent/query','start_ns':1,'end_ns':2,
             'status':200,'body_send_completed':True}
        good={'complete':True,'drained':True,'dropped':0}
        self.assertFalse(trace_verdict([row,row],{'x'},good)[0])
        self.assertFalse(trace_verdict([{**row,'status':503}],{'x'},good)[0])

    def test_p99_nearest_rank_and_tail_denominator(self):
        value=stats([1.,900.])
        self.assertEqual(value['n'],2)
        self.assertEqual(value['p99_ms'],900.)
        self.assertEqual(value['over_500ms'],1)

    def test_local_events_derived_from_source_state(self):
        previous={'stage':'idle','battery':80,'task_id':None}
        current={'stage':'to_source','battery':30,'task_id':'T1'}
        self.assertEqual([r['kind'] for r in expected_local_events(current,previous,10)],
                         ['stage_changed','task_changed','low_energy_observed'])
        self.assertEqual(expected_local_events(current,current,11),[])
        self.assertEqual(expected_local_events({'stage':'idle','battery':80},None,0),
                         [{'kind':'observation_started','sim_time':0}])

    def test_missing_or_failed_closed_proof_rejected(self):
        value={'closed':True,'agents':100,'failed':False,'backend':'neo4j','live_processes':0,
               'http_service_alive':False,'writer_alive':False,'all_owned_processes_stopped':True}
        self.assertTrue(closed_verdict(value))
        for key in value:
            bad=dict(value);del bad[key]
            self.assertFalse(closed_verdict(bad),key)
        self.assertFalse(closed_verdict({**value,'failed':True}))


if __name__=='__main__':unittest.main()
