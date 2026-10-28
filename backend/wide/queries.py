"""Small bounded temporal read API; mutable current nodes are never past truth."""
import json


COOPERATION_KINDS = [
    'cargo_handoff','assistance_requested','assistance_deferred','handoff_reserved',
    'traffic_retimed','traffic_rerouted','robot_stopped','robot_stop_cleared',
    'robot_reenabled','robot_power_isolated','battery_measurement_corrected','task_requeued',
]


def _identity(value, name):
    if not isinstance(value,str) or not 1 <= len(value) <= 128 or any(ord(c)<32 for c in value):
        raise ValueError('invalid '+name)
    return value


def _integer(value, name, low=1, high=None):
    if isinstance(value,bool) or not isinstance(value,int) or value < low or high is not None and value > high:
        raise ValueError('invalid '+name)
    return value


def _payload(value):
    return json.loads(value) if isinstance(value,str) else value


class WideQueries:
    def __init__(self, mirror):
        self.mirror = mirror

    def _cutoff(self, run_id, seq):
        _identity(run_id,'run ID')
        rows = self.mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
            RETURN r.committed_seq AS seq''',{'run':run_id})
        if not rows or not rows[0]['seq']:
            raise ValueError('unknown or uncommitted run')
        head = int(rows[0]['seq'])
        seq = head if seq is None else _integer(seq,'committed cutoff',1,head)
        return seq,head

    def goods_history(self, run_id, goods_id, seq=None, limit=200, after_event_seq=0):
        _identity(goods_id,'goods ID')
        _integer(limit,'limit',1,1000)
        _integer(after_event_seq,'event cursor',0)
        cutoff,head = self._cutoff(run_id,seq)
        rows = self.mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
            MATCH (g:WHCargo {key:$cargo_key})
            CALL (g) { MATCH (v:WHGoodsState)-[:OF_CARGO]->(g) WHERE v.seq<=$seq
                RETURN v.location_id AS location_id,v.event_seq AS event_seq,v.sim_time AS sim_time
                ORDER BY v.event_seq DESC LIMIT 1 }
            CALL (g) { MATCH (h:WHGoodsState)-[:OF_CARGO]->(g)
                WHERE h.seq<=$seq AND h.event_seq>$after
                WITH h ORDER BY h.event_seq LIMIT $limit
                RETURN collect(h{.event_seq,.seq,.sim_time,.kind,.location_id,.event_key}) AS history }
            RETURN location_id,event_seq,sim_time,history''',
            {'run':run_id,'cargo_key':run_id+':'+goods_id,'seq':cutoff,'after':after_event_seq,'limit':limit+1})
        if not rows:
            return {'run_id':run_id,'goods_id':goods_id,'seq':cutoff,'graph_committed_seq':head,
                    'current_as_of':None,'rows':[],'has_more':False,'next_after_event_seq':None}
        value = rows[0]
        history = value['history']
        more = len(history)>limit
        history = history[:limit]
        return {'run_id':run_id,'goods_id':goods_id,'seq':cutoff,'graph_committed_seq':head,
                'current_as_of':{k:value[k] for k in ['location_id','event_seq','sim_time']},
                'rows':history,'has_more':more,
                'next_after_event_seq':history[-1]['event_seq'] if more else None}

    def robot_context(self, run_id, robot_id, seq=None):
        _integer(robot_id,'robot ID',1,100)
        cutoff,head = self._cutoff(run_id,seq)
        rows = self.mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
            MATCH (v:WHAgentState {key:$state_key})
            OPTIONAL MATCH (t:WHTask {key:$run+':'+v.task_id})
            CALL (t) { OPTIONAL MATCH (ts:WHTaskState)-[:OF_TASK]->(t) WHERE ts.seq<=$seq
                RETURN ts.payload AS task_payload ORDER BY ts.seq DESC LIMIT 1 }
            RETURN v.payload AS robot_payload,task_payload''',
            {'run':run_id,'seq':cutoff,'state_key':f'{run_id}:{cutoff}:{robot_id}'})
        if not rows:
            raise ValueError('robot snapshot absent at requested committed cutoff')
        return {'run_id':run_id,'robot_id':robot_id,'seq':cutoff,'graph_committed_seq':head,
                'robot':_payload(rows[0]['robot_payload']),'task':_payload(rows[0].get('task_payload'))}

    def cooperation_history(self, run_id, task_id=None, seq=None, limit=100, after_event_seq=0):
        if task_id is not None:
            _identity(task_id,'task ID')
        _integer(limit,'limit',1,1000)
        _integer(after_event_seq,'event cursor',0)
        cutoff,head = self._cutoff(run_id,seq)
        start = ('MATCH (t:WHTask {key:$task_key})<-[:ABOUT_TASK]-(e:WHEvent)' if task_id is not None else
                 'MATCH (r)<-[:IN_RUN]-(e:WHEvent)')
        rows = self.mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
            '''+start+'''
            WHERE e.run_id=$run AND e.commit_seq<=$seq AND e.event_seq>$after AND e.kind IN $kinds
            RETURN e.event_seq AS event_seq,e.commit_seq AS seq,e.payload AS payload
            ORDER BY e.event_seq LIMIT $limit''',
            {'run':run_id,'task_key':run_id+':'+task_id if task_id is not None else None,
             'seq':cutoff,'after':after_event_seq,'kinds':COOPERATION_KINDS,'limit':limit+1})
        more = len(rows)>limit
        rows = rows[:limit]
        return {'run_id':run_id,'task_id':task_id,'seq':cutoff,'graph_committed_seq':head,
                'rows':[{'event_seq':row['event_seq'],'seq':row['seq'],'event':_payload(row['payload'])} for row in rows],
                'has_more':more,'next_after_event_seq':rows[-1]['event_seq'] if more else None}
