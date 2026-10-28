"""Run-scoped source-verified Neo4j history reader for live decisions."""
import json
import zlib
from ..knowledge import digest
from dataclasses import asdict
from ..query_v2.service_fragments import FragmentQueryService
from .policy import DecisionRejected


class GraphHistoryProvider:
    def __init__(self, journal, query_store, *, max_lag_seconds=30):
        self.journal, self.store = journal, query_store
        if type(max_lag_seconds) is not int or max_lag_seconds < 0:
            raise ValueError('nonnegative knowledge lag budget required')
        self.max_lag_seconds = max_lag_seconds

    def context(self, sim, task):
        status = self.journal.status(sim.run_id)
        seq = status['mirrored_seq']
        if type(seq) is not int or type(status['durable_seq']) is not int or seq<=0 or seq != status['durable_seq'] or status['last_error']:
            raise DecisionRejected('knowledge mirror is not at this writer committed head')
        row = self.journal.db.execute('SELECT checksum,batch FROM commits WHERE run_id=? AND seq=?',
                                      (sim.run_id,seq)).fetchone()
        if row is None:
            raise DecisionRejected('knowledge head absent from local source')
        batch=json.loads(zlib.decompress(row[1]))
        if digest(batch)!=row[0] or batch['run_id']!=sim.run_id or type(batch['seq']) is not int or batch['seq']!=seq:
            raise DecisionRejected('knowledge source checksum or identity mismatch')
        if digest(batch['metadata'])!=digest(sim.run_metadata) or digest(batch['config'])!=digest(asdict(sim.config)):
            raise DecisionRejected('knowledge source configuration/metadata differs from live run')
        if type(batch.get('sim_time')) is not int or batch['sim_time']<0:
            raise DecisionRejected('invalid source simulation time')
        lag=sim.elapsed-batch['sim_time']
        if not 0 <= lag <= self.max_lag_seconds:
            raise DecisionRejected('knowledge observation is future or too old')
        value=self.store.cooperation_context(sim.run_id,seq,task['task_id'],task['goods_id'])
        if (value['batch_checksum'] != row[0] or type(value.get('seq')) is not int or value['seq']!=seq
                or type(value.get('graph_committed_seq')) is not int or value['graph_committed_seq'] != seq
                or value.get('run_id')!=sim.run_id or value.get('task_id')!=task['task_id']
                or value.get('goods_id')!=task['goods_id']):
            raise DecisionRejected('knowledge graph and source watermark differ')
        checkpoint=self.journal.latest_checkpoint(sim.run_id)
        original=checkpoint['tasks'].get(task['task_id']) if checkpoint and checkpoint['knowledge_seq']==seq else None
        if original is None or digest(original)!=digest(value.get('focus_task')):
            raise DecisionRejected('shared task payload differs from committed source')
        value['source_sim_time']=batch['sim_time']
        value['observation_lag_seconds']=lag
        return value

    def close(self):
        self.store.close()
