"""Actual coordinator hooks: shared history gates planning and physical transfer."""
from copy import deepcopy
import time
from ..knowledge import digest
from ..real_scale.writer import RealSimulation
from . import VERSION
from .policy import authorize, DecisionRejected


class KnowledgeSimulation(RealSimulation):
    def __init__(self,*args,checkpoint=None,knowledge_mode='required',**kwargs):
        if knowledge_mode not in ('required','rules'):
            raise ValueError('explicit knowledge mode required')
        self.knowledge_mode=knowledge_mode
        self.history_provider=None
        self._planning_decision=None
        self._transfer_tokens={}
        self.knowledge_stats={'queries':0,'approved_plans':0,'approved_transfers':0,'rejected':0,
                              'query_seconds':0.,'helper_order_changes':0}
        super().__init__(*args,checkpoint=None,**kwargs)
        self.runtime_identity={'version':VERSION,'mode':knowledge_mode,'source':self.real_identity['execution_sha256']}
        self.run_metadata['knowledge_runtime']=deepcopy(self.runtime_identity)
        self.run_metadata['execution_code_sha256']=digest(self.runtime_identity)
        if checkpoint is not None:
            self.restore(checkpoint)

    def _stamp(self,donor,task):
        robot=self.robots[donor]
        return digest({'robot':robot,'task':task,'goods':self.goods.get(task['goods_id']),
                       'assist':self.assists.get(donor)})

    def _decision(self,donor,task,receiver=None):
        if self.history_provider is None:
            raise DecisionRejected('required history provider not attached')
        before=self._stamp(donor,task)
        started=time.perf_counter()
        self.knowledge_stats['queries']+=1
        try:
            context=self.history_provider.context(self,task)
            result=authorize(context,run=self.run_id,donor=donor,task=task,goods=task['goods_id'],receiver=receiver)
            if before!=self._stamp(donor,task):
                raise DecisionRejected('live state changed while querying knowledge')
            if (self.goods.get(task['goods_id'])!=f'ROBOT-{donor}' or task['robot_id']!=donor
                    or not self.robots[donor]['carrying'] or self.robots[donor]['cargo_id']!=task['goods_id']):
                raise DecisionRejected('live custody changed before action')
            self.event('knowledge_decision',donor,task['task_id'],decision=result['action'],
                knowledge_seq=result['seq'],knowledge_batch_checksum=result['batch_checksum'],
                context_sha256=result['context_sha256'],history_event_seqs=result['history_event_seqs'],
                receiver_robot_id=receiver,mode='required')
            return result
        except Exception:
            self.knowledge_stats['rejected']+=1
            raise
        finally:
            self.knowledge_stats['query_seconds']+=time.perf_counter()-started

    def request_assistance(self,donor_id,reason='low_energy'):
        donor=self.robots[donor_id]
        if self.knowledge_mode=='rules' or not donor['carrying'] or donor_id in self.assists:
            return super().request_assistance(donor_id,reason)
        task=self.tasks[donor['task_id']]
        self._planning_decision=self._decision(donor_id,task)
        self.knowledge_stats['approved_plans']+=1
        try:
            ok=super().request_assistance(donor_id,reason)
            if ok:
                self.event('knowledge_action_applied',donor_id,task['task_id'],
                    action='handoff_plan_reserved',knowledge_seq=self._planning_decision['seq'],
                    receiver_robot_id=self.assists[donor_id]['receiver'])
            return ok
        finally:
            self._planning_decision=None

    def _assistance_helpers(self,donor,task):
        original=super()._assistance_helpers(donor,task)
        if self._planning_decision is None:
            return original
        previous=set(self._planning_decision['historical_donor_robot_ids'])
        ordered=sorted(original,key=lambda r:r['robot_id'] in previous)
        self.knowledge_stats['helper_order_changes']+=int([r['robot_id'] for r in ordered]!=[r['robot_id'] for r in original])
        return ordered

    def marker(self,robot,job,marker,task_id=None):
        if marker!='handoff_received' or self.knowledge_mode=='rules':
            return super().marker(robot,job,marker,task_id)
        donor=job['assist_donor']
        task=self.tasks[task_id or job['task_id']]
        key=(donor,robot['robot_id'],task['task_id'],self.elapsed)
        result=self._transfer_tokens.pop(key,None)
        if result is None:
            raise DecisionRejected('physical transfer lacks pre-step knowledge authorization')
        assist=self.assists.get(donor)
        if not assist or assist['receiver']!=robot['robot_id'] or assist['task_id']!=task['task_id']:
            raise DecisionRejected('live reservation changed before transfer')
        # Original transfer_custody rechecks pose, energy-related availability,
        # unique owner and receiver; the physical transaction remains intact.
        super().marker(robot,job,marker,task_id)
        self.knowledge_stats['approved_transfers']+=1
        self.event('knowledge_action_applied',robot['robot_id'],task['task_id'],
            action='cargo_transferred',knowledge_seq=result['seq'],
            donor_robot_id=donor,receiver_robot_id=robot['robot_id'],goods_id=task['goods_id'])

    def step(self):
        # Query before ANY robot moves. A rejected/unavailable knowledge read
        # leaves the previous physical commit intact, rather than persisting
        # a partially executed multi-robot step.
        self._transfer_tokens={}
        try:
            if self.knowledge_mode=='required':
                for owner,job in list(self.jobs.items()):
                    at=self.elapsed-job['start']
                    for part in job['parts']:
                        if part['end']==at+1 and part.get('marker')=='handoff_received':
                            donor=job['assist_donor']
                            task=self.tasks[part.get('task_id') or job['task_id']]
                            assist=self.assists.get(donor)
                            receiver=self.robots[owner]
                            if (not assist or assist['receiver']!=owner or assist['task_id']!=task['task_id']
                                    or receiver['carrying'] or receiver.get('cargo_id') is not None
                                    or receiver.get('task_id') not in (None,task['task_id'])):
                                raise DecisionRejected('live transfer participants/reservation invalid before step')
                            key=(donor,owner,task['task_id'],self.elapsed+1)
                            self._transfer_tokens[key]=self._decision(donor,task,owner)
            return super().step()
        finally:
            self._transfer_tokens={}

    def knowledge_boundary_due(self):
        """Request publication before a physical transfer at any sampling cadence.

        The coordinator calls this before step(), never from inside a partly
        executed physical step. Only the publication cadence changes.
        """
        if self.knowledge_mode!='required':
            return False
        return any(part['end']==self.elapsed-job['start']+1
                   and part.get('marker')=='handoff_received'
                   for job in self.jobs.values() for part in job['parts'])

    def checkpoint(self):
        state=super().checkpoint()
        state['knowledge_runtime']={'identity':deepcopy(self.runtime_identity),'stats':deepcopy(self.knowledge_stats)}
        return state

    def restore(self,state):
        if state.get('knowledge_runtime',{}).get('identity')!=self.runtime_identity:
            raise ValueError('knowledge decision mode/source mismatch on restore')
        super().restore(state)
        self.knowledge_stats=deepcopy(state['knowledge_runtime']['stats'])

    def execution_status(self):
        value=super().execution_status()
        value['knowledge_runtime']={'identity':deepcopy(self.runtime_identity),'stats':deepcopy(self.knowledge_stats)}
        return value
