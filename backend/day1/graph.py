"""Additive run-scoped spatial, cargo custody and charging history for day-one."""
from ..knowledge_graph import Neo4jMirror

class DayOneMirror(Neo4jMirror):
    def ensure_schema(self):
        super().ensure_schema()
        with self.driver.session(database=self.database) as s:
            for label in ['WHLocation','WHGoodsState','WHCharger','WHChargerState']:
                s.run(f'CREATE CONSTRAINT {label.lower()}_key IF NOT EXISTS FOR (n:{label}) REQUIRE n.key IS UNIQUE').consume()
            s.run('CREATE INDEX whgoodsstate_history IF NOT EXISTS FOR (n:WHGoodsState) ON (n.run_id,n.goods_id,n.event_seq)').consume()
            s.run('CREATE INDEX whchargerstate_history IF NOT EXISTS FOR (n:WHChargerState) ON (n.run_id,n.charger_id,n.seq)').consume()
            # A run-only COUNT cannot safely use a composite index whose other fields may be absent.
            for label in ['WHEvent','WHTaskState','WHCargo']:
                s.run(f'CREATE INDEX day1_{label.lower()}_run IF NOT EXISTS FOR (n:{label}) ON (n.run_id)').consume()
            s.run('CALL db.awaitIndexes(120)').consume()
    @staticmethod
    def _apply(tx,batches):
        DayOneMirror.check_batches(batches)
        current=tx.run('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq',run=batches[0]['run_id']).single()
        head=int(current['seq'] or 0) if current else 0
        Neo4jMirror._apply(tx,batches)
        batches=[b for b in batches if b['seq']>head]
        if not batches:return  # Do not regress mutable cargo locations on idempotent replay.
        run=batches[0]['run_id'];scene=batches[0]['metadata']['scene']
        locations=[]
        if batches[0]['seq']==1:
            for b in batches[0]['slots']:
                locations.append({'key':run+':'+b['slot_id'],'run_id':run,'location_id':b['slot_id'],'kind':'slot','x':b['x'],'y':b['y']})
            for kind,items in [('home',scene['homes']),('charger',scene['docks'])]:
                for b in items:locations.append({'key':run+':'+b['id'],'run_id':run,'location_id':b['id'],'kind':kind,'x':b['point']['x'],'y':b['point']['y']})
            for b in scene['stations']:locations.append({'key':run+':'+b['id'],'run_id':run,'location_id':b['id'],'kind':b['kind'],'x':b['x'],'y':b['y']})
            for i in range(1,101):locations.append({'key':run+f':ROBOT-{i}','run_id':run,'location_id':f'ROBOT-{i}','kind':'robot-custody'})
            locations.append({'key':run+':SHIPPED','run_id':run,'location_id':'SHIPPED','kind':'warehouse-boundary'})
            tx.run('''UNWIND $rows AS p MATCH (r:WHRun {run_id:$run})
                MERGE (l:WHLocation {key:p.key}) SET l+=p MERGE (l)-[:IN_RUN]->(r)''',rows=locations,run=run).consume()
            tx.run('''MATCH (l:WHLocation {run_id:$run,kind:'charger'}) SET l:WHCharger''',run=run).consume()
            tx.run('''MATCH (s:WHSlot {run_id:$run}) MATCH (l:WHLocation {key:$run+':'+s.slot_id})
                MERGE (s)-[:AT_LOCATION]->(l)''',run=run).consume()
        tasks=[];goods=[];chargers=[]
        for b in batches:
            for t in b['tasks']:tasks.append({'key':run+':'+t['task_id'],'goods':run+':'+t['goods_id'],'goods_id':t['goods_id'],'source':run+':'+t['source_id'],'target':run+':'+t['target_id']})
            for e in b['events']:
                if 'goods_id' not in e or 'location_id' not in e:continue
                goods.append({'key':e['event_id']+':custody','run_id':run,'goods_id':e['goods_id'],'cargo_key':run+':'+e['goods_id'],
                    'location_key':run+':'+e['location_id'],'location_id':e['location_id'],'event_key':e['event_id'],
                    'step_key':run+':'+str(b['seq']),'event_seq':e['event_seq'],'seq':b['seq'],'sim_time':e['sim_time'],'kind':e['kind']})
            used={}
            for r in b['robots']:
                if r.get('dock_id'):used.setdefault(r['dock_id'],[]).append(r)
            # Include only configured, usable docks in this run's state history.
            count=b['config']['chargers'];balanced=[]
            for a,c in zip(scene['docks'][:12],scene['docks'][12:]):balanced.extend([a,c])
            for d in balanced[:count]:
                reserved=used.get(d['id'],[]);occupants=[r['robot_id'] for r in reserved if r['stage']=='charging']
                if len(occupants)>1:raise ValueError('multiple occupants of one charger')
                chargers.append({'key':run+':'+str(b['seq'])+':'+d['id'],'run_id':run,'charger_id':d['id'],
                    'charger_key':run+':'+d['id'],'step_key':run+':'+str(b['seq']),'seq':b['seq'],'sim_time':b['sim_time'],
                    'reserved_robot_ids':[r['robot_id'] for r in reserved],'occupant_robot_id':occupants[0] if occupants else None})
        tx.run('''UNWIND $rows AS p MATCH (t:WHTask {key:p.key}),(a:WHLocation {key:p.source}),(b:WHLocation {key:p.target})
            MERGE (g:WHCargo {key:p.goods}) SET g.run_id=$run,g.cargo_id=p.goods_id
            MERGE (t)-[:MOVES]->(g) MERGE (t)-[:FROM_LOCATION]->(a) MERGE (t)-[:TO_LOCATION]->(b)''',rows=tasks,run=run).consume()
        tx.run('''UNWIND $rows AS p WITH p ORDER BY p.event_seq
            MATCH (l:WHLocation {key:p.location_key}),(e:WHEvent {key:p.event_key}),(s:WHStep {key:p.step_key})
            MERGE (g:WHCargo {key:p.cargo_key}) SET g.run_id=p.run_id,g.cargo_id=p.goods_id,g.current_location_id=p.location_id
            MERGE (v:WHGoodsState {key:p.key}) SET v+=p
            MERGE (v)-[:OF_CARGO]->(g) MERGE (v)-[:AT_LOCATION]->(l) MERGE (v)-[:CAUSED_BY]->(e) MERGE (v)-[:IN_STEP]->(s)''',rows=goods).consume()
        tx.run('''UNWIND $rows AS p MATCH (d:WHCharger {key:p.charger_key}),(s:WHStep {key:p.step_key})
            MERGE (v:WHChargerState {key:p.key}) SET v+=p MERGE (v)-[:OF_CHARGER]->(d) MERGE (v)-[:IN_STEP]->(s)''',rows=chargers).consume()
    @staticmethod
    def check_batches(batches):
        if any(b['metadata'].get('domain')!='warehouse-inbound-outbound-day1' for b in batches):raise ValueError('wrong graph domain')
