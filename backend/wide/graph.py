"""Wide-layout temporal graph: canonical bays, aliases and real cargo custody.

Only warehouse-prefixed labels and the requested Run are touched. The base
Robot/Task/Step/Event writer is reused; day1's 12+12 charger selection is not.
"""
from ..knowledge import KnowledgeError
from ..knowledge_graph import Neo4jMirror
from ..scheduling_v3.graph import V3Mirror, _handoff_row, _robot_id, audit_helpers


RECOVERY_KINDS = {
    'traffic_retimed', 'traffic_rerouted', 'robot_stopped', 'assistance_requested',
    'handoff_reserved', 'robot_reenabled', 'robot_stop_cleared', 'robot_power_isolated',
}


def _affected(event):
    values = event.get('affected_robot_ids', event.get('affected_robots',
        [event['robot_id']] if event.get('robot_id') is not None else []))
    if not isinstance(values, (list, tuple)):
        raise ValueError('affected robot IDs must be an array')
    return sorted({_robot_id(value) for value in values})


def _registry(scene):
    docks = scene.get('docks', [])
    homes = scene.get('homes', [])
    if len(docks) != 100 or len(homes) != 100:
        raise ValueError('wide graph requires 100 physical bays and 100 home aliases')
    by_id = {dock['id']: dock for dock in docks}
    if len(by_id) != 100 or len({home['id'] for home in homes}) != 100:
        raise ValueError('duplicate bay or alias identity')
    if set(by_id) & {home['id'] for home in homes}:
        raise ValueError('canonical bay and alias names must remain distinct')
    if len({(d['point']['x'],d['point']['y']) for d in docks}) != 100 or any(d.get('capacity',1)!=1 for d in docks):
        raise ValueError('physical bays must have unique positions and capacity one')
    claimed = set()
    for home in homes:
        parent = home.get('alias_of')
        if parent not in by_id or parent in claimed or home['point'] != by_id[parent]['point']:
            raise ValueError('home alias must uniquely identify its colocated physical bay')
        claimed.add(parent)
    return docks, homes


def _locations(run, scene, slots):
    docks, homes = _registry(scene)
    rows = [{'key':run+':'+slot['slot_id'], 'run_id':run, 'location_id':slot['slot_id'],
             'kind':'slot', 'x':slot['x'], 'y':slot['y']} for slot in slots]
    rows.extend({'key':run+':'+dock['id'], 'run_id':run, 'location_id':dock['id'],
                 'kind':'charger', 'bay_id':dock['id'], 'physical_bay':True, 'capacity':1,
                 'x':dock['point']['x'], 'y':dock['point']['y']} for dock in docks)
    rows.extend({'key':run+':'+home['id'], 'run_id':run, 'location_id':home['id'],
                 'kind':'home-alias', 'physical_bay':False, 'alias_of':home['alias_of'],
                 'canonical_location_id':home['alias_of'],
                 'x':home['point']['x'], 'y':home['point']['y']} for home in homes)
    rows.extend({'key':run+':'+p['id'], 'run_id':run, 'location_id':p['id'],
                 'kind':p['kind'], 'x':p['x'], 'y':p['y']} for p in scene['stations'])
    rows.extend({'key':run+f':ROBOT-{i}', 'run_id':run, 'location_id':f'ROBOT-{i}',
                 'kind':'robot-custody'} for i in range(1,101))
    rows.append({'key':run+':SHIPPED', 'run_id':run, 'location_id':'SHIPPED', 'kind':'warehouse-boundary'})
    return rows


class WideMirror(V3Mirror):
    @staticmethod
    def check_batches(batches):
        if not batches:
            return
        run = batches[0]['run_id']
        for batch in batches:
            if batch['run_id'] != run:
                raise KnowledgeError('mixed runs in wide graph transaction')
            if batch['metadata'].get('domain') not in {
                'warehouse-inbound-outbound-day1', 'warehouse-wide-inbound-outbound'}:
                raise ValueError('wrong wide warehouse graph domain')
            _registry(batch['metadata']['scene'])
            count = batch['config']['chargers']
            if isinstance(count,bool) or not isinstance(count,int) or not 1 <= count <= 100:
                raise ValueError('invalid configured wide charger count')
            for event in batch['events']:
                if event['kind'] == 'cargo_handoff':
                    _handoff_row(run,event)
                if event['kind'] in RECOVERY_KINDS:
                    _affected(event)

    @staticmethod
    def _apply(tx, batches):
        if not batches:
            return
        WideMirror.check_batches(batches)
        run = batches[0]['run_id']
        current = tx.run('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq',run=run).single()
        head = int(current['seq'] or 0) if current else 0
        # Sequence gaps/conflicting replays are validated by the base writer.
        Neo4jMirror._apply(tx,batches)
        forward = [batch for batch in batches if batch['seq'] > head]
        if not forward:
            return
        scene = forward[0]['metadata']['scene']
        docks, homes = _registry(scene)
        if forward[0]['seq'] == 1:
            locations = _locations(run,scene,forward[0]['slots'])
            tx.run('''UNWIND $rows AS p MATCH (r:WHRun {run_id:$run})
                MERGE (l:WHLocation {key:p.key}) SET l+=p MERGE (l)-[:IN_RUN]->(r)''',
                rows=locations,run=run).consume()
            tx.run('''UNWIND $rows AS p MATCH (l:WHLocation {key:p.key})
                SET l:WHCharger:WHPhysicalBay''',
                rows=[{'key':run+':'+dock['id']} for dock in docks]).consume()
            tx.run('''UNWIND $rows AS p MATCH (a:WHLocation {key:p.key}), (b:WHLocation {key:p.bay_key})
                WHERE b:WHPhysicalBay
                SET a:WHLocationAlias MERGE (a)-[:ALIAS_OF]->(b)''', rows=[{
                    'key':run+':'+home['id'],'bay_key':run+':'+home['alias_of']} for home in homes]).consume()
            tx.run('''UNWIND $rows AS p MATCH (a:WHRobot {key:p.robot_key}), (b:WHLocation {key:p.bay_key})
                WHERE b:WHPhysicalBay
                MERGE (a)-[:INITIAL_BAY]->(b)''', rows=[{
                    'robot_key':run+':'+str(i+1),'bay_key':run+':'+home['alias_of']} for i,home in enumerate(homes)]).consume()
            tx.run('''UNWIND $rows AS p MATCH (s:WHSlot {key:p.key}), (l:WHLocation {key:p.key})
                MERGE (s)-[:AT_LOCATION]->(l)''',rows=[{'key':run+':'+s['slot_id']} for s in forward[0]['slots']]).consume()

        tasks, goods, charger_states, handoffs, recoveries = [], [], [], [], []
        for batch in forward:
            for task in batch['tasks']:
                tasks.append({'key':run+':'+task['task_id'], 'goods':run+':'+task['goods_id'],
                    'goods_id':task['goods_id'], 'source':run+':'+task['source_id'], 'target':run+':'+task['target_id']})
            for event in batch['events']:
                if 'goods_id' in event and 'location_id' in event:
                    goods.append({'key':event['event_id']+':custody', 'run_id':run, 'goods_id':event['goods_id'],
                        'cargo_key':run+':'+event['goods_id'], 'location_key':run+':'+event['location_id'],
                        'location_id':event['location_id'], 'event_key':event['event_id'],
                        'step_key':run+':'+str(batch['seq']), 'event_seq':event['event_seq'],
                        'seq':batch['seq'], 'sim_time':event['sim_time'], 'kind':event['kind']})
                if event['kind'] == 'cargo_handoff':
                    handoffs.append(_handoff_row(run,event))
                if event['kind'] in RECOVERY_KINDS:
                    recoveries.append({'event_key':event['event_id'],'kind':event['kind'],
                        'reason':event.get('reason'),'affected_robot_ids':_affected(event)})
            for dock in docks[:batch['config']['chargers']]:
                center = dock['point']
                occupants = [r for r in batch['robots'] if
                    (r['x']-center['x'])**2+(r['y']-center['y'])**2 <= 1e-10]
                if len(occupants) > 1:
                    raise ValueError('multiple bodies occupy one canonical wide bay')
                reserved = [r['robot_id'] for r in batch['robots'] if r.get('dock_id') == dock['id']]
                charging = [r['robot_id'] for r in occupants if r['stage']=='charging']
                charger_states.append({'key':run+':'+str(batch['seq'])+':'+dock['id'], 'run_id':run,
                    'charger_id':dock['id'],'physical_bay_id':dock['id'], 'capacity':1,
                    'charger_key':run+':'+dock['id'],'step_key':run+':'+str(batch['seq']),
                    'seq':batch['seq'],'sim_time':batch['sim_time'], 'reserved_robot_ids':reserved,
                    'occupant_robot_id':occupants[0]['robot_id'] if occupants else None,
                    'occupant_robot_ids':[r['robot_id'] for r in occupants],
                    'charging_robot_id':charging[0] if charging else None,
                    'occupant_stage':occupants[0]['stage'] if occupants else None})

        tx.run('''UNWIND $rows AS p MATCH (t:WHTask {key:p.key}),
            (a:WHLocation {key:p.source}), (b:WHLocation {key:p.target})
            MERGE (g:WHCargo {key:p.goods}) SET g.run_id=$run,g.cargo_id=p.goods_id
            MERGE (t)-[:MOVES]->(g) MERGE (t)-[:FROM_LOCATION]->(a) MERGE (t)-[:TO_LOCATION]->(b)''',
            rows=tasks,run=run).consume()
        tx.run('''UNWIND $rows AS p WITH p ORDER BY p.event_seq
            MATCH (l:WHLocation {key:p.location_key}), (e:WHEvent {key:p.event_key}), (s:WHStep {key:p.step_key})
            MERGE (g:WHCargo {key:p.cargo_key}) SET g.run_id=p.run_id,g.cargo_id=p.goods_id,g.current_location_id=p.location_id
            MERGE (v:WHGoodsState {key:p.key}) SET v+=p
            MERGE (v)-[:OF_CARGO]->(g) MERGE (v)-[:AT_LOCATION]->(l)
            MERGE (v)-[:CAUSED_BY]->(e) MERGE (v)-[:IN_STEP]->(s)''',rows=goods).consume()
        tx.run('''UNWIND $rows AS p MATCH (d:WHCharger {key:p.charger_key}), (s:WHStep {key:p.step_key})
            MERGE (v:WHChargerState {key:p.key}) SET v+=p
            MERGE (v)-[:OF_CHARGER]->(d) MERGE (v)-[:IN_STEP]->(s)''',rows=charger_states).consume()
        if handoffs:
            tx.run('''UNWIND $rows AS p MATCH (e:WHEvent {key:p.event_key}),
                (d:WHRobot {key:p.donor_key}), (a:WHRobot {key:p.receiver_key}),
                (t:WHTask {key:p.task_key}), (g:WHCargo {key:p.cargo_key})
                SET e.donor_robot_id=p.donor_robot_id,e.receiver_robot_id=p.receiver_robot_id
                MERGE (e)-[:DONOR]->(d) MERGE (e)-[:RECEIVER]->(a)
                MERGE (e)-[:ABOUT_TASK]->(t) MERGE (e)-[:TRANSFERS]->(g)''',rows=handoffs).consume()
        if recoveries:
            tx.run('''UNWIND $rows AS p MATCH (e:WHEvent {key:p.event_key})
                SET e.affected_robot_ids=p.affected_robot_ids,e.reason=p.reason
                WITH e,p UNWIND p.affected_robot_ids AS owner
                MATCH (a:WHRobot {key:$run+':'+toString(owner)})
                MERGE (e)-[:AFFECTS]->(a)''',rows=recoveries,run=run).consume()


def audit_wide(mirror_or_store, sim, *, expected_handoffs=None):
    mirror = getattr(mirror_or_store,'mirror',mirror_or_store)
    if mirror is None:
        return None
    run = sim.run_id
    counts = {}
    for label in ['WHRobot','WHTask','WHPhysicalBay','WHLocationAlias','WHCharger']:
        counts[label] = mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
            CALL (r) { MATCH (r)<-[:IN_RUN]-(n:''' + label + ''') RETURN count(n) AS n } RETURN n''',{'run':run})[0]['n']
    aliases = mirror.query('''MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id)
        MATCH (r)<-[:IN_RUN]-(a:WHLocationAlias)
        OPTIONAL MATCH (a)-[:ALIAS_OF]->(b:WHPhysicalBay)
        WITH a,count(b) AS targets RETURN count(a) AS aliases,sum(CASE WHEN targets=1 THEN 0 ELSE 1 END) AS invalid''',{'run':run})[0]
    inside = mirror.query('''MATCH (g:WHCargo {run_id:$run}) USING INDEX g:WHCargo(run_id)
        WHERE g.current_location_id IS NOT NULL AND g.current_location_id<>'SHIPPED' RETURN count(g) AS n''',{'run':run})[0]['n']
    counts['inside'] = inside
    if (counts['WHRobot'] != 100 or counts['WHPhysicalBay'] != 100 or counts['WHCharger'] != 100 or
        counts['WHLocationAlias'] != 100 or counts['WHTask'] != sim.cursor or inside != sim.inside or
        aliases['aliases'] != 100 or aliases['invalid'] != 0):
        raise AssertionError('wide graph/source physical resource or custody mismatch')
    counts['collaboration'] = audit_helpers(mirror,run,expected_handoffs=expected_handoffs)
    return counts
