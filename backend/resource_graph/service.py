"""Bounded historical rack inventory, joined to actual committed custody."""
import json
import math

from .model import VERSION, ident, integer


RACK_INVENTORY_QUERY = '''MATCH (r:WHRun {run_id:$run})
    WHERE r.committed_seq >= $seq AND r.resource_graph_seq >= $seq
      AND r.resource_graph_version=$version
    MATCH (step:WHStep {key:$run+':'+toString($seq)})
    MATCH (r)-[:USES_LAYOUT]->(w:Warehouse)-[:HAS_RACK]->(rack:Rack {rack_id:$rack})
    WHERE w.id=r.resource_graph_warehouse_id
    CALL (rack) {
        MATCH (rack)-[:HAS_SLOT]->(l:Location)
        WITH l ORDER BY l.location_id LIMIT $budget
        CALL (l) {
            OPTIONAL MATCH (v:WHSlotState {run_id:$run,slot_id:l.location_id})
            USING INDEX v:WHSlotState(run_id,slot_id,seq) WHERE v.seq <= $seq
            WITH v ORDER BY v.run_id DESC,v.slot_id DESC,v.seq DESC LIMIT 1
            RETURN v }
        CALL (v) {
            OPTIONAL MATCH (g:WHGoodsState {run_id:$run,goods_id:v.cargo_id})
            USING INDEX g:WHGoodsState(run_id,goods_id,event_seq)
            WHERE g.seq <= $seq AND g.event_seq >= 0
            WITH g ORDER BY g.run_id DESC,g.goods_id DESC,g.event_seq DESC LIMIT 1
            RETURN g }
        RETURN collect({location_id:l.location_id,level:l.level,face:l.face,bay:l.bay,
            slot_seq:v.seq,slot_payload:v.payload,occupied:v.occupied,goods_id:v.cargo_id,
            custody_location:g.location_id,custody_seq:g.seq,custody_event_seq:g.event_seq,
            custody_time:g.sim_time,custody_kind:g.kind}) AS slots }
    RETURN r.run_id AS run_id,r.committed_seq AS head,r.resource_graph_seq AS resource_head,
        r.resource_graph_dataset_id AS dataset_id,w.id AS warehouse_id,
        rack.rack_id AS rack_id,rack.capacity AS capacity,step.checksum AS batch_checksum,
        step.sim_time AS sim_time,slots'''


class RackInventoryService:
    def __init__(self, mirror, timeout=5):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('invalid resource query timeout')
        self.mirror, self.timeout = mirror, timeout

    def inventory(self, run, seq, rack_id, *, limit=512, include_empty=True):
        ident(run, 'run'); ident(rack_id, 'rack'); integer(seq, 'cutoff seq', 1)
        integer(limit, 'slot limit', 1)
        if limit > 1000 or type(include_empty) is not bool:
            raise ValueError('invalid rack inventory budget/options')
        rows = self.mirror.query(RACK_INVENTORY_QUERY, {'run': run, 'seq': seq, 'rack': rack_id,
            'budget': limit + 1, 'version': VERSION}, timeout=self.timeout)
        if len(rows) != 1:
            raise ValueError('rack or committed resource cutoff absent')
        row = rows[0]
        if row['run_id'] != run or row['rack_id'] != rack_id:
            raise ValueError('foreign rack inventory identity')
        for name in ('head', 'resource_head'):
            if integer(row[name], name, 1) < seq:
                raise ValueError('resource inventory watermark is behind requested cutoff')
        checksum = ident(row['batch_checksum'], 'batch checksum')
        if len(checksum) != 64 or any(c not in '0123456789abcdef' for c in checksum):
            raise ValueError('invalid resource source checksum')
        if isinstance(row['sim_time'], bool) or not isinstance(row['sim_time'], (int, float)) or not math.isfinite(row['sim_time']) or row['sim_time'] < 0:
            raise ValueError('invalid resource source time')
        capacity = integer(row['capacity'], 'rack capacity', 1)
        if len(row['slots']) > limit:
            raise ValueError('rack slot budget exceeded; explicitly raise limit')
        if len(row['slots']) != capacity:
            raise ValueError('rack inventory incomplete static slot mapping')
        slots, ids = [], set()
        for item in row['slots']:
            location = ident(item['location_id'], 'slot')
            if location in ids:
                raise ValueError('duplicate rack inventory slot')
            ids.add(location)
            if integer(item['slot_seq'], 'slot seq', 1) > seq or type(item['occupied']) is not bool:
                raise ValueError('missing/future/invalid rack slot state')
            payload = json.loads(item['slot_payload']) if isinstance(item['slot_payload'], str) else item['slot_payload']
            if (not isinstance(payload, dict) or type(payload.get('occupied')) is not bool
                    or (payload.get('slot_id'),payload.get('occupied'),payload.get('cargo_id')) != (location,item['occupied'],item['goods_id'])):
                raise ValueError('rack slot payload differs from selected source properties')
            for name in ('level', 'face', 'bay'):
                integer(item[name], name, 1)
            if item['occupied']:
                ident(item['goods_id'], 'goods')
                if item['custody_location'] != location or integer(item['custody_seq'], 'custody seq', 1) > seq:
                    raise ValueError('slot state and committed cargo custody disagree')
                integer(item['custody_event_seq'], 'custody event sequence', 1)
                stamp = item['custody_time']
                if isinstance(stamp, bool) or not isinstance(stamp, (int, float)) or not math.isfinite(stamp) or not 0 <= stamp <= row['sim_time']:
                    raise ValueError('invalid/future resource custody time')
            elif item['goods_id'] is not None:
                raise ValueError('empty slot still references cargo')
            slots.append({key: item[key] for key in ('location_id','level','face','bay','slot_seq','occupied','goods_id',
                'custody_location','custody_seq','custody_event_seq','custody_time','custody_kind')})
        occupied = [item for item in slots if item['occupied']]
        if len({item['goods_id'] for item in occupied}) != len(occupied):
            raise ValueError('same cargo occupies multiple rack slots')
        return {'version': VERSION, 'run_id': run, 'seq': seq, 'rack_id': rack_id,
            'warehouse_id': row['warehouse_id'], 'dataset_id': row['dataset_id'],
            'graph_committed_seq': row['head'], 'resource_committed_seq': row['resource_head'],
            'batch_checksum': row['batch_checksum'], 'sim_time': row['sim_time'],
            'capacity': capacity, 'occupied': len(occupied), 'empty': capacity - len(occupied),
            'slots': slots if include_empty else occupied, 'read_only': True,
            'scope': 'historical committed prefix; INITIAL_AT is not current inventory'}
