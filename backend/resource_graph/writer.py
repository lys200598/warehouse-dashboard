"""Same-transaction resource projection hooks; never opens its own driver."""
from ..knowledge import digest
from .model import VERSION, descriptor, validate_forward_batches, materialize_task_records

SCHEMA_QUERIES = tuple(
    f'CREATE CONSTRAINT resource_{label.lower()}_id IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE'
    for label in ('Warehouse', 'Rack', 'Location', 'Dataset', 'Goods', 'WarehouseTask')) + (
    'CREATE INDEX resource_slot_history IF NOT EXISTS FOR (n:WHSlotState) ON (n.run_id,n.slot_id,n.seq)',
    'CREATE INDEX resource_goods_history IF NOT EXISTS FOR (n:WHGoodsState) ON (n.run_id,n.goods_id,n.event_seq)',
)

BOOTSTRAP_WAREHOUSE = '''MERGE (w:Warehouse {id:$warehouse.id}) SET w += $warehouse
    WITH w MATCH (r:WHRun {run_id:$run}) MERGE (r)-[:USES_LAYOUT]->(w)'''
BOOTSTRAP_RACKS = '''UNWIND $rows AS row MATCH (w:Warehouse {id:row.warehouse_id})
    MERGE (rack:Rack {id:row.id}) SET rack += row MERGE (w)-[:HAS_RACK]->(rack)'''
BOOTSTRAP_LOCATIONS = '''UNWIND $rows AS row MATCH (w:Warehouse {id:row.warehouse_id})
    MERGE (l:Location {id:row.id}) SET l += row MERGE (w)-[:HAS_LOCATION]->(l)
    WITH l,row MATCH (actual:WHLocation {key:$run+':'+row.location_id})
    MERGE (actual)-[:INSTANCE_OF]->(l)'''
LABEL_BOUNDARY_GROUPS = '''UNWIND $rows AS row
    MATCH (l:Location {id:row.id}) SET l:BoundaryGroup'''
LINK_RACK_SLOTS = '''UNWIND $rows AS row
    MATCH (rack:Rack {id:row.rack_node_id}),(l:Location {id:row.id}),
          (actual:WHSlot {key:$run+':'+row.location_id})
    MERGE (rack)-[:HAS_SLOT]->(l) MERGE (actual)-[:INSTANCE_OF]->(l)'''
BOOTSTRAP_DATASET = '''MERGE (d:Dataset {id:$dataset.id}) SET d += $dataset
    WITH d MATCH (r:WHRun {run_id:$run}),(w:Warehouse {id:$dataset.warehouse_id})
    MERGE (r)-[:USES_DATASET]->(d) MERGE (d)-[:USES_LAYOUT]->(w)'''
BOOTSTRAP_GOODS = '''UNWIND $rows AS row MATCH (d:Dataset {id:row.dataset_id})
    MERGE (g:Goods {id:row.id}) SET g += row MERGE (d)-[:INCLUDES_GOODS]->(g)
    WITH row,g MATCH (actual:WHCargo {key:$run+':'+row.goods_id})
    MERGE (actual)-[:INSTANCE_OF]->(g)'''
BOOTSTRAP_INITIAL = '''UNWIND $rows AS row
    MATCH (g:Goods {id:row.goods_node_id}),(l:Location {id:row.location_node_id})
    MERGE (g)-[:INITIAL_AT]->(l)'''
BOOTSTRAP_TASKS = '''UNWIND $rows AS row MATCH (d:Dataset {id:row.dataset_id}),
    (g:Goods {id:row.goods_node_id}),(a:Location {id:row.source_node_id}),
    (b:Location {id:row.target_node_id})
    MERGE (t:WarehouseTask {id:row.id}) SET t += row
    MERGE (d)-[:INCLUDES_TASK]->(t) MERGE (t)-[:CONCERNS]->(g)
    MERGE (t)-[:FROM]->(a) MERGE (t)-[:TO]->(b)
    WITH t,row MATCH (actual:WHTask {key:$run+':'+row.task_id})
    MERGE (actual)-[:INSTANCE_OF]->(t)'''

RESOURCE_HEAD = '''MATCH (r:WHRun {run_id:$run})
    RETURN r.resource_graph_seq AS resource_seq,r.resource_graph_version AS version,
        r.resource_graph_descriptor_sha256 AS source_sha256,
        r.resource_graph_warehouse_id AS warehouse_id,r.resource_graph_dataset_id AS dataset_id'''
ADVANCE_RESOURCE_HEAD = '''MATCH (r:WHRun {run_id:$run})
    SET r.resource_graph_seq=$seq,r.resource_graph_version=$version,
        r.resource_graph_descriptor_sha256=$source_sha256,
        r.resource_graph_warehouse_id=$warehouse_id,r.resource_graph_dataset_id=$dataset_id'''


def ensure_schema(session):
    for query in SCHEMA_QUERIES:
        session.run(query).consume()


def apply_resources(tx, forward_batches, *, prepared=None):
    """Append projection within the parent's existing transaction.

    Parent must first validate/filter replays using its WHStep checksum and
    invoke RealMirror._apply on the same transaction. Failures propagate so the
    authoritative rows and resource projection roll back together. There is
    no fallback or independent success watermark.
    """
    if not forward_batches:
        return
    first = forward_batches[0]; run = first['run_id']; last = forward_batches[-1]['seq']
    source_sha = digest(descriptor(first['metadata']))
    header = tx.run(RESOURCE_HEAD, run=run).single()
    if header is None:
        raise ValueError('resource parent WHRun absent in transaction')
    head = header['resource_seq']
    if head is None:
        if first['seq'] != 1:
            raise ValueError('resource graph requires explicit seq1 bootstrap; no implicit old-run migration')
        head = 0
    elif (type(head) is not int or head < 1 or header['version'] != VERSION
          or header['source_sha256'] != source_sha):
        raise ValueError('resource graph version/input/head identity mismatch')
    if head >= last:
        return  # Parent is responsible for validating checksums of replayed source batches.
    if first['seq'] != head + 1:
        raise ValueError('resource graph sequence gap')
    if first['seq'] == 1:
        package = prepared or validate_forward_batches(forward_batches)
        if package['run_id'] != run or package['descriptor_sha256'] != source_sha:
            raise ValueError('prepared resource package does not match transaction')
        warehouse_id = package['warehouse']['id']; dataset_id = package['dataset']['id']
        tx.run(BOOTSTRAP_WAREHOUSE, warehouse=package['warehouse'], run=run).consume()
        tx.run(BOOTSTRAP_RACKS, rows=package['racks']).consume()
        tx.run(BOOTSTRAP_LOCATIONS, rows=package['locations'], run=run).consume()
        groups = [row for row in package['locations'] if row['kind'] == 'logical-boundary']
        if groups:
            tx.run(LABEL_BOUNDARY_GROUPS, rows=groups).consume()
        tx.run(LINK_RACK_SLOTS, rows=[row for row in package['locations'] if row['kind'] == 'slot'], run=run).consume()
        tx.run(BOOTSTRAP_DATASET, dataset=package['dataset'], run=run).consume()
        tx.run(BOOTSTRAP_GOODS, rows=package['goods'], run=run).consume()
        tx.run(BOOTSTRAP_INITIAL, rows=package['inventory']).consume()
        tx.run(BOOTSTRAP_TASKS, rows=package['tasks'], run=run).consume()
    else:
        # Layout/initial inventory were declared in seq1. Task facts are
        # published lazily by the actual simulation, then projected in the
        # same source transaction; never reveal unreleased CSV rows.
        validate_forward_batches(forward_batches)
        warehouse_id = header['warehouse_id']; dataset_id = header['dataset_id']
    for batch in forward_batches:
        if batch['seq'] == 1:
            continue  # Its (possibly empty) published task subset is above.
        incremental = materialize_task_records(batch, warehouse_id, dataset_id)
        if incremental['goods']:
            tx.run(BOOTSTRAP_GOODS, rows=incremental['goods'], run=run).consume()
            tx.run(BOOTSTRAP_TASKS, rows=incremental['tasks'], run=run).consume()
    tx.run(ADVANCE_RESOURCE_HEAD, run=run, seq=last, version=VERSION,
           source_sha256=source_sha, warehouse_id=warehouse_id, dataset_id=dataset_id).consume()
