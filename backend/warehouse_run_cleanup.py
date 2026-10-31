"""Bounded, retryable deletion of one runtime Run without detaching huge hubs."""
from __future__ import annotations

from typing import Any, Callable

# Delete sampled/leaf records before their highly connected Robot and Run hubs.
_LABELS = ('AgentState', 'ExecutionState', 'RuntimeEvent', 'Step', 'RunStat',
           'DispatchState', 'OwnershipChange', 'Handoff', 'Assist', 'Execution', 'Robot')
_SCOPE = '(n.run_id=$run_id OR (n:Run AND n.id=$run_id))'
_READ_EDGES = ('UNWIND $ids AS id MATCH (n) WHERE elementId(n)=id AND ' + _SCOPE +
               ' MATCH (n)-[r]-() RETURN DISTINCT elementId(r) AS id LIMIT $batch')
_DELETE_EDGES = ('UNWIND $ids AS id MATCH (a)-[r]->(b) WHERE elementId(r)=id AND '
    '(a.run_id=$run_id OR (a:Run AND a.id=$run_id) OR '
    'b.run_id=$run_id OR (b:Run AND b.id=$run_id)) '
    'DELETE r RETURN count(r) AS deleted')
_DELETE_NODES = ('UNWIND $ids AS id MATCH (n) WHERE elementId(n)=id AND ' + _SCOPE +
                 ' DELETE n RETURN count(n) AS deleted')


def delete_runtime_run(driver: Any, database: str, run_id: str, *, node_batch: int = 512,
                       relationship_batch: int = 2048,
                       progress: Callable[[dict[str, int]], None] | None = None) -> dict[str, int]:
    """Each transaction has a bounded ID list and deletes nodes or edges, never both.

    The caller must stop/flush producers before deletion. A concurrent writer may
    make plain DELETE fail, safely preserving the remaining data for a retry.
    Already committed batches may be retried without deleting another run.
    """
    if not isinstance(run_id, str) or not run_id:
        raise ValueError('run_id must be nonempty')
    for name, value in (('node_batch', node_batch), ('relationship_batch', relationship_batch)):
        if type(value) is not int or not 1 <= value <= 10000:
            raise ValueError(name + ' must be in 1..10000')
    stats = {'nodes_deleted': 0, 'relationships_deleted': 0, 'node_batches': 0,
             'relationship_batches': 0, 'max_node_batch': 0, 'max_relationship_batch': 0}

    with driver.session(database=database) as session:
        def emit():
            if progress is not None:
                progress(dict(stats))

        def remove(ids):
            while True:
                edges = list(dict.fromkeys(row['id'] for row in session.run(
                    _READ_EDGES, ids=ids, run_id=run_id, batch=relationship_batch).data()))
                if not edges:
                    break
                deleted = session.run(_DELETE_EDGES, ids=edges, run_id=run_id).single()['deleted']
                if not deleted:
                    raise RuntimeError('relationships changed during cleanup; stop producers and retry')
                stats['relationships_deleted'] += deleted
                stats['relationship_batches'] += 1
                stats['max_relationship_batch'] = max(stats['max_relationship_batch'], len(edges))
                emit()
            deleted = session.run(_DELETE_NODES, ids=ids, run_id=run_id).single()['deleted']
            if not deleted:
                raise RuntimeError('nodes changed during cleanup; stop producers and retry')
            stats['nodes_deleted'] += deleted
            stats['node_batches'] += 1
            stats['max_node_batch'] = max(stats['max_node_batch'], len(ids))
            emit()

        selectors = [('MATCH (n:' + label + ' {run_id:$run_id}) '
                      'RETURN elementId(n) AS id LIMIT $batch') for label in _LABELS]
        selectors += ['MATCH (n {run_id:$run_id}) WHERE NOT n:Run '
                      'RETURN elementId(n) AS id LIMIT $batch',
                      'MATCH (n:Run {id:$run_id}) RETURN elementId(n) AS id LIMIT $batch']
        for select in selectors:
            while True:
                ids = list(dict.fromkeys(row['id'] for row in session.run(
                    select, run_id=run_id, batch=node_batch).data()))
                if not ids:
                    break
                remove(ids)
    return stats
