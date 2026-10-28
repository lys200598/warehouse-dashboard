"""Run-bounded graph audit using existing ownership/step relationships.

No global label scan, schema mutation, timeout increase, or weaker count check.
The relationship walk is anchored at the unique WHRun index.
"""
from time import perf_counter


def audit_graph(store, sim):
    if not store.mirror:
        return None
    mirror, run = store.mirror, sim.run_id
    counts, timings = {}, {}
    direct = {'WHRobot', 'WHTask', 'WHSlot', 'WHEvent', 'WHLocation'}
    labels = ['WHRobot', 'WHAgentState', 'WHTask', 'WHTaskState', 'WHSlot',
              'WHEvent', 'WHGoodsState', 'WHChargerState', 'WHLocation']
    for label in labels:
        walk = ('(r)<-[:IN_RUN]-(n:' + label + ')' if label in direct else
                '(r)<-[:IN_RUN]-(s:WHStep)<-[:IN_STEP]-(n:' + label + ')')
        query = ('MATCH (r:WHRun {run_id:$run}) USING INDEX r:WHRun(run_id) '
                 'CALL (r) { MATCH ' + walk + ' WHERE n.run_id=$run RETURN count(n) AS n } RETURN n')
        started = perf_counter()
        counts[label] = mirror.query(query, {'run': run})[0]['n']
        timings[label] = perf_counter() - started
    counts['cargo_locations'] = mirror.query(
        'MATCH (g:WHCargo {run_id:$run}) RETURN g.current_location_id AS location,count(*) AS n', {'run': run})
    counts['inside'] = mirror.query(
        "MATCH (g:WHCargo {run_id:$run}) WHERE g.current_location_id IS NOT NULL AND g.current_location_id<>'SHIPPED' RETURN count(g) AS n",
        {'run': run})[0]['n']
    if counts['inside'] != sim.inside or counts['WHRobot'] != 100 or counts['WHTask'] != sim.cursor:
        raise AssertionError('graph/source count mismatch')
    counts['audit_seconds_by_label'] = timings
    return counts
