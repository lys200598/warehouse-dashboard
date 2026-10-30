"""Read-only complete-step robot state input. Never falls back to memory."""
import json
import time


class KGStateError(RuntimeError):
    pass


class KGStateReader:
    def __init__(self, *, uri, user, password, database='neo4j', driver=None, timeout=2.0):
        if driver is None:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=timeout,
                                         connection_acquisition_timeout=timeout)
        self.driver, self.database, self.timeout = driver, database, timeout
        self.timings = {}
        self.latest_step = None

    def read(self, run_id, tick, expected_ids, fields):
        from neo4j import Query
        began = time.monotonic()
        deadline = began + self.timeout
        expected_ids = set(expected_ids)
        if len(expected_ids) != 100:
            raise KGStateError('neo4j dispatch requires exactly 100 robots')
        try:
            with self.driver.session(database=self.database, default_access_mode='READ') as session:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise KGStateError(f'Run.latest_step timed out waiting for tick {tick}')
                    row = session.run(Query('MATCH (r:Run {id:$run}) RETURN r.latest_step AS tick',
                                            timeout=remaining), run=run_id).single()
                    if row and row['tick'] is not None and row['tick'] >= tick:
                        self.latest_step = row['tick']
                        break
                    time.sleep(min(.005, max(0, deadline - time.monotonic())))
                read_began = time.monotonic()
                self.timings['wait_step_ms'] = (read_began - began) * 1000
                remaining = deadline - read_began
                if remaining <= 0:
                    raise KGStateError(f'graph read deadline exceeded at tick {tick}')
                rows = session.run(Query(
                    'MATCH (r:Robot {run_id:$run, last_written_tick:$tick}) '
                    'RETURN r.robot_id AS robot_id, r.payload AS payload', timeout=remaining),
                    run=run_id, tick=tick).data()
            if time.monotonic() > deadline:
                raise KGStateError(f'graph read deadline exceeded at tick {tick}')
            if len(rows) != 100 or {r['robot_id'] for r in rows} != expected_ids:
                raise KGStateError(f'incomplete robot state at tick {tick}: {len(rows)} rows')
            states = {}
            for row in rows:
                state = json.loads(row['payload'])
                if not isinstance(state, dict) or set(state) != set(fields) or state.get('robot_id') != row['robot_id']:
                    raise KGStateError(f'invalid robot payload at tick {tick}, robot {row["robot_id"]}')
                states[row['robot_id']] = state
            self.timings['read_ms'] = (time.monotonic() - read_began) * 1000
            return states
        except KGStateError:
            raise
        except Exception as exc:
            raise KGStateError(f'graph state read failed at tick {tick}: {exc}') from exc

    def close(self):
        self.driver.close()
