"""One read-only graph connection per robot process; metrics never drive dispatch."""
import os
import time

from .catalog import read_context


class RobotContextClient:
    def __init__(self, config=None, *, driver=None, **options):
        config = dict(config or {})
        config.update(options)
        self.database = config.get('database', 'neo4j')
        self.options = {key: config[key] for key in ('state_window', 'event_window', 'radius', 'helper_min_battery')
                        if key in config}
        if driver is None:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(config['uri'], auth=(config['user'], config.get('password')),
                                          max_connection_pool_size=2, connection_timeout=2.,
                                          connection_acquisition_timeout=2.)
        self.driver = driver

    def query(self, run_id, robot_id, tick=None):
        began = time.perf_counter()
        context = read_context(self.driver, run_id, robot_id, tick=tick,
                               database=self.database, require_complete=True, **self.options)
        return {'robot_id': robot_id, 'pid': os.getpid(), 'tick': context['tick'],
                'total_ms': (time.perf_counter() - began) * 1000,
                'pack_ms': context['pack_ms'], 'pack_rows': context['pack_rows'],
                'query_ms': context['query_ms'], 'missing': context['missing']}

    def close(self):
        self.driver.close()
