"""Bounded independent-process acceptance. Journal mode is explicitly a fixture.

Neo4j mode requires a genuine canonical batch exported by the simulation.
This tool never invents a WHRun, robot or source watermark for the graph.
"""
import argparse
import json
from pathlib import Path
import time

from backend.agent_runtime import AgentFleet, JournalBackend, Neo4jBackend


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--robots', type=int, default=100)
    parser.add_argument('--backend', choices=['journal', 'neo4j'], default='journal')
    parser.add_argument('--source-batch', help='actual canonical batch JSON; mandatory for Neo4j')
    parser.add_argument('--timeout', type=float, default=30.)
    parser.add_argument('--kill-agent', type=int, help='exercise fail-closed process death after initial successful publication')
    args = parser.parse_args(argv)
    if not 1 <= args.robots <= 100: parser.error('robots must be within 1..100')
    if args.backend == 'neo4j' and not args.source_batch: parser.error('Neo4j needs an actual --source-batch')
    output = Path(args.output)
    if output.exists() and any(output.iterdir()): raise FileExistsError('preserve previous evidence; select a fresh output')
    if args.source_batch:
        source = json.loads(Path(args.source_batch).read_text(encoding='utf-8'))
        robots = {row['robot_id']: row for row in source['robots']}
        if len(robots) != args.robots: parser.error('source robot count differs from --robots')
        run_id, sim_time, seq = source['run_id'], source['sim_time'], source['seq']
    else:
        robots = {rid: {'robot_id': rid, 'stage': 'idle', 'soc': 80.} for rid in range(1, args.robots+1)}
        run_id, sim_time, seq = 'protocol-fixture-only', 0, 1
    backend = Neo4jBackend.from_env() if args.backend == 'neo4j' else JournalBackend()
    started = time.monotonic()
    result = {'backend': backend.name, 'input_kind': 'actual-canonical-batch' if args.source_batch else 'synthetic-protocol-fixture',
              'run_id': run_id, 'canonical_seq': seq, 'requested_processes': args.robots,
              'graph_qualified': False, 'failure_closed_verified': False}
    with AgentFleet(run_id=run_id, robot_ids=robots, backend=backend, output_dir=output,
                    timeout=args.timeout, startup_timeout=max(120., args.timeout)) as fleet:
        rows = fleet.publish(robots, sim_time, seq)
        result.update(fleet.status(), initial_all_wrote=all(row['wrote'] for row in rows),
                      initial_all_queried=all(row['queried'] for row in rows),
                      graph_qualified=args.backend == 'neo4j')
        repeated = fleet.publish(robots, sim_time, seq)
        result['unchanged_sample_no_redundant_queries'] = not any(row['queried'] for row in repeated)
        if args.kill_agent:
            if args.kill_agent not in robots: parser.error('kill-agent must name a registered robot')
            fleet.processes[args.kill_agent].terminate(); fleet.processes[args.kill_agent].join(timeout=5)
            try: fleet.publish(robots, sim_time, seq)
            except RuntimeError: result['failure_closed_verified'] = fleet.failed
            else: raise AssertionError('process failure did not close the admission gate')
        result['wall_seconds'] = time.monotonic()-started
    result['all_requested_agents_verified'] = (result['unique_pids'] == args.robots and
        result['initial_all_wrote'] and result['initial_all_queried'])
    (output/'smoke-result.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result), flush=True)
    return 0 if result['all_requested_agents_verified'] else 2


if __name__ == '__main__': raise SystemExit(main())
