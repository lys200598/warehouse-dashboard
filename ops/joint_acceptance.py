"""Joint acceptance: one live run, million-node graph, writes during queries, 100 concurrent clients.

The CLI delegates to kg_joint_acceptance and executes the C-label context
client used by robot actors, in 100 independent processes. Optional synthetic
negative-tick history is scoped to the selected test run and cleaned afterward.
The earlier A-label report helpers remain for inspecting archived results.

Exit codes: 0 pass; 1 metric not met or tool error; 2 not a qualified
acceptance (too few rounds, too few nodes, no concurrent writes, run changed).
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace

from . import quick_load as q
from .pressure_plan import QUERIES

VERSION = 'warehouse-joint-acceptance-v1'


def count_run_nodes(run_id):
    """Distinct nodes carrying this run_id; None when Neo4j is not configured."""
    if not os.environ.get('NEO4J_PASSWORD'):
        return None
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(os.environ.get('NEO4J_URI', 'bolt://127.0.0.1:7687'),
                                  auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD']))
    try:
        with driver.session(database=os.environ.get('NEO4J_DATABASE', 'neo4j')) as session:
            return session.run('MATCH (n) WHERE n.run_id = $run RETURN count(n) AS n', run=run_id).single()['n']
    finally:
        driver.close()


def observe(base, cfg, count_nodes=True):
    health = q.fetch_json(base, cfg['health_path'])
    run_id, seq = q.point_from_health(health, cfg)
    runtime = health.get('runtime', {})
    knowledge = runtime.get('knowledge', {})
    return {'at': datetime.now(timezone.utc).isoformat(), 'run_id': run_id, 'query_seq': seq,
            'status': runtime.get('status'), 'elapsed': runtime.get('elapsed'),
            'durable_seq': knowledge.get('durable_seq'), 'mirrored_seq': knowledge.get('mirrored_seq'),
            'lag': knowledge.get('lag'), 'nodes': count_run_nodes(run_id) if count_nodes else None}


def round_record(index, before, after, summary):
    groups = summary.get('groups', {}) if isinstance(summary, dict) else {}
    stats = {name: g['complete_http_json'] for name, g in groups.items()}
    count = sum(s['count'] for s in stats.values())
    mean = sum(s['mean_ms'] * s['count'] for s in stats.values()) / count if count else None
    outcomes = summary.get('outcomes', {}) if isinstance(summary, dict) else {}
    advance = (after['durable_seq'] or 0) - (before['durable_seq'] or 0)
    return {'round': index, 'before': before, 'after': after, 'tool_status': summary.get('status'), 'run_complete': summary.get('run_complete') is True,
            'requests': count, 'mean_ms': mean, 'max_ms': max((s['max_ms'] for s in stats.values()), default=None),
            'over_500ms': sum(s['over_500ms'] for s in stats.values()),
            'p99_ms_by_query': {name: s['p99_ms'] for name, s in stats.items()},
            'errors': sum(v for k, v in outcomes.items() if k != 'ok'),
            'client_observed_peak': summary.get('client_observed_http_peak'),
            'writer_seq_advance': advance, 'writes_during_round': advance > 0 and after['status'] == 'running'}


def verdict(rounds, *, min_rounds, min_nodes, threshold_ms):
    runs = {r['before']['run_id'] for r in rounds} | {r['after']['run_id'] for r in rounds}
    reasons = []
    if len(rounds) < min_rounds:
        reasons.append(f'only {len(rounds)} rounds, protocol requires {min_rounds}')
    if len(runs) != 1:
        reasons.append('the live run changed between rounds')
    if any(r['before']['nodes'] is None for r in rounds):
        reasons.append('graph size not verified (set NEO4J_PASSWORD/NEO4J_URI to count nodes)')
    elif any(r['before']['nodes'] < min_nodes for r in rounds):
        reasons.append(f'a round started below {min_nodes} nodes of this run')
    if any(not r['writes_during_round'] for r in rounds):
        reasons.append('the writer did not advance during every round')
    if any(r['tool_status'] != 'measured' or not r['run_complete'] or r['requests'] == 0 for r in rounds):
        reasons.append('a round did not complete its measurement')
    if any(r['errors'] for r in rounds):
        reasons.append('some requests failed or were dropped')
    means = [r['mean_ms'] for r in rounds if r['mean_ms'] is not None]
    mean_ok = bool(means) and len(means) == len(rounds) and max(means) < threshold_ms
    strict_ok = mean_ok and all(r['over_500ms'] == 0 for r in rounds)
    qualified = not reasons
    return {'qualified': qualified, 'not_qualified_reasons': reasons,
            'original_metric_mean_under_threshold_every_round': mean_ok,
            'additional_all_requests_under_threshold': strict_ok,
            'pass': qualified and mean_ok, 'threshold_ms': threshold_ms}


def render(result):
    v, p = result['verdict'], result['protocol']
    head = '通过' if v['pass'] else ('未满足验收资格' if not v['qualified'] else '未达标')
    lines = [f'# 联合验收结果：{head}', '',
             f"协议：{p['rounds']}轮 × {p['clients']}客户端（closed，{p['query_id']}查询），每轮最多{p['duration']}秒、每客户端最多{p['requests']}次；"
             f"节点下限{p['min_nodes']}；判据为每轮平均<{p['threshold_ms']}ms。", '',
             '| 轮 | 节点数 | 写入水位前进 | 请求数 | 平均ms | 最慢ms | >500ms | 失败 | 并发峰值 |', '|---|---|---|---|---|---|---|---|---|']
    for r in result['rounds']:
        mean = '—' if r['mean_ms'] is None else f"{r['mean_ms']:.1f}"
        top = '—' if r['max_ms'] is None else f"{r['max_ms']:.1f}"
        lines.append(f"| {r['round']} | {r['before']['nodes']} | {r['writer_seq_advance']} | {r['requests']} | {mean} | "
                     f"{top} | {r['over_500ms']} | {r['errors']} | {r['client_observed_peak']} |")
    lines += ['', f"原始指标（每轮平均<{v['threshold_ms']}ms）：{'满足' if v['original_metric_mean_under_threshold_every_round'] else '不满足'}",
              f"附加：全部请求<{v['threshold_ms']}ms：{'是' if v['additional_all_requests_under_threshold'] else '否'}"]
    if v['not_qualified_reasons']:
        lines += ['', '不满足验收资格的原因：'] + [f'- {x}' for x in v['not_qualified_reasons']]
    lines += ['', '各轮原始记录在 round-N/ 目录；节点数在每轮测量开始前统计，不计入延迟。']
    return '\n'.join(lines) + '\n'


def _legacy_a_main(argv=None):
    q.configure_console()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', required=True, help='live service, e.g. http://127.0.0.1:14192')
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--clients', type=int, default=100)
    p.add_argument('--duration', type=float, default=60, help='seconds per round')
    p.add_argument('--requests', type=int, default=200, help='per-client cap; a round ends at the cap or the duration')
    p.add_argument('--query', choices=QUERIES, default='mixed')
    p.add_argument('--min-nodes', type=int, default=1_000_000)
    p.add_argument('--threshold-ms', type=float, default=500)
    p.add_argument('--config', type=Path, default=q.ROOT / 'config/pressure-test.json')
    p.add_argument('--output', type=Path)
    args = p.parse_args(argv)
    cfg, base = q.load_config(args.config), args.url.rstrip('/')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = (args.output or q.ROOT / 'data/local-load' / f'joint-acceptance-{stamp}').resolve()
    out.mkdir(parents=True, exist_ok=False)
    protocol = {'version': VERSION, 'rounds': args.rounds, 'clients': args.clients, 'processes': args.clients,
                'mode': 'closed', 'query_id': args.query, 'duration': args.duration, 'requests': args.requests,
                'min_nodes': args.min_nodes, 'threshold_ms': args.threshold_ms}
    plan = {k: protocol[k] for k in ('clients', 'processes', 'mode', 'query_id', 'duration', 'requests')}
    (out / 'plan.json').write_text(json.dumps(plan, indent=2), encoding='utf-8')
    rounds = []
    for index in range(1, args.rounds + 1):
        print(f'== 第{index}/{args.rounds}轮：记录运行状态与节点数……', flush=True)
        before = observe(base, cfg)
        folder = out / f'round-{index}'
        q.run(SimpleNamespace(output=folder, plan_json=out / 'plan.json', server_trace=[], phase='read-write'),
              cfg, base, 'burst')
        after = observe(base, cfg, count_nodes=False)
        summary = json.loads((folder / 'summary.json').read_text(encoding='utf-8'))
        record = round_record(index, before, after, summary)
        analysis = folder / 'analysis.json'
        if analysis.exists():
            record['client_observed_peak'] = json.loads(analysis.read_text(encoding='utf-8')).get('client_observed_http_peak')
        rounds.append(record)
    result = {'protocol': protocol, 'rounds': rounds,
              'verdict': verdict(rounds, min_rounds=3, min_nodes=args.min_nodes, threshold_ms=args.threshold_ms)}
    (out / 'acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    (out / 'report.md').write_text(render(result), encoding='utf-8')
    print(render(result) + f'\n结果目录：{out}', flush=True)
    v = result['verdict']
    return 0 if v['pass'] else (2 if not v['qualified'] else 1)


def main(argv=None):
    """Current acceptance uses C-label robot context; old helpers serve stored reports."""
    from .kg_joint_acceptance import main as c_main
    return c_main(argv)


if __name__ == '__main__':
    raise SystemExit(main())
