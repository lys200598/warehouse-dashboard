"""Beginner read-only catalog load wizard; delegates measurement to load_harness.

Never controls a simulation, changes a database, or upgrades audit qualification.
"""
import argparse
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from copy import deepcopy
import hashlib
import http.client
import json
import os
import signal
from pathlib import Path
import sys
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, build_opener

from . import load_harness as harness
from .load_validation import validate_catalog
from .pressure_plan import PROFILES, normalize, requires_confirmation, cases

ROOT = Path(__file__).resolve().parents[1]


class SetupError(ValueError):
    pass


def configure_console():
    # Windows redirected stdout can otherwise use the legacy code page while
    # editors and terminal capture expect UTF-8, corrupting the Chinese menu.
    if sys.platform == 'win32':
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, 'reconfigure'):
                stream.reconfigure(encoding='utf-8', errors='replace')


def save(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')


def pick(value, keys):
    if not isinstance(keys, list) or not keys or any(not isinstance(k, str) for k in keys):
        raise SetupError('配置中的字段路径必须是非空字符串列表')
    try:
        for key in keys:
            value = value[key]
    except (KeyError, TypeError):
        raise SetupError('接口缺少字段：'+'.'.join(keys)) from None
    return value


def load_config(path):
    with Path(path).open(encoding='utf-8-sig') as stream:
        cfg = json.load(stream)
    if cfg.get('version') != 'warehouse-pressure-test-v1':
        raise SetupError('配置版本不匹配，请使用config/pressure-test.json')
    for key in ('health_path', 'catalog_path', 'query_path'):
        value = cfg.get(key)
        if not isinstance(value, str) or not value.startswith('/') or value.startswith('//') or '#' in value:
            raise SetupError('配置中的接口路径无效：'+key)
    for key in ('run_id_path', 'seq_path', 'graph_enabled_path', 'catalog_queries_path'):
        if not isinstance(cfg.get(key), list) or not cfg[key] or any(not isinstance(k, str) for k in cfg[key]):
            raise SetupError('字段路径配置无效：'+key)
    harness.origin(cfg.get('base_url', ''))
    return cfg


def point_from_health(health, cfg):
    if health.get('status') in ('degraded', 'failed'):
        raise SetupError('服务报告运行异常，请先检查服务日志；本工具不会替你重启服务')
    if pick(health, cfg['graph_enabled_path']) is not True:
        raise SetupError('这个服务尚未启用图谱查询。请使用Neo4j模式服务，勿用journal回放端口')
    run, seq = pick(health, cfg['run_id_path']), pick(health, cfg['seq_path'])
    if not isinstance(run, str) or not run or len(run) > 200:
        raise SetupError('健康接口没有有效的运行编号，请检查当前数据集')
    if type(seq) is not int or seq < 1:
        raise SetupError('Neo4j还没有已提交的查询批次，请先检查运行/写入状态')
    return run, seq


def workload(cfg, profile, run, seq, plan=None):
    plan = plan or normalize(profile)
    rows = []
    for query, weight in cases(plan):
        body = {'query_id': query, 'run_id': run, 'seq': seq, 'robot_id': '{{client_id}}'}
        if query not in ('self','readiness'):
            body.update(window_steps=plan['window_steps'], event_limit=plan['event_limit'],
                        history_limit=plan['history_limit'])
        rows.append({'name': profile+'-'+query, 'query_class': query, 'weight': weight,
                     'kind': 'read', 'path': cfg['query_path'], 'body': body})
    return {'version': 'warehouse-load-v1', 'cases': rows}


def fetch_json(base, path):
    # The harness itself connects directly; use the same no-proxy behavior here.
    try:
        with build_opener(ProxyHandler({})).open(base+path, timeout=15) as response:
            result = json.load(response)
    except HTTPError as exc:
        raise SetupError(f'接口{path}返回HTTP {exc.code}，请核对端口、路径或认证') from None
    except (URLError, OSError) as exc:
        raise SetupError(f'无法连接{base}{path}：{exc}；请检查服务或SSH端口映射') from None
    if not isinstance(result, dict):
        raise SetupError('接口必须返回JSON对象：'+path)
    return result


def preflight(base, case, run, seq):
    scheme, host, port = harness.origin(base)
    cls = http.client.HTTPSConnection if scheme == 'https' else http.client.HTTPConnection
    connection = cls(host, port, timeout=15)
    try:
        return harness.request_once(connection, case,
            {'client_id': 1, 'request_index': 0, 'run_id': run},
            'preflight-'+uuid.uuid4().hex)
    finally:
        connection.close()


def invoke(arguments, log):
    with Path(log).open('x', encoding='utf-8') as stream:
        # Run the unmodified entry point in-process so its Ctrl+C/finally owns
        # and drains all spawned workers, rather than killing an intermediate
        # parent and accidentally orphaning its clients.
        previous = Path.cwd()
        try:
            os.chdir(ROOT)
            with redirect_stdout(stream), redirect_stderr(stream):
                try:
                    return harness.main(arguments)
                except SystemExit as exc:
                    return int(exc.code or 0)
        finally:
            os.chdir(previous)


def build_summary(analysis, run, seq, profile, plan=None):
    # Do not recompute a percentile, omit failures, or promote qualification.
    return {
        'version': 'warehouse-pressure-summary-v1', 'status': 'measured',
        'scope': 'HTTP catalog sample; not full graph/business acceptance',
        'run_id': run, 'seq': seq, 'profile': profile,
        'configured_clients': (plan or PROFILES[profile])['clients'], 'plan': plan or normalize(profile),
        'planned_arrivals': analysis['planned_arrivals'], 'outcomes': analysis['outcomes'],
        'run_complete': analysis['run_complete'], 'groups': deepcopy(analysis['groups']),
        'structural_validation': analysis['offline_validation'],
        'all_requests_under_500ms': analysis['all_requests_under_500ms'],
        'client_observed_http_peak': analysis['client_observed_http_peak'],
        'actual_server_api_peak': analysis['actual_server_api_peak'],
        'server_peak_qualified': analysis['server_peak_qualified'],
        'server_probe_status': analysis['server_probe_status'],
        'parse_errors': analysis['parse_errors'], 'duplicate_ids': analysis['duplicate_ids'],
        'clients_without_requests': analysis['clients_without_requests'],
        'independent_source_verified': False, 'million_prefix_qualified': False,
        'business_hundred_verified': False, 'continuous_writes_verified': False,
        'cold_or_fully_warm_qualified': False,
        'preflight_semantics': 'one check per query kind before load; not cold/all-worker warm proof',
    }


def number(value):
    return '未提供' if value is None else f'{value:.2f}'


def render(summary, output):
    lines = ['# 仓库查询检查结果', '', f"结果目录：`{output}`", '']
    if summary['status'] != 'measured':
        lines += ['本次未完成测量。', '', summary['error']]
    else:
        verdict = '本次样本的延迟与结构检查通过' if summary['all_requests_under_500ms'] else '本次样本未满足全请求500ms条件'
        lines += [verdict, '', f"方案：{summary.get('plan',PROFILES[summary['profile']])['label']}",
            f"运行：`{summary['run_id']}`；固定知识提交点：{summary['seq']}",
            f"配置客户端：{summary['configured_clients']}；计划到达：{summary['planned_arrivals']}",
            f"成功/失败/丢到达原统计：`{json.dumps(summary['outcomes'], ensure_ascii=False)}`", '',
            '|种类|样本数|平均ms|P99 ms|最大ms|超过500ms|错误/丢到达|',
            '|---|---:|---:|---:|---:|---:|---:|']
        for name, group in summary['groups'].items():
            m = group['complete_http_json']
            lines.append(f"|{name}|{m['count']}|{number(m['mean_ms'])}|{number(m['p99_ms'])}|{number(m['max_ms'])}|{m['over_500ms']}|{group['errors_or_drops']}|")
        server = str(summary['actual_server_api_peak']) if summary['server_peak_qualified'] else '未取得完整有效证据'
        lines += ['', f"服务端实际在途峰：{server}。客户端数量不代替服务端并发。",
            f"结构校验错误：{len(summary['structural_validation']['errors'])}；客户端解析/文件错误：{len(summary['parse_errors'])}。",
            '', '## 本次未验收的项目',
            '- 没有进行独立journal/oracle来源验真，结构检查只验证接口契约和自洽SHA。',
            '- 没有验证同run百万规模、100机器人实际工作、持续写入或历史控制效果。',
            '- 每种查询先发1次连通性检查，未证明全冷或所有worker均已预热。',
            '- 本工具不查询Agent私有接口、不测ANN、不改仿真和数据库。',
            '', '## 原件',
            '- analysis.json：原分析器完整结果；client/：每个客户端的完整请求和响应。',
            '- preflight.json：额外连通性请求；workload.json：实际查询输入。',
            '- run.log / analyze.log：工具原始输出。',
            '- 有失败或慢请求时保留现场，按LOAD_TEST_COOKBOOK排查；不自动重试或删样本。']
    return '\n'.join(lines)+'\n'


def print_result(summary, output):
    if summary['status'] != 'measured':
        print('\n未完成：'+summary['error'], flush=True)
    else:
        print('\n'+('本次样本检查通过。' if summary['all_requests_under_500ms'] else '本次样本未达标，请查看报告。'))
        print('请求结果：'+json.dumps(summary['outcomes'], ensure_ascii=False))
        for name, group in summary['groups'].items():
            m = group['complete_http_json']
            print(f"{name}：平均{number(m['mean_ms'])}ms；P99 {number(m['p99_ms'])}ms；"
                  f"最慢{number(m['max_ms'])}ms；超过500ms共{m['over_500ms']}条。")
        print('本次检查HTTP与返回结构；百万规模、真实业务与独立来源验真未在这里验收。')
    print('中文报告：'+str(output/'report.md'), flush=True)


def run(args, cfg, base, profile):
    identity = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    output = (args.output or ROOT/'data/local-load'/('easy-'+identity)).resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        plan = normalize(profile, json.loads(args.plan_json.read_text(encoding='utf-8'))
                         if getattr(args, 'plan_json', None) else None)
        save(output/'identity.json', {'tool': 'ops.quick_load', 'python': sys.version,
            'tool_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'harness_sha256': hashlib.sha256(Path(harness.__file__).read_bytes()).hexdigest(),
            'config': cfg, 'base_url': base, 'profile': profile, 'plan': plan,
            'server_trace': [str(p.resolve()) for p in args.server_trace]})
        print('1/3 检查服务与查询输入……', flush=True)
        health = fetch_json(base, cfg['health_path'])
        run_id, seq = point_from_health(health, cfg)
        catalog = fetch_json(base, cfg['catalog_path'])
        queries = pick(catalog, cfg['catalog_queries_path'])
        if not isinstance(queries, dict) or any(query not in queries for query,_ in cases(plan)):
            raise SetupError('服务目录缺少本方案所需的查询类型')
        spec = workload(cfg, profile, run_id, seq, plan)
        save(output/'workload.json', spec)
        # Preserve the original harness path safety policy, including new POST paths.
        try:
            harness.read_workload(output/'workload.json')
        except ValueError as exc:
            raise SetupError('负载与原发起器不兼容：'+str(exc)+
                '。若改了POST路径，请按手册更新只读白名单或保留旧路由') from None
        for index, case in enumerate(spec['cases']):
            row = preflight(base, case, run_id, seq)
            save(output/('preflight.json' if index == 0 else f'preflight-{index}.json'), row)
            if row['outcome'] != 'ok':
                raise SetupError(f"单请求检查失败：{row['outcome']}，HTTP {row['status']}。详见preflight记录")
            try:
                validate_catalog(row)
            except (AssertionError, KeyError, TypeError, ValueError):
                raise SetupError('返回结构与标准catalog接口不兼容。查看preflight.json；'
                                 '若接口已修改，请同步适配校验器，勿跳过检查') from None
        print(f'  已找到运行{run_id}，固定读取第{seq}批知识。', flush=True)
        print('2/3 执行'+plan['label']+'，原始记录写入独立目录……', flush=True)
        p = plan
        code = invoke(['run', '--base-url', base, '--workload', str(output/'workload.json'),
            '--output', str(output/'client'), '--session', 'easy-'+identity, '--run-id', run_id,
            '--clients', str(p['clients']), '--processes', str(p['processes']), '--mode', p['mode'],
            '--duration', str(p['duration']), '--requests', str(p['requests']), '--interval', str(p['interval']),
            '--rate', str(p['rate']), '--timeout', str(p['timeout']),
            '--phase', getattr(args, 'phase', 'static')], output/'run.log')
        if code != 0:
            state = output/'client/run.json'
            if state.exists() and json.loads(state.read_text()).get('interrupted'):
                raise KeyboardInterrupt
            raise SetupError('请求发起工具未完整结束，退出码'+str(code)+'；查看run.log')
        print('3/3 分析完整HTTP＋JSON耗时，检查返回结构……', flush=True)
        analysis_args = ['analyze', '--input', str(output/'client'), '--output', str(output/'analysis.json'),
            '--require-run', run_id, '--validator', 'ops.load_validation:validate_catalog']
        if args.server_trace:
            analysis_args += ['--server-trace', *[str(p.resolve()) for p in args.server_trace]]
        code = invoke(analysis_args, output/'analyze.log')
        if code not in (0, 2):
            raise SetupError('分析工具异常退出，查看analyze.log；原请求全部保留')
        summary = build_summary(json.loads((output/'analysis.json').read_text(encoding='utf-8')),
                                run_id, seq, profile, plan)
        result = 0 if summary['all_requests_under_500ms'] else 2
    except KeyboardInterrupt:
        summary = {'status': 'interrupted', 'error': '用户中断；保留已有日志和请求，不称为完整通过。'}
        result = 130
    except Exception as exc:
        summary = {'status': 'setup_or_tool_error', 'error': type(exc).__name__+': '+str(exc)}
        result = 1
    save(output/'summary.json', summary)
    report = render(summary, output)
    (output/'report.md').write_text(report, encoding='utf-8')
    print_result(summary, output)
    return result


def main(argv=None):
    configure_console()
    if sys.flags.optimize:
        print('未开始：请关闭python -O或PYTHONOPTIMIZE，保留原校验器的完整断言检查。', file=sys.stderr)
        return 1
    if sys.platform=='win32' and hasattr(signal,'SIGBREAK'):
        def stop_from_gui(signum,frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGBREAK,stop_from_gui)
    parser = argparse.ArgumentParser(description='仓库查询向导：自动取run/seq、发请求、生成中文报告；不改服务')
    parser.add_argument('--url', help='已有实时服务的地址，例如http://127.0.0.1:14192')
    parser.add_argument('--profile', choices=tuple(PROFILES), help='不选则显示菜单；非交互默认quick')
    parser.add_argument('--config', type=Path, default=ROOT/'config/pressure-test.json')
    parser.add_argument('--output', type=Path, help='可选：新的结果目录，默认自动生成')
    parser.add_argument('--server-trace', type=Path, action='append', default=[], help='可选：同主机同入口已封口的观测日志')
    parser.add_argument('--yes', action='store_true', help='明确同意100客户端/重负载方案；不降低判据')
    parser.add_argument('--plan-json', type=Path, help='受限自定义参数文件；测量仍由原工具执行')
    parser.add_argument('--gui', action='store_true', help='启动独立网页压测控制台')
    parser.add_argument('--port', type=int, default=14595, help='GUI监听端口，仅绑定127.0.0.1')
    parser.add_argument('--data-root', type=Path, help='GUI实验记录目录')
    args = parser.parse_args(argv)
    try:
        if args.gui:
            from .pressure_gui import serve
            return serve(args.port, args.data_root or ROOT/'data/pressure-gui', args.config, args.url)
        cfg = load_config(args.config)
        base, profile = args.url or cfg['base_url'], args.profile
        interactive = sys.stdin.isatty()
        if interactive and profile is None:
            print('仓库查询检查向导（只读，不会启动/暂停/切换仿真）')
            base = input(f'服务地址 [回车使用{base}]：').strip() or base
            print('1. 连通性小检查：1客户端×3次（推荐先选）\n'
                  '2. 日常错峰：100客户端，每人5秒最多一次，测10秒\n'
                  '3. 轻查询突发：100客户端，每人最多2次\n'
                  '4. 全局重查询突发：100客户端，每人最多2次')
            choice = input('选择 [回车选1]：').strip() or '1'
            if choice not in ('1', '2', '3', '4'):
                raise SetupError('请选择1—4，未发出压测请求')
            profile = tuple(PROFILES)[int(choice)-1]
        profile = profile or 'quick'
        harness.origin(base)
        base = base.rstrip('/')
        plan = normalize(profile, json.loads(args.plan_json.read_text(encoding='utf-8')) if args.plan_json else None)
        if requires_confirmation(plan) and not args.yes:
            if not interactive or input('此方案会向现有服务施压。确认使用自己的测试服务？输入yes：').strip().lower() != 'yes':
                raise SetupError('尚未确认，未发出请求。命令行明确同意时可加--yes')
        return run(args, cfg, base, profile)
    except (ValueError, OSError, KeyError, EOFError) as exc:
        print('未开始：'+str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
