"""Read existing JSON evidence only; never connect to Neo4j or control services."""
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT = Path('/tmp/wz-kg-evidence/backlog120-20261031/analysis')
SSD = Path('/tmp/wz-kg-evidence/cleanup-tail-fix-20261031/ssd-long/day')
HDD = Path('/tmp/wz-kg-evidence/c76995e-validation-20261030/day')
ACCEPT = Path('/tmp/wz-kg-evidence/joint-c-latest-20261031/acceptance')
UTC = timezone.utc
BJT = timezone(timedelta(hours=8))
LOAD_START = datetime(2026, 10, 31, 4, 40, tzinfo=UTC)
LOAD_END = LOAD_START + timedelta(minutes=2)

def date(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00'))

def iso(d):
    return d.isoformat()

def p99(values):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * .99) - 1)] if ordered else None

def metric(rows):
    values = [float(r['write_ms']) for r in rows]
    return {'n': len(values), 'mean_ms': sum(values)/len(values) if values else None,
            'p99_ms': p99(values), 'max_ms': max(values) if values else None,
            'over_1000ms': sum(v > 1000 for v in values),
            'over_5000ms': sum(v > 5000 for v in values)}

def event(r):
    start, end = date(r['started_at_utc']), date(r['ended_at_utc'])
    write_upper = start + timedelta(seconds=1, milliseconds=float(r['write_ms']))
    return {**r, 'started_at_beijing': iso(start.astimezone(BJT)),
            'ended_at_beijing': iso(end.astimezone(BJT)),
            'initialization_tick_zero': r['tick'] == 0,
            'history_tick': r['tick'] % 4 == 0,
            'load_window_start_in_2min': LOAD_START <= start < LOAD_END,
            'load_window_possible_write_overlap_2min': start < LOAD_END and write_upper > LOAD_START}

def read_timing(directory):
    path = directory/'robot-graph-timing.jsonl'
    # Capture file length once. An appended partial last line is never included.
    with path.open('rb') as stream:
        initial = path.stat().st_size
        raw = stream.read(initial)
    cut = raw.rfind(b'\n') + 1
    complete, tail = raw[:cut], raw[cut:]
    rows, invalid = [], []
    for number, line in enumerate(complete.splitlines(), 1):
        try:
            r = json.loads(line)
            assert isinstance(r['tick'], int)
            assert math.isfinite(float(r['write_ms'])) and r['write_ms'] >= 0
            date(r['started_at_utc']); date(r['ended_at_utc'])
            rows.append(r)
        except Exception as exc:
            invalid.append({'line': number, 'error': repr(exc)})
    ticks = [r['tick'] for r in rows]
    counts = Counter(ticks)
    largest = max(ticks, default=-1)
    source = {'path': str(path), 'captured_bytes': len(raw),
              'complete_line_bytes': len(complete), 'ignored_trailing_bytes': len(tail),
              'captured_sha256': hashlib.sha256(raw).hexdigest(), 'complete_rows': len(rows),
              'invalid_complete_lines': invalid, 'max_tick': largest,
              'duplicate_ticks': [k for k,v in counts.items() if v > 1],
              'missing_ticks': [k for k in range(largest+1) if k not in counts],
              'strictly_increasing': all(a < b for a,b in zip(ticks, ticks[1:])),
              'snapshot_utc': iso(datetime.now(UTC))}
    assert not invalid and not source['duplicate_ticks'] and not source['missing_ticks']
    assert source['strictly_increasing']
    active = [r for r in rows if r['tick'] > 0]
    windows = []
    for low in range(1, largest+1, 7200):
        high = low+7199
        part = [r for r in active if low <= r['tick'] <= high]
        windows.append({'label': f'{(low-1)//3600}-{high//3600} simulated hours',
                        'tick_start_inclusive': low, 'tick_end_expected_inclusive': high,
                        'tick_end_observed_inclusive': max(r['tick'] for r in part),
                        'complete_window': len(part) == 7200, **metric(part),
                        'first_started_utc': part[0]['started_at_utc'],
                        'last_ended_utc': part[-1]['ended_at_utc']})
    result = {'source': source, 'overall_excluding_initialization': metric(active),
              'initialization': [event(r) for r in rows if r['tick'] == 0],
              'windows': windows, 'over_1000ms_all_events': [event(r) for r in rows if r['write_ms'] > 1000],
              'over_5000ms_all_events': [event(r) for r in rows if r['write_ms'] > 5000],
              'errored_rows': [event(r) for r in rows if r.get('error')]}
    progress = directory/'progress.json'
    if progress.exists():
        result['progress_snapshot'] = json.loads(progress.read_text())
    return result, rows

def acceptance():
    summary = json.loads((ACCEPT/'acceptance.json').read_text())
    output = {'path': str(ACCEPT), 'generated_at_utc_metadata': summary.get('generated_at'),
              'generated_at_note': 'metadata is not the end time; exact observed request times below',
              'run_id': summary['run_id'], 'hardware': summary['hardware'], 'protocol': summary['protocol'],
              'summary_other_fields': {k:v for k,v in summary.items() if k not in ('rounds','hardware','protocol')},
              'rounds': []}
    for round_id in (1,2,3):
        records = json.loads((ACCEPT/f'round-{round_id}.json').read_text())
        writer = json.loads((ACCEPT/f'round-{round_id}-writer.json').read_text())
        saved = next(r for r in summary['rounds'] if r['round'] == round_id)
        samples = [s for worker in records for s in worker['samples']]
        values = [s['total_ms'] for s in samples]
        start_ms, end_ms = min(s['start_wall_ms'] for s in samples), max(s['end_wall_ms'] for s in samples)
        start, end = (datetime.fromtimestamp(t/1000, UTC) for t in (start_ms,end_ms))
        compact = {k:v for k,v in saved.items() if k not in ('unique_worker_pids', 'workers', 'per_robot', 'robot_metrics', 'pack_metrics') and not isinstance(v, (list,dict))}
        # Preserve full summarized per-round metadata separately for unrecognized qualification fields.
        output['rounds'].append({'round': round_id, 'reported_scalar_fields': compact,
            'before': saved['before'], 'after': saved['after'],
            'raw_recomputed': {'requests': len(samples), 'independent_pids': len({r['pid'] for r in records}),
                'mean_ms': sum(values)/len(values), 'p99_ms_nearest_rank': p99(values),
                'max_ms': max(values), 'over_500ms': sum(v > 500 for v in values),
                'worker_error_fields': [r for r in records if r.get('error')]},
            'query_start_utc': iso(start), 'query_end_utc': iso(end),
            'query_start_beijing': iso(start.astimezone(BJT)), 'query_end_beijing': iso(end.astimezone(BJT)),
            'writer_interval_epoch_ms': writer.get('interval'),
            'reported_mean_matches_raw': abs(saved['mean_ms']-sum(values)/len(values)) < 1e-8,
            'reported_count_matches_raw': saved['count'] == len(values),
            'original_mean_under_500ms': sum(values)/len(values) < 500})
    return output

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    ssd, sr = read_timing(SSD)
    hdd, hr = read_timing(HDD)
    horizon = min(ssd['source']['max_tick'], hdd['source']['max_tick'])
    load = []
    for label, end in (('two_natural_minutes_1240_1241',LOAD_END), ('strict_first_minute_1240',LOAD_START+timedelta(minutes=1))):
        inside = [r for r in sr if LOAD_START <= date(r['started_at_utc']) < end]
        possible = [r for r in sr if date(r['started_at_utc']) < end and
                    date(r['started_at_utc'])+timedelta(seconds=1,milliseconds=r['write_ms']) > LOAD_START]
        load.append({'label': label, 'start_utc_inclusive': iso(LOAD_START), 'end_utc_exclusive': iso(end),
                     'start_beijing_inclusive': iso(LOAD_START.astimezone(BJT)),
                     'end_beijing_exclusive': iso(end.astimezone(BJT)),
                     'classification': 'started_at_utc belongs to half-open interval; timestamps truncated to seconds',
                     'metric_by_start': metric(inside), 'ticks_by_start': [r['tick'] for r in inside],
                     'over_1000ms_by_start': [event(r) for r in inside if r['write_ms'] > 1000],
                     'over_5000ms_by_start': [event(r) for r in inside if r['write_ms'] > 5000],
                     'possible_write_overlap_ticks': [r['tick'] for r in possible],
                     'possible_overlap_note': 'conservative start+[0,1s) truncation uncertainty plus write_ms; not exact write end'})
    report = {'snapshot_utc': iso(datetime.now(UTC)), 'snapshot_beijing': iso(datetime.now(BJT)),
              'method': {'p99': 'nearest rank ceil(n*.99)', 'threshold': 'strictly greater than 1000/5000ms',
                         'window': 'tick1..7200,7201..14400,etc; tick0 initialization separate',
                         'scope': 'completed worker timing records only; current inflight frame excluded',
                         'comparison_limit': 'historical HDD vs newer SSD separate runs/time/environment, observational not single-variable causality'},
              'ssd': ssd, 'hdd': hdd, 'matched_simulation_horizon': {'last_tick': horizon,
                  'ssd': metric([r for r in sr if 1 <= r['tick'] <= horizon]),
                  'hdd': metric([r for r in hr if 1 <= r['tick'] <= horizon])},
              'ssd_read_load_windows': load, 'formal_acceptance': acceptance()}
    json_path = OUT/f'tail-snapshot-{stamp}.json'
    json_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    lines = ['# SSD 后台写入长尾：只读快照', '', '### 任务背景',
        '分析正在运行的 SSD 全天实验与旧 HDD 实验逐步写入日志；核对正式查询验收及北京时间 12:40～12:41 的读负载。未连接数据库、未改变或停止任何服务。', '',
        '### 关键结论', f'- 快照北京时间：{report["snapshot_beijing"]}。SSD 已落入日志至第 {ssd["source"]["max_tick"]} 步，属于进行中快照，不能当作全天完成。',
        f'- 同步骤前缀 1～{horizon}：SSD/HDD 写入统计如下。两次运行条件不同，不能仅凭这一比较证明存储是唯一根因。',
        '- 初始化 tick 0 独立列出；每窗覆盖 7200 个模拟步骤；最后不足一窗明确标为未完成。阈值使用严格大于，P99 使用最近秩。',
        '- 所有 >1 秒与 >5 秒记录均完整保存于 JSON，包括 tick、UTC、北京时间、读负载窗口标记；不把窗口相关性写成因果关系。', '',
        '|运行（相同模拟前缀）|样本|均值 ms|P99 ms|最大 ms|>1s|>5s|', '|---|---:|---:|---:|---:|---:|---:|']
    for label in ('ssd','hdd'):
        m=report['matched_simulation_horizon'][label]
        lines.append(f'|{label}|{m["n"]}|{m["mean_ms"]:.3f}|{m["p99_ms"]:.3f}|{m["max_ms"]:.3f}|{m["over_1000ms"]}|{m["over_5000ms"]}|')
    lines += ['', '### 分段写入统计', '|运行|模拟小时（步骤从1起）|样本|完整窗|均值 ms|P99 ms|最大 ms|>1s|>5s|', '|---|---|---:|---|---:|---:|---:|---:|---:|']
    for label, data in (('SSD',ssd),('HDD',hdd)):
        for w in data['windows']:
            lines.append(f'|{label}|{w["label"]}|{w["n"]}|{w["complete_window"]}|{w["mean_ms"]:.3f}|{w["p99_ms"]:.3f}|{w["max_ms"]:.3f}|{w["over_1000ms"]}|{w["over_5000ms"]}|')
    lines += ['', '### 读负载与正式验收', '北京时间两个自然分钟 12:40、12:41 明确定义为 UTC [04:40:00,04:42:00)；另保留严格第一分钟 [04:40:00,04:41:00)。按 frame 开始时间归类，写阶段可能跨界的步骤另存 JSON。']
    for w in load:
        m=w['metric_by_start']
        lines.append(f'- {w["label"]}：{m["n"]} 步，均值 {m["mean_ms"]:.3f} ms，P99 {m["p99_ms"]:.3f} ms，最大 {m["max_ms"]:.3f} ms，>1s {m["over_1000ms"]} 次、>5s {m["over_5000ms"]} 次。')
    lines += ['', '|轮|真实查询北京时间|同运行节点数|进程数|请求数|均值 ms|P99 ms|最大 ms|>500ms|窗口观察到新提交|', '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for r in report['formal_acceptance']['rounds']:
        m=r['raw_recomputed']
        lines.append(f'|{r["round"]}|{r["query_start_beijing"]} ～ {r["query_end_beijing"]}|{r["before"]["node_count"]}|{m["independent_pids"]}|{m["requests"]}|{m["mean_ms"]:.3f}|{m["p99_ms_nearest_rank"]:.3f}|{m["max_ms"]:.3f}|{m["over_500ms"]}|{r["after"].get("observed_commit_advances")}|')
    lines += ['', '查询验收使用同一运行的百万级数据、100 独立进程，并在查询窗口观察到新的完整 Step；原始判据为每轮均值 <500ms。元数据 generated_at 不是完成时刻，以上时间从实际请求原始样本重算。', '',
        '### 执行过的操作', '- 读取 SSD/HDD 的 robot-graph-timing.jsonl，固定本次读取字节长度；丢弃非换行结尾的未完成尾行，检查完整行、顺序、重复和缺失步骤。',
        '- 读取 acceptance.json、round-1/2/3.json 及对应 writer JSON；从原始请求重算样本数、均值、P99、最大值及墙钟区间。',
        '- 仅执行本分析脚本；没有查询数据库、压测、重启或修改实验。', '', '### 修改内容',
        f'- 新增本目录 JSON/Markdown 快照；完整机器数据：`{json_path.name}`。没有修改主仓库或服务配置。', '', '### 后续建议',
        '- 全天结束后再运行同一只读分析以补齐末窗，结合诊断线程栈、GC 和设备 I/O 判断稀有长尾根因。',
        '- 不将异步主线程 write_ms=0 当成零写入成本；本报告采用后台 worker 实际 write_ms。']
    md_path=OUT/f'tail-snapshot-{stamp}.md'
    md_path.write_text('\n'.join(lines)+'\n')
    print(json.dumps({'json':str(json_path),'markdown':str(md_path),
        'ssd_max_tick':ssd['source']['max_tick'],'ssd':ssd['overall_excluding_initialization'],
        'hdd':hdd['overall_excluding_initialization'],'matched':report['matched_simulation_horizon'],
        'load_windows':[{k:v for k,v in w.items() if k in ('label','metric_by_start','over_1000ms_by_start')} for w in load],
        'acceptance_rounds':report['formal_acceptance']['rounds']},ensure_ascii=False,indent=2))

if __name__ == '__main__':
    main()
