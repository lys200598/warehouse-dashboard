"""One explicit CLI for fixed inputs, simulation, shared queries and playback."""
import argparse
import json
from pathlib import Path
import sys
from . import VERSION

ROOT=Path(__file__).resolve().parents[2]
SCENE=ROOT/'data/layouts/warehouse100-wide-v2.json'


def emit(value,output=None):
    raw=json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n'
    if output:
        with Path(output).open('x',encoding='utf-8') as stream:stream.write(raw)
    else:print(raw,end='')


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('status',help='show entrypoints and explicit execution/measurement scope')
    q=sub.add_parser('validate',help='validate warehouse fixed-file input without a calendar cap')
    q.add_argument('--scene',default=str(SCENE));q.add_argument('--dataset',required=True)
    q.add_argument('--max-tasks',type=int);q.add_argument('--output')
    q=sub.add_parser('convert-boundaries',help='create a new logical INBOUND/OUTBOUND dataset; preserve goods, time and rack references')
    q.add_argument('--scene',default=str(SCENE));q.add_argument('--dataset',required=True)
    q.add_argument('--output',required=True);q.add_argument('--role',choices=['both','inbound','outbound'],default='both')
    q=sub.add_parser('generate',help='reproducible input; robot energy/faults stay outside CSV')
    q.add_argument('--kind',choices=['multiday','uniform','peak','hotspot','continuous'],default='multiday')
    q.add_argument('--scene',default=str(SCENE));q.add_argument('--output',required=True)
    q.add_argument('--days',type=int,default=7);q.add_argument('--profiles')
    q.add_argument('--tasks-per-day',type=int,default=6912);q.add_argument('--seed',type=int,default=20261028)
    q.add_argument('--date',default='2026-10-28');q.add_argument('--active-slots',type=int)
    q.add_argument('--max-tasks',type=int,default=250000)
    q.add_argument('--duration-seconds',type=int,default=3600);q.add_argument('--slot-interval-seconds',type=int,default=21600)
    from ..knowledge_runtime.runner import build_parser
    q=sub.add_parser('run',parents=[build_parser()],add_help=False,help='selected V5 with required shared history or explicit rules baseline')
    q.set_defaults(scene=str(SCENE),knowledge_backend='neo4j',commit_seconds=5)
    # Existing parser marks scene required; one platform default is sufficient.
    for action in q._actions:
        if action.dest=='scene':action.required=False
    q.add_argument('--mode',dest='knowledge_mode',choices=['required','rules'],default=argparse.SUPPRESS)
    q.add_argument('--resource-graph',action='store_true',help='project racks/resources in the same Neo4j commit; new run only')
    q=sub.add_parser('query',help='versioned read-only warehouse query catalog')
    q.add_argument('--catalog',action='store_true');q.add_argument('--request-json')
    q.add_argument('--output');q.add_argument('--timeout',type=float,default=15)
    q.add_argument('--summary-root',type=Path,help='source journal root for exact-cutoff derived summaries; baseline if omitted')
    q.add_argument('--query-summary-cache',type=Path)
    q=sub.add_parser('serve',help='read-only saved frames, preserves existing camera controls')
    q.add_argument('--output',required=True);q.add_argument('--port',type=int,default=14183)
    q.add_argument('--dist',default=str(ROOT/'dist/v5'));q.add_argument('--neo4j',action='store_true')
    sub.add_parser('live',help='owned live controls, dataset selection and graph queries',add_help=False)
    sub.add_parser('retrieval-eval',help='forward arguments to the independent retrieval evaluator',add_help=False)
    return p


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0]=='retrieval-eval':
        from ..retrieval_eval.__main__ import main as evaluate
        return evaluate(argv[1:])
    if argv and argv[0]=='live':
        # A fresh process lets an operator select an immutable producer tree
        # without mixing already imported backends from different revisions.
        import subprocess
        return subprocess.call([sys.executable,str(ROOT/'ops/warehouse_live.py'),*argv[1:]])
    p=parser();args=p.parse_args(argv)
    if args.command=='status':
        from ..scheduling_v5.sim import execution_identity
        from ..real_scale.writer import SELECTED_POLICY,SELECTED_PARAMETERS,SELECTED_PLANNER
        emit({'version':VERSION,'scene':str(SCENE),'source_identity':execution_identity(SELECTED_POLICY,SELECTED_PARAMETERS,SELECTED_PLANNER),
            'default_mode':'required','default_knowledge_backend':'neo4j','default_commit_seconds':5,
            'execution':'one central V5 coordinator with 100 actual simulated robots',
            'legacy_ray_entrypoint':'python -m backend.server --help',
            'playback':'serve consumes recorded --frames output; no synthetic frames',
            'duration_cap':None,'subcommands':['validate','convert-boundaries','generate','run','query','serve','live','status','retrieval-eval']})
        return 0
    if args.command=='convert-boundaries':
        from ops.convert_logical_boundaries import convert
        manifest,report=convert(args.scene,args.dataset,args.output,
            inbound=args.role!='outbound',outbound=args.role!='inbound')
        emit({'manifest':manifest,'validation':report});return 0
    if args.command=='validate':
        from ..wide.data import validate_dataset
        emit(validate_dataset(args.scene,args.dataset,max_tasks=args.max_tasks),args.output);return 0
    if args.command=='generate':
        if args.kind=='multiday':
            from ..wide.multiday import generate_multiday
            result=generate_multiday(args.scene,args.output,days=args.days,
                profiles=args.profiles.split(',') if args.profiles else None,
                tasks_per_day=args.tasks_per_day,seed=args.seed,date=args.date,
                active_slots=args.active_slots,max_tasks=args.max_tasks)
        else:
            from ..wide.scenarios import generate_scenario
            result=generate_scenario(args.scene,args.output,scenario=args.kind,
                duration_seconds=args.duration_seconds,slot_interval_seconds=args.slot_interval_seconds,
                seed=args.seed,active_slots=args.active_slots,max_tasks=args.max_tasks)
        emit({'manifest':result[0],'validation':result[1]});return 0
    if args.command=='run':
        from functools import partial
        from ..knowledge_runtime.runner import run
        from ..knowledge_runtime.sim import KnowledgeSimulation
        from ..real_scale.writer import Config,RealMirror,audit_real
        factory=partial(KnowledgeSimulation,commit_seconds=args.commit_seconds,knowledge_mode=args.knowledge_mode,resource_graph=args.resource_graph)
        if args.resource_graph:
            if args.knowledge_backend!='neo4j':p.error('--resource-graph needs Neo4j')
            from ..resource_graph_mirror import ResourceMirror
            RealMirror=ResourceMirror
        return run(args,simulation_class=factory,config_class=Config,mirror_class=RealMirror,graph_auditor=audit_real)
    if args.command=='query':
        from ..query_catalog import QueryCatalogService
        if args.catalog:
            if args.request_json:p.error('choose --catalog or --request-json')
            emit(QueryCatalogService.describe(),args.output);return 0
        if not args.request_json:p.error('--request-json or --catalog required')
        request=json.loads(sys.stdin.read() if args.request_json=='-' else Path(args.request_json).read_text(encoding='utf-8'))
        options=({'summary_root':args.summary_root,'summary_cache_root':args.query_summary_cache}
                 if args.summary_root is not None else {})
        if args.query_summary_cache is not None and args.summary_root is None:
            p.error('--query-summary-cache requires --summary-root')
        service=QueryCatalogService.from_env(timeout=args.timeout,**options)
        try:emit(service.execute(request),args.output)
        finally:service.close()
        return 0
    if args.command=='serve':
        if not all((Path(args.output)/name).is_file() for name in ('latest.json','frames.sqlite','checkpoint.json')):
            p.error('serve needs recorded frames/latest/checkpoint; run with --frames in a new output')
        from ..wide.web import serve
        serve(args);return 0
    raise AssertionError(args.command)


if __name__=='__main__':raise SystemExit(main())
