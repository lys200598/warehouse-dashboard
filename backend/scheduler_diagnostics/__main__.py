from pathlib import Path
from ..scheduling_v3.runner import run,build_parser
from ..wide.sim import Config
from .trace import TraceSimulation


def main():
    parser=build_parser()
    parser.add_argument('--trace-task',default='V3-T-0001673')
    parser.add_argument('--profile-from',type=int,default=3000)
    parser.add_argument('--profile-to',type=int,default=3120)
    args=parser.parse_args()
    if args.resume or args.knowledge_backend!='journal' or args.profile_to<=args.profile_from:
        parser.error('diagnostic only: fresh journal run, positive profile window')
    TraceSimulation.options={'task':args.trace_task,'directory':str(Path(args.output)/'diagnostics'),
        'profile_from':args.profile_from,'profile_to':args.profile_to,
        'snapshots':[2820,2940,3000,3300,3600,3780]}
    try:
        return run(args,simulation_class=TraceSimulation,config_class=Config)
    finally:
        if TraceSimulation.instance is not None:
            TraceSimulation.instance.close_diagnostics()


if __name__=='__main__':
    raise SystemExit(main())
