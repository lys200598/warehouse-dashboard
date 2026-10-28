import argparse
from .evaluate import run


def main(argv=None):
    p=argparse.ArgumentParser(description='Source-derived exact/ANN retrieval evaluation; read-only graph')
    p.add_argument('--oracle-index',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--max-states',type=int,default=1000000)
    p.add_argument('--queries',type=int,default=24)
    p.add_argument('--k',type=int,default=5)
    p.add_argument('--embargo-steps',type=int,default=20)
    p.add_argument('--window-steps',type=int,default=60)
    p.add_argument('--neo4j',action='store_true')
    args=p.parse_args(argv)
    run(args.oracle_index,args.output,max_states=args.max_states,queries=args.queries,k=args.k,
        embargo_steps=args.embargo_steps,window_steps=args.window_steps,neo4j=args.neo4j)
    return 0


if __name__=='__main__':raise SystemExit(main())
