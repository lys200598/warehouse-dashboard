"""Reproducible source-derived retrieval evaluation; never writes the graph."""
import hashlib
import json
import math
from pathlib import Path
import time

from ..knowledge import digest
from ..real_scale.oracle import RealOracle
from ..real_scale import real_vector,VECTOR_INDEX
from . import VERSION,business_vector,business_relevant,cosine,family


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda:stream.read(1024*1024),b''):h.update(part)
    return h.hexdigest()


def distribution(values):
    values=sorted(values)
    return {'count':len(values),'mean':sum(values)/max(1,len(values)),
        'p99':values[max(0,math.ceil(len(values)*.99)-1)] if values else None,
        'max':max(values,default=None)}


def rank_numpy(matrix,vector,indices,k,seqs,robots):
    import numpy as np
    # Exact float64 dot products of already L2-normalized rows. Stable tie key
    # is (-score,seq,robot), shared with the independent scalar reference.
    scores=matrix[indices] @ np.asarray(vector,dtype=np.float64)
    if len(indices)>k:
        threshold=np.partition(scores,-k)[-k]
        selected=np.flatnonzero(scores>=threshold-1e-12)
    else:selected=np.arange(len(indices))
    ordered=sorted(selected,key=lambda j:(-float(scores[j]),int(seqs[indices[j]]),int(robots[indices[j]])))[:k]
    return [(int(indices[j]),float(scores[j])) for j in ordered]


def rank_scalar(matrix,vector,indices,k,seqs,robots):
    import heapq
    scored=((int(i),cosine(vector,matrix[i])) for i in indices)
    return heapq.nsmallest(k,scored,key=lambda p:(-p[1],int(seqs[p[0]]),int(robots[p[0]])))


def select_queries(seqs,robots,physics,queries,embargo_steps):
    """Diversify low-SOC episodes, then span the ENTIRE source with other points."""
    import numpy as np
    available=np.flatnonzero(seqs>embargo_steps+1)
    low=available[(physics[available,2]<=35)&(physics[available,3]==1)]
    episodes=[];seen=set()
    for i in low:
        marker=(int(robots[i]),int(seqs[i])//200)
        if marker not in seen:episodes.append(int(i));seen.add(marker)
    count=min(queries//2,len(episodes))
    chosen=[episodes[j] for j in np.linspace(0,len(episodes)-1,count,dtype=int)] if count else []
    remaining=available[~np.isin(available,chosen)]
    needed=queries-len(chosen)
    if len(remaining)<needed:raise ValueError('not enough distinct source-derived query cases')
    chosen += [int(remaining[j]) for j in np.linspace(0,len(remaining)-1,needed,dtype=int)]
    return sorted(chosen,key=lambda i:(int(seqs[i]),int(robots[i])))


def run(oracle_index,output,*,max_states=1000000,queries=24,k=5,embargo_steps=20,
        window_steps=60,neo4j=False):
    import numpy as np
    if (type(max_states) is not int or not 100<=max_states<=2000000 or max_states%100
        or type(queries) is not int or not 1<=queries<=24 or type(k) is not int or not 1<=k<=20
        or type(embargo_steps) is not int or not 1<=embargo_steps<10000
        or type(window_steps) is not int or not embargo_steps<=window_steps<=10000):
        raise ValueError('bounded state/query/k/window budgets required')
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    before=file_sha(oracle_index); oracle=RealOracle.open(oracle_index); store=None
    try:
        identity=oracle.identity; prefix=min(oracle.prefix,max_states//100); count=prefix*100
        if prefix<embargo_steps+2:raise ValueError('source too short beyond temporal embargo')
        geometry=identity['metadata']['scene']['spec'];width,depth=geometry['width'],geometry['depth']
        legacy=np.empty((count,32),dtype=np.float64); business=np.empty((count,17),dtype=np.float64)
        seqs=np.empty(count,dtype=np.int32); robots=np.empty(count,dtype=np.int16)
        physics=np.empty((count,4),dtype=np.float64); phases=np.empty(count,dtype=np.int8)
        stage_names=[];states_for_queries={}; start=time.perf_counter();loaded=0
        for seq,robot,payload,expected in oracle._db().execute(
                'SELECT seq,robot,payload,sha256 FROM states WHERE seq<=? ORDER BY seq,robot',(prefix,)):
            state=json.loads(payload)
            if digest(state)!=expected:raise ValueError('source state payload SHA mismatch')
            i=loaded;seqs[i],robots[i]=seq,robot
            legacy[i]=real_vector(state,seq,robot);business[i]=business_vector(state,width,depth)
            physics[i]=[state['x'],state['y'],state['battery'],float(state['carrying'])]
            name=family(state['stage'])
            if name not in stage_names:stage_names.append(name)
            phases[i]=stage_names.index(name);loaded+=1
        if loaded!=count:raise ValueError('source prefix does not contain exactly100states/commit')
        load_seconds=time.perf_counter()-start
        # The query set covers the full prefix, with at most one low-SOC case
        # per robot/200-commit episode. Outcome labels never choose the cases.
        chosen=select_queries(seqs,robots,physics,queries,embargo_steps)
        if neo4j:
            from ..real_scale.store import RealStore
            store=RealStore.from_env(timeout=60)
            graph_before=store.head(oracle.run_id)
        else:graph_before=None
        records=[]
        for case,index in enumerate(chosen,1):
            seq,robot=int(seqs[index]),int(robots[index]);cutoff=seq-embargo_steps
            valid=np.flatnonzero((seqs<=cutoff)&(robots!=robot))
            window=valid[seqs[valid]>=max(1,seq-window_steps)]
            query_state=oracle.state(seq,robot)
            physical_delta=physics[valid,:2]-physics[index,:2]
            relevance=((physics[valid,3]==physics[index,3])&(phases[valid]==phases[index])
                &(np.abs(physics[valid,2]-physics[index,2])<=10)
                &((physical_delta*physical_delta).sum(axis=1)<=15**2))
            relevant_available=int(np.count_nonzero(relevance))
            row={'case':case,'seq':seq,'robot_id':robot,'query_key':f'{oracle.run_id}:{seq}:{robot}',
                'query_physical':query_state,'eligible_candidates':len(valid),'bounded_window_candidates':len(window),
                'business_relevant_candidates':relevant_available,
                'maximum_possible_business_precision_at_k':min(k,relevant_available)/k,
                'embargo_steps':embargo_steps,'exclude_entire_query_robot':True,'methods':{}}
            for name,matrix in [('legacy32-exact',legacy),('business17-exact',business)]:
                started=time.perf_counter();ranking=rank_numpy(matrix,matrix[index],valid,k,seqs,robots)
                elapsed=(time.perf_counter()-started)*1000
                # First10k eligible rows are a declared independent scalar
                # cross-check domain. It is NOT reported as full-million scalar timing.
                small=valid[:10000]
                fast=rank_numpy(matrix,matrix[index],small,k,seqs,robots)
                scalar=rank_scalar(matrix,matrix[index],small,k,seqs,robots)
                if {i for i,s in fast}!={i for i,s in scalar}:
                    # Ties at the boundary may differ only within float tolerance.
                    if any(abs(fast[j][1]-scalar[j][1])>1e-10 for j in range(min(len(fast),len(scalar)))):
                        raise AssertionError('matrix exact disagrees with independent scalar cosine')
                results=[]
                for i,score in ranking:
                    state=oracle.state(int(seqs[i]),int(robots[i]))
                    results.append({'key':f'{oracle.run_id}:{int(seqs[i])}:{int(robots[i])}',
                        'seq':int(seqs[i]),'robot_id':int(robots[i]),'cosine':score,
                        'business_relevant':business_relevant(query_state,state)})
                row['methods'][name]={'search_ms':elapsed,'rows':results,
                    'business_precision_at_k':sum(r['business_relevant'] for r in results)/len(results),
                    'scalar_crosscheck_candidates':len(small),'scalar_crosscheck_passed':True}
            # Production advisory exact window is intentionally a different
            # candidate domain from all-prefix ANN; compare separate counts/times.
            started=time.perf_counter();wrank=rank_numpy(business,business[index],window,k,seqs,robots)
            row['bounded_business_exact']={'search_ms':(time.perf_counter()-started)*1000,
                'rows':[{'key':f'{oracle.run_id}:{int(seqs[i])}:{int(robots[i])}','cosine':score} for i,score in wrank]}
            if store:
                truth={r['key'] for r in row['methods']['legacy32-exact']['rows']}
                for limit in (100,1000):
                    started=time.perf_counter()
                    found=store.mirror.query('''CALL db.index.vector.queryNodes($index,$count,$vector)
                        YIELD node,score WHERE node:WHRealState AND node.run_id=$run
                          AND node.seq<=$cutoff AND node.robot_id<>$robot
                        RETURN node.key AS key,node.seq AS seq,node.robot_id AS robot_id,
                          node.real_embedding AS vector,node.real_payload_sha256 AS sha256,score
                        ORDER BY score DESC,node.seq,node.robot_id LIMIT $k''',
                        {'index':VECTOR_INDEX,'count':limit,'vector':legacy[index].tolist(),
                         'run':oracle.run_id,'cutoff':cutoff,'robot':robot,'k':k},timeout=60)
                    ms=(time.perf_counter()-started)*1000
                    if len({r['key'] for r in found})!=len(found):raise ValueError('duplicate ANN result')
                    for r in found:
                        if r['seq']>cutoff or r['robot_id']==robot or r['key']!=f"{oracle.run_id}:{r['seq']}:{r['robot_id']}":
                            raise ValueError('ANN future/self/foreign result')
                        original=oracle.state(r['seq'],r['robot_id'])
                        if digest(original)!=r['sha256'] or not np.allclose(real_vector(original,r['seq'],r['robot_id']),r['vector'],atol=1e-10,rtol=0):
                            raise ValueError('ANN vector differs from source')
                        if abs((1+cosine(legacy[index],r['vector']))/2-r['score'])>2e-4:
                            raise ValueError('ANN score differs from independent cosine')
                    row['methods'][f'neo4j-ann{limit}']={'search_ms':ms,'returned':len(found),
                        'recall_at_k':len(truth&{r['key'] for r in found})/len(truth),
                        'rows':[{key:value for key,value in r.items() if key!='vector'} for r in found]}
            records.append(row)
            print(json.dumps({'case':case,'queries':queries,'seq':seq,'eligible':len(valid)}),flush=True)
        summaries={}
        for method in records[0]['methods']:
            items=[r['methods'][method] for r in records]
            summaries[method]={'search_ms':distribution([r['search_ms'] for r in items])}
            for metric in ('business_precision_at_k','recall_at_k'):
                if metric in items[0]:summaries[method][metric]=distribution([r[metric] for r in items])
        graph_after=store.head(oracle.run_id) if store else None
        report={'version':VERSION,'source':{'oracle_file_sha256':before,'identity':identity,
                'evaluated_prefix':prefix,'actual_states':count,'construction_seconds':load_seconds,
                'array_bytes':sum(v.nbytes for v in (legacy,business,seqs,robots,physics,phases))},
            'queries':len(records),'k':k,'summary':summaries,'cases':records,
            'business_reference_coverage':{'cases_with_relevant_candidates':sum(r['business_relevant_candidates']>0 for r in records),
                'cases_with_at_least_k_relevant_candidates':sum(r['business_relevant_candidates']>=k for r in records),
                'all_queries_retained_in_precision_denominator':True,
                'mean_attainable_precision_at_k':sum(r['maximum_possible_business_precision_at_k'] for r in records)/len(records)},
            'query_selection':'up to half low-SOC carrying episodes unique by(robot,seq//200); remaining points span full prefix; no outcome labels',
            'source_unchanged':file_sha(oracle_index)==before,'graph_before':graph_before,'graph_after':graph_after,
            'graph_head_unchanged':graph_before==graph_after,
            'scope':'read-only source-derived offline selection, all-prefix exact/ANN domain; scalar crosscheck <=10000/query; no100-client latency or policy-benefit certification',
            'business_relevance_definition':'same carrying, same phase family, battery difference <=10pp, distance<=15m; not task success',
            'limitations':['legacy ANN still uses existing32D indexed vectors; business17 is offline/new bounded exact only',
                'all candidate prefixes exclude entire query robot and temporal embargo; no self-hit score',
                'query/index construction and scalar validation cost reported separately from search_ms']}
        if not report['source_unchanged'] or not report['graph_head_unchanged']:raise ValueError('source/graph changed during evaluation')
        (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
        return report
    finally:
        if store:store.close()
        oracle.close()
