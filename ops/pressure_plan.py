"""Strict, bounded read-only GUI/CLI load plans; no shell or arbitrary workload."""
import math

PROFILES = {
    'quick': {'label': '连通性小检查', 'clients': 1, 'requests': 3, 'duration': 30,
              'mode': 'closed', 'query_id': 'self'},
    'normal': {'label': '100机器人日常错峰查询', 'clients': 100, 'requests': 0, 'duration': 10,
               'mode': 'periodic', 'query_id': 'self'},
    'burst': {'label': '100客户端轻查询突发', 'clients': 100, 'requests': 2, 'duration': 60,
              'mode': 'closed', 'query_id': 'self'},
    'heavy': {'label': '100客户端全局重查询突发', 'clients': 100, 'requests': 2, 'duration': 60,
              'mode': 'closed', 'query_id': 'global'},
}
QUERIES = ('self', 'readiness', 'global', 'local_awareness', 'energy',
           'assistance', 'memory', 'progress', 'mixed')
FIELDS = {'clients', 'processes', 'requests', 'duration', 'mode', 'query_id',
          'interval', 'rate', 'timeout', 'window_steps', 'event_limit', 'history_limit', 'self_percent'}


def normalize(profile, override=None):
    if profile not in PROFILES:
        raise ValueError('未知方案')
    override = {} if override is None else override
    if not isinstance(override, dict) or set(override)-FIELDS:
        raise ValueError('自定义参数含未知字段')
    base = PROFILES[profile]
    defaults = {**base, 'processes': base['clients'], 'interval': 5, 'rate': 20,
             'timeout': 15, 'window_steps': 60, 'event_limit': 10000,
             'history_limit': 3601, 'self_percent': 80}
    value = {**defaults, **override}
    for name, low, high in [('clients',1,100), ('processes',1,100), ('requests',0,10000),
                           ('window_steps',1,3600), ('event_limit',1,10000),
                           ('history_limit',1,10000), ('self_percent',1,99)]:
        if type(value[name]) is not int or not low <= value[name] <= high:
            raise ValueError(f'{name}应为{low}—{high}的整数')
    for name, low, high in [('duration',1,300), ('timeout',1,60), ('interval',.1,3600), ('rate',.1,1000)]:
        x = value[name]
        if type(x) not in (int,float) or not math.isfinite(x) or not low <= x <= high:
            raise ValueError(f'{name}应为{low}—{high}的有限数值')
    if value['processes'] > value['clients']:
        raise ValueError('进程数不能超过客户端数')
    if value['requests']*value['clients'] > 100000:
        raise ValueError('单轮请求预算不能超过100000')
    if value['mode'] == 'open' and value['duration']*value['rate'] > 100000:
        raise ValueError('固定到达率预算不能超过100000')
    if value['mode'] not in ('closed','periodic','open') or value['query_id'] not in QUERIES:
        raise ValueError('未知负载模式或查询种类')
    if value['mode']=='closed' and value['requests']==0:
        raise ValueError('闭环模式请设置每客户端请求上限，避免无界高速负载')
    if value['mode']=='periodic' and value['clients']*(math.ceil(value['duration']/value['interval'])+1)>100000:
        raise ValueError('错峰方案的计划到达预算不能超过100000')
    value['profile'] = profile
    value['label'] = base['label']+('（自定义）' if any(value[k]!=defaults[k] for k in override) else '')
    return value


def requires_confirmation(plan):
    return not (plan['query_id'] in ('self','readiness') and plan['clients'] == 1
                and 1 <= plan['requests'] <= 3 and plan['mode'] == 'closed' and plan['duration'] <= 30)


def cases(plan):
    if plan['query_id'] == 'mixed':
        return [('self', plan['self_percent']), ('global', 100-plan['self_percent'])]
    return [(plan['query_id'], 1)]
