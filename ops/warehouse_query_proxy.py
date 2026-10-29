"""Loopback-only query forwarding; the single owner still selects cutoffs.

Full response bytes are delivered without a second gateway JSON decode/encode.
No response cache, oracle or retry lives here.
"""
from urllib.parse import urlsplit


def validate_upstreams(values):
    if not isinstance(values,(list,tuple)) or not 1<=len(values)<=8:
        raise ValueError('one to eight query-only loopback endpoints required')
    result=[]
    for value in values:
        if not isinstance(value,str):raise ValueError('query endpoint must be a URL')
        url=urlsplit(value)
        try:port=url.port
        except ValueError:raise ValueError('invalid query endpoint port') from None
        if (url.scheme!='http' or url.hostname not in ('127.0.0.1','::1') or port is None
                or not 1<=port<=65535 or url.username is not None or url.password is not None
                or url.path not in ('','/') or url.query or url.fragment):
            raise ValueError('query endpoint must be an explicit loopback HTTP origin')
        normalized=value.rstrip('/')
        if normalized in result:raise ValueError('duplicate query endpoint')
        result.append(normalized)
    return tuple(result)


class HttpCatalogClient:
    def __init__(self,endpoints,*,transport=None):
        import httpx
        self.endpoints=validate_upstreams(endpoints)
        self.clients=[httpx.AsyncClient(base_url=url,trust_env=False,follow_redirects=False,
            timeout=httpx.Timeout(20.),limits=httpx.Limits(max_connections=3,max_keepalive_connections=3),
            transport=transport) for url in self.endpoints]
        self.active=[0]*len(self.clients);self.cursor=0

    async def execute(self,body):
        # Owner event loop makes this selection before the first await.
        # Least-inflight avoids keepalive connections pinning load to one PID.
        size=len(self.clients)
        index=min(range(size),key=lambda i:(self.active[i],(i-self.cursor)%size))
        self.cursor=(index+1)%size;self.active[index]+=1
        try:
            response=await self.clients[index].post('/api/knowledge/query',json=body)
            if (response.status_code not in (200,400,503)
                    or response.headers.get('content-type','').split(';')[0]!='application/json'):
                raise RuntimeError('unexpected query-only service response')
            return response
        finally:self.active[index]-=1

    async def close(self):
        for client in self.clients:await client.aclose()
