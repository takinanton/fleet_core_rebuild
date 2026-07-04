import json, os, sys, time
sys.path.insert(0,'/root/nado_xnn_bot'); os.chdir('/root/nado_xnn_bot')
from nado_protocol.client import create_nado_client, NadoClientMode
import nado_protocol.indexer_client.types.query as tq
try: ctx=create_nado_client(NadoClientMode.MAINNET)
except TypeError: ctx=create_nado_client()
out={}
# 1) contracts info — may carry funding interval / period
try:
    ci=ctx.market.get_perp_contracts_info()
    out['contracts_info']=str(ci)[:1200]
except Exception as e:
    out['ci_err']=repr(e)[:200]
# 2) market snapshots over 7d for BTC(2) ETH(4) to get cumulative funding -> realized mean
now=int(time.time())
gran=86400  # daily granularity guess
times=[now-i*86400 for i in range(0,8)]
for name,params_cls in [('mkt','IndexerMarketSnapshotsParams')]:
    pass
try:
    P=tq.IndexerMarketSnapshotsParams
    import inspect
    out['mkt_snap_sig']=str(inspect.signature(P))
    p=P(interval={'count':8,'granularity':86400}, product_ids=[2,4,8])
    ms=ctx.market.get_market_snapshots(p)
    out['mkt_snap']=str(ms)[:1500]
except Exception as e:
    out['ms_err']=repr(e)[:300]
print(json.dumps(out,default=str)[:4500])
