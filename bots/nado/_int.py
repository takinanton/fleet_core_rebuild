import json, os, sys
sys.path.insert(0,'/root/nado_xnn_bot'); os.chdir('/root/nado_xnn_bot')
from nado_protocol.client import create_nado_client, NadoClientMode
try: ctx=create_nado_client(NadoClientMode.MAINNET)
except TypeError: ctx=create_nado_client()
out={}
# Inspect SDK funding model fields + any docstring hinting interval
import nado_protocol.indexer_client.query as q
import inspect
src=inspect.getsource(q.IndexerClient.get_perp_funding_rate)
out['fund_src']=src[:800]
# Look for product snapshots which often carry funding + oracle + interval semantics
try:
    import nado_protocol.indexer_client.types.query as tq
    out['snap_params']=[n for n in dir(tq) if 'Snapshot' in n or 'Funding' in n]
except Exception as e:
    out['e1']=repr(e)[:200]
# Try historical funding via product snapshots over a time range to measure cadence
try:
    snaps_fn=ctx.market.get_product_snapshots
    out['snap_sig']=str(inspect.signature(snaps_fn))
except Exception as e:
    out['e2']=repr(e)[:200]
print(json.dumps(out,default=str)[:3500])
