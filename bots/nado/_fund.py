import json, os, sys
sys.path.insert(0,'/root/nado_xnn_bot'); os.chdir('/root/nado_xnn_bot')
from nado_protocol.client import create_nado_client, NadoClientMode
try: ctx=create_nado_client(NadoClientMode.MAINNET)
except TypeError: ctx=create_nado_client()
syms=ctx.market.get_all_product_symbols()
# build coin->product_id for *-PERP
perps={}
for ps in syms:
    s=ps.symbol
    if s.endswith('-PERP'):
        perps[s.replace('-PERP','')]=ps.product_id
out={'perps':perps}
ids=list(perps.values())
try:
    fr=ctx.market.get_perp_funding_rates(ids)
    out['funding_raw']=str(fr)[:3000]
except Exception as e:
    out['rates_err']=repr(e)[:300]
# also single BTC to see fields/interval
try:
    one=ctx.market.get_perp_funding_rate(perps['BTC'])
    out['btc_single']=str(one)[:1500]
except Exception as e:
    out['single_err']=repr(e)[:300]
print(json.dumps(out,default=str)[:6000])
