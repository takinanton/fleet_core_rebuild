import json, os, sys
sys.path.insert(0,'/root/nado_xnn_bot'); os.chdir('/root/nado_xnn_bot')
from nado_protocol.client import create_nado_client, NadoClientMode
try: ctx=create_nado_client(NadoClientMode.MAINNET)
except TypeError: ctx=create_nado_client()
syms=ctx.market.get_all_product_symbols()
perps={ps.symbol.replace('-PERP',''):ps.product_id for ps in syms if ps.symbol.endswith('-PERP')}
fr=ctx.market.get_perp_funding_rates(list(perps.values()))
# fr is dict pid->obj
res={}
for coin,pid in perps.items():
    o=fr.get(str(pid)) or fr.get(pid)
    if o is not None:
        rate=int(o.funding_rate_x18)/1e18
        res[coin]={'pid':pid,'rate_daily':rate,'update_time':o.update_time}
print(json.dumps(res))
