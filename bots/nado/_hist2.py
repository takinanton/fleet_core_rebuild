import json, os, sys, time
sys.path.insert(0,'/root/nado_xnn_bot'); os.chdir('/root/nado_xnn_bot')
from nado_protocol.client import create_nado_client, NadoClientMode
import nado_protocol.indexer_client.types.query as tq
try: ctx=create_nado_client(NadoClientMode.MAINNET)
except TypeError: ctx=create_nado_client()
perps={'BTC':2,'ETH':4,'SOL':8,'HYPE':16,'ENA':72,'DOGE':52,'AVAX':64,'LINK':74,'XRP':10,'BNB':14,'ASTER':48,'SUI':24,'AAVE':26,'LTC':76,'ARB':62}
pids=list(perps.values())
P=tq.IndexerMarketSnapshotsParams
I=tq.IndexerMarketSnapshotInterval
# daily granularity, 8 points
p=P(interval=I(count=8, granularity=86400), product_ids=pids)
ms=ctx.market.get_market_snapshots(p)
snaps=ms.snapshots
rows=[]
for s in snaps:
    rows.append({'ts':s.timestamp, 'fr':{k:int(v) for k,v in s.funding_rates.items()}})
print(json.dumps({'perps':perps,'rows':rows}))
