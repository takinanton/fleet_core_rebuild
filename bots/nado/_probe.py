import json, os, sys
sys.path.insert(0,'/root/nado_xnn_bot')
os.chdir('/root/nado_xnn_bot')
out={}
try:
    from nado_protocol.client import create_nado_client, NadoClientMode
    # read-only: no signer needed for market queries
    try:
        ctx = create_nado_client(NadoClientMode.MAINNET)
    except TypeError:
        ctx = create_nado_client()
    out['client']='ok'
    # list products
    try:
        prods = ctx.market.get_all_engine_markets()
        out['markets_type']=str(type(prods))
        # try to enumerate product symbols
        syms = ctx.market.get_all_product_symbols()
        out['symbols']=str(syms)[:2000]
    except Exception as e:
        out['markets_err']=repr(e)[:300]
except Exception as e:
    out['client_err']=repr(e)[:400]
print(json.dumps(out, default=str)[:4000])
