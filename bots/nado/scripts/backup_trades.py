import sqlite3, sys, os, time, glob, gzip, shutil
db, bkdir = sys.argv[1], sys.argv[2]
if not os.path.exists(db):
    print("no DB:", db); sys.exit(0)
os.makedirs(bkdir, exist_ok=True)
ts = time.strftime("%Y%m%d_%H%M%S")
dest = os.path.join(bkdir, f"trades_{ts}.db")
src = sqlite3.connect(db); dst = sqlite3.connect(dest)
with dst: src.backup(dst)           # ONLINE backup API — safe during writes
src.close(); dst.close()
with open(dest,"rb") as fi, gzip.open(dest+".gz","wb") as fo: shutil.copyfileobj(fi, fo)
os.remove(dest)
keep = 72                            # ~3 days hourly
for old in sorted(glob.glob(os.path.join(bkdir,"trades_*.db.gz")))[:-keep]:
    os.remove(old)
sz = os.path.getsize(dest+".gz")
print(f"backup OK {dest}.gz ({sz}B), retained<= {keep}")
