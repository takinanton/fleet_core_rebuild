#!/usr/bin/env bash
# Snapshot<->live sync guard (drift class: rebuild snapshot != live VPS code).
# Instance 2026-07-02: live nado trader.py carried the P1b eff_lev port that the
# rebuild snapshot was missing — snapshot patched blind would have REVERTED a live fix.
# Compares md5 of every live bot/*.py against fleet_core_rebuild/bots/<venue>/bot/.
# Live is source of truth: any DRIFT line means PULL LIVE INTO SNAPSHOT FIRST,
# then apply your patch on top. Exit 1 on any drift. Run BEFORE patching the snapshot.
set -u
BOTS_DIR="$(cd "$(dirname "$0")/../bots" && pwd)"
FAIL=0
while read -r venue target; do
  host=${target%%:*}; dir=${target#*:}
  # ssh -n: MUST NOT read stdin — it would eat the remaining heredoc venue lines and
  # silently skip 3 of 4 venues (partial-coverage bug caught 2026-07-02, first run).
  live=$(ssh -n -o ConnectTimeout=10 "$host" "cd $dir && md5sum bot/*.py" 2>/dev/null) || {
    echo "FAIL $venue: ssh/md5sum failed ($target)"; FAIL=1; continue; }
  while read -r sum path; do
    base=$(basename "$path")
    loc="$BOTS_DIR/$venue/bot/$base"
    if [ ! -f "$loc" ]; then
      echo "DRIFT $venue/$base: exists on live, MISSING in snapshot"; FAIL=1; continue
    fi
    lsum=$(md5 -q "$loc" 2>/dev/null || md5sum "$loc" | awk '{print $1}')
    [ "$sum" = "$lsum" ] || { echo "DRIFT $venue/$base: live != snapshot"; FAIL=1; }
  done <<< "$live"
  # P1 fleet_core (2026-07-02): live <dir>/fleet_core/*.py must match the local canonical
  # fleet_core_rebuild/fleet_core/ — cross-venue divergence here IS the R1 drift class.
  CORE_DIR="$BOTS_DIR/../fleet_core"
  livec=$(ssh -n -o ConnectTimeout=10 "$host" "cd $dir && md5sum fleet_core/*.py" 2>/dev/null)
  if [ -n "$livec" ]; then
    while read -r sum path; do
      base=$(basename "$path")
      loc="$CORE_DIR/$base"
      if [ ! -f "$loc" ]; then
        echo "DRIFT $venue/fleet_core/$base: on live, missing in canonical"; FAIL=1; continue
      fi
      lsum=$(md5 -q "$loc" 2>/dev/null || md5sum "$loc" | awk '{print $1}')
      [ "$sum" = "$lsum" ] || { echo "DRIFT $venue/fleet_core/$base: live != canonical"; FAIL=1; }
    done <<< "$livec"
  fi
done <<EOF
extended extended-bot:/root/extended_xnn_bot
pacifica pacifica-bot:/home/ubuntu/pacifica_xnn_bot
nado nado-bot:/root/nado_xnn_bot
hl hl-bot:/home/ubuntu/hl_combo_bot
EOF
if [ $FAIL -eq 0 ]; then echo "snapshot-live sync: PASS (4 venues, bot/*.py)"; else echo "snapshot-live sync: FAIL — pull live files into snapshot before patching"; fi
exit $FAIL
