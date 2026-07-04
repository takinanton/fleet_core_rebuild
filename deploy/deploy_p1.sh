#!/usr/bin/env bash
# P1 fleet_core deploy for ONE venue.
# Usage: deploy_p1.sh <venue> [--restart]
#   venue ∈ hl|pacifica|extended|nado
#   Without --restart: stage + backup + flip files + py_compile + parity ONLY (no restart).
#   With    --restart: also restart unit + 120s journal verify.
# Rollback: deploy_p1.sh <venue> --rollback <TS>
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENUE="$1"; shift || true
MODE="${1:-stage}"

case "$VENUE" in
  hl)       HOST=hl-bot;       DIR=/home/ubuntu/hl_combo_bot;      UNIT=valantis-bot; SUDO="sudo";;
  pacifica) HOST=pacifica-bot; DIR=/home/ubuntu/pacifica_xnn_bot;  UNIT=pacifica-bot; SUDO="sudo";;
  extended) HOST=extended-bot; DIR=/root/extended_xnn_bot;         UNIT=extended-bot; SUDO="";;
  nado)     HOST=nado-bot;     DIR=/root/nado_xnn_bot;             UNIT=nado-bot;     SUDO="";;
  *) echo "unknown venue $VENUE"; exit 2;;
esac

# Per-venue flip list (HL keeps its own strategy_xnn — bespoke combo adapter)
case "$VENUE" in
  hl) MODULES="xnn_core liquidity warmup_backfill risk journal orphan_sweep";;
  *)  MODULES="xnn_core liquidity warmup_backfill strategy_xnn risk journal orphan_sweep";;
esac

TS=$(date -u +%Y%m%dT%H%M%SZ)

if [ "$MODE" = "--rollback" ]; then
  RTS="$2"
  echo "=== ROLLBACK $VENUE to bak_p1_$RTS ==="
  for m in $MODULES; do
    ssh "$HOST" "cd $DIR && [ -f bot/$m.py.bak_p1_$RTS ] && cp bot/$m.py.bak_p1_$RTS bot/$m.py && echo restored $m"
  done
  ssh "$HOST" "$SUDO systemctl restart $UNIT" && echo "restarted $UNIT"
  exit 0
fi

echo "=== P1 deploy $VENUE ($HOST:$DIR unit=$UNIT) TS=$TS modules: $MODULES ==="

echo "--- 1. full backup of live bot/*.py"
ssh "$HOST" "cd $DIR && mkdir -p backups && tar czf backups/pre_p1_$TS.tgz bot/*.py 2>/dev/null; ls -la backups/pre_p1_$TS.tgz"

echo "--- 2. rsync fleet_core package + parity scripts"
rsync -az "$ROOT/fleet_core/" "$HOST:$DIR/fleet_core/"
rsync -az "$ROOT/parity/" "$HOST:$DIR/fleet_core/parity/"

echo "--- 3. per-module backup + shim flip (idempotent: skips already-flipped, keeps original TS)"
FLIP_TS=$(ssh "$HOST" "cat $DIR/.p1_flip_ts 2>/dev/null" || true)
if [ -z "$FLIP_TS" ]; then
  FLIP_TS="$TS"
  ssh "$HOST" "echo $FLIP_TS > $DIR/.p1_flip_ts"
fi
for m in $MODULES; do
  if ssh "$HOST" "grep -q 'fleet_core loader shim' $DIR/bot/$m.py 2>/dev/null"; then
    echo "already-flipped $m (original backup bak_p1_$FLIP_TS)"
    continue
  fi
  scp -q "$ROOT/shims/$m.py" "$HOST:$DIR/bot/.$m.py.p1shim"
  ssh "$HOST" "cd $DIR && cp bot/$m.py bot/$m.py.bak_p1_$FLIP_TS && mv bot/.$m.py.p1shim bot/$m.py && echo flipped $m"
done

echo "--- 4. py_compile (fleet_core + live bot files)"
ssh "$HOST" "cd $DIR && ls fleet_core/*.py bot/*.py | grep -v -E 'bak|orig' | xargs venv/bin/python -m py_compile && echo PY_COMPILE_OK"

echo "--- 5. parity: original module vs canonical (on host, in venv)"
PARITY_FAIL=0
for m in $MODULES; do
  if ssh "$HOST" "test -f $DIR/fleet_core/parity/${m}_parity.py"; then
    # importlib loaders need a .py suffix — parity runs against a temp .py copy of the backup
    ssh "$HOST" "mkdir -p /tmp/p1_parity && cp $DIR/bot/$m.py.bak_p1_$FLIP_TS /tmp/p1_parity/old_$m.py"
    set +e
    ssh "$HOST" "cd $DIR && timeout 120 venv/bin/python fleet_core/parity/${m}_parity.py --old /tmp/p1_parity/old_$m.py --new fleet_core/$m.py --venue $VENUE" 2>/tmp/p1_parity_err_$m
    RC=$?
    if [ "$RC" = "2" ]; then  # argparse usage error — script takes no --venue
      ssh "$HOST" "cd $DIR && timeout 120 venv/bin/python fleet_core/parity/${m}_parity.py --old /tmp/p1_parity/old_$m.py --new fleet_core/$m.py"
      RC=$?
    fi
    set -e
    if [ "$RC" = "0" ]; then echo "PARITY_OK $m"; else cat /tmp/p1_parity_err_$m 2>/dev/null | tail -5; echo "PARITY_FAIL $m (rc=$RC)"; PARITY_FAIL=1; fi
  else
    echo "PARITY_MISSING $m"; PARITY_FAIL=1
  fi
done
if [ "$PARITY_FAIL" = "1" ]; then
  echo "!!! PARITY FAILED — NOT restarting. Files are flipped on disk but old process still runs old code."
  echo "!!! Roll back with: $0 $VENUE --rollback $FLIP_TS"
  exit 1
fi

echo "TS=$TS"
if [ "$MODE" != "--restart" ]; then
  echo "=== staged only (no restart). Restart with: $0 $VENUE --restart (re-runs whole flow idempotently) or manually: ssh $HOST '$SUDO systemctl restart $UNIT'"
  exit 0
fi

echo "--- 6. restart"
ssh "$HOST" "$SUDO systemctl restart $UNIT"
sleep 5
ssh "$HOST" "systemctl is-active $UNIT"

echo "--- 7. journal verify (120s watch)"
ssh "$HOST" "$SUDO journalctl -u $UNIT --since '1 minute ago' -n 200 --no-pager | tail -60"
echo "--- waiting 120s for a scan cycle, then checking for errors"
sleep 120
ssh "$HOST" "$SUDO journalctl -u $UNIT --since '3 minutes ago' --no-pager | grep -E 'ERROR|CRITICAL|Traceback|Exception' | grep -vE 'no error' | head -20" && echo "!!! errors above — inspect" || echo "JOURNAL_CLEAN"
ssh "$HOST" "$SUDO journalctl -u $UNIT --since '3 minutes ago' --no-pager | tail -25"
echo "=== done $VENUE TS=$TS (rollback: $0 $VENUE --rollback $TS)"
