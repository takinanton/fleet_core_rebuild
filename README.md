# fleet_core_rebuild

Shared execution and safety core extracted from a multi-venue crypto trading bot fleet
(Hyperliquid, Extended, Pacifica, Nado, IB, Bybit). The goal is one hardened implementation
of the parts every venue-specific bot needs, instead of six separately drifting copies.

## What lives here

- **Order lifecycle contract** — a common state machine (`unsent → acked → resting → filled | cancelled | rejected`) with read-back verification after every mutation. No "naked success" writes — an order is only considered placed once the venue confirms it appears in the live order table.
- **SL / TP manager** — reduce-only stop-loss and take-profit placement, heal loops, and cross-venue flatten semantics that hold rather than oophan a hedge when the hedging venue is down.
- **Position reconciler** — periodic diff between local journal and venue-reported positions, with configurable heal actions.
- **Failsafe primitives** — atomic emergency close, watchdog, host-liveness probe.
- **Test harness** — contract tests + chaos harness (dropped packets, half-open sockets, duplicate acks) so venue adapters can be verified against the same suite.

## Layout

- `bot/` — engine, order manager, SL manager, journal.
- `proofs/` — host-run logs and reconciliation dumps for post-mortems.
- `tests/` — contract + chaos suites.

## Non-goals

- No strategy code. Strategy lives in per-venue bots that import this core.
- No wallet / signing code. Venue adapters own that.

## Status

Under active hardening — Phase 3 engine tests passing (191/191), Phase 4c 4/4.
Extended venue in shadow mode pending cutover gate.