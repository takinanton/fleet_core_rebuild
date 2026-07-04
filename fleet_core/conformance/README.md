# fleet_core/conformance — P2 contract conformance / chaos harness

Kills audit verdict R3 (adapter error-contract inversion) by executable spec:
every `ExchangeClient` method × transport fault must raise TYPED errors
(`ReadUnknown`/`StaleData`/`WriteUnconfirmed`/`VenueRejected`/`RateLimited`),
never neutral values; every write must be readback-confirmed, cross-checked
against what the simulated venue ACTUALLY recorded.

## Layout
- `faults.py` — `TransportGate` + `Fault` injector; patches the LOWEST
  transport layer (`requests.Session.send` / `aiohttp.ClientSession._request`)
  so each adapter's own error handling runs; intercept-all + socket-block
  guard makes live API hits impossible under the harness.
- `checks.py` — the assertions (pytest-free; shared by suite + runner).
- `suite.py` / `test_conformance.py` / `conftest.py` — pytest layer,
  `--venue=` parameterized. P3 crash-at-transition tests = SKIPPED
  placeholders (land with the entry state machine).
- `runner.py` — no-pytest fallback runner for bot venvs.
- `bindings/fake.py` — reference venue + reference CORRECT client (local
  green gate). `bindings/{hl,pacifica,extended,nado}.py` — lazy venue
  bindings: fabricate inert creds, build the real adapter offline, render the
  shared FakeVenue truth store into venue wire shapes.

## Run — local (reference; must be green before venue work)
    cd fleet_core_rebuild && /usr/bin/python3 -m pytest fleet_core/conformance -q

## Run — on each bot host (from the bot root; fleet_core/ sits next to bot/)
    cd <bot root> && venv/bin/python -m pytest fleet_core/conformance -q --venue=hl
    # pytest missing in the venv? identical coverage, zero deps:
    cd <bot root> && venv/bin/python fleet_core/conformance/runner.py --venue=hl
Venues: `hl` `pacifica` `extended` `nado` (one per its own host).

## What to expect on first host runs (by design)
1. `test_raw_adapter_constructs_offline` proves the adapter constructor runs
   under intercept-all with fabricated keys (DRY_RUN=0 posture — live-shaped
   code paths, transport 100% intercepted, sockets blocked).
2. Contract checks SKIP with reason `fleet_core.venues.<name> not landed yet`
   until the venue's ExchangeClient implementation exists — then they run.
3. `UninterceptedRealCall: <METHOD> <URL>` failures = endpoints the venue
   responder doesn't simulate yet. Add a branch to the binding's responder
   for that endpoint and re-run (fail-loud iteration loop; nado's SDK
   discovery will need this most).

## Sensitivity proof (2026-07-02)
`mutation_sensitivity.py` injects 14 mutations re-introducing the audited
defect classes ({}-mask, 0.0-mark, fabricated fill, request-echo partial,
blind FlatResult, naked-success SL, unbounded calls, stale-serve, per-item
drop-and-continue — census 3c) into the reference client — all 14 caught:

    /usr/bin/python3 fleet_core/conformance/mutation_sensitivity.py

## Adversarial-review closures (2026-07-02)
- 3c: `corrupt_item` fault kind (valid envelope, ONE unparseable
  position/order row; responder support in fake + all 4 venue bindings) +
  `read_open_{positions,orders}_corrupt_item_never_dropped` checks — a
  silently dropped item is a failure; ReadUnknown (or venue-truth-resolved)
  is the only legal outcome.
- 3b: nado responder now simulates subaccount_info / indexer matches / engine
  open-orders / trigger list / market_liquidity / execute place+cancel over
  the shared FakeVenue with DIGEST-ONLY acks — fills land at venue px
  (mark ± SLIP), truth only via the matches feed, no_land/partial honored, so
  the fabricated-fill class is exercised on nado (field spellings refined
  on-host via the fail-loud loop).
- 3e: partial-outage selector is per-binding (`partial_outage_match` on
  BoundContext; fake = xyz scope leg, HL = HIP-3 dex leg, single-endpoint
  venues = their one leg) — no venue substrings in checks. Mock oid shapes
  moved onto the scenario surface (`has_live_trigger`/`has_order`/
  `unknown_oid`).
- minor: aiohttp `ClientTimeout(total=None, sock_read=X)` now counts as
  bounded (sock_read honored; connect-only timeouts still count as unbounded
  for reads).
