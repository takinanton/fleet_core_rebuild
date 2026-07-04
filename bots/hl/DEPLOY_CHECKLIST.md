# XNN → HL framework: DRY deploy checklist (2026-06-10)

Источник: локальная копия `/Users/ak/Desktop/HL/hl_xnn_prep/framework/` (live-копия hl-bot VPS + xnn-патчи ниже). Деплой = **НОВАЯ** директория (репоинт-дисциплина: per-dir trades.db/.env — память `feedback_deploy_repoint_splits_per_dir_state`). Старый uk_v102-чеклист этой копии замещён (он живёт на VPS в hl_bot_v2_dev).

## 0. Что в этой копии изменено vs live (верифицировать ПОСЛЕ rsync, ДО старта)
| # | Файл | Патч |
|---|------|------|
| 1 | `bot/xnn_core.py` | NEW — чистая математика XNN, 1:1 порт `xnn_long_stateless.py` (parity-тест пройден 5856 чеков / 0 расхождений) |
| 2 | `bot/strategy_xnn.py` | NEW — адаптер контракта strategy_uk_v102 (Signal/Position/PositionManager/scan_for_signal/scan_for_short_signal/compute_indicators/_estimate_tick), per-TF конфиг ВШИТ (XNN_TF_CONFIG) |
| 3 | `bot/scanner.py` | import → strategy_xnn (scanner.py:37); `SCAN_CANDLES_LIMIT` env вместо hardcode 300 |
| 4 | `bot/main.py` | import → strategy_xnn (2 места); `RESTING_ORDERS_ENABLE` гейт на resting-блок; untracked-protect sweep: skip при DRY_RUN + `FOREIGN_SKIP_PREFIXES` |
| 5 | `bot/trader.py` | import → strategy_xnn (trader.py:43) |
| 6 | `bot/resting_orders.py` | import → strategy_xnn (логика модуля = Donchian, ОСТАЁТСЯ выключенной env-гейтом) |
| 7 | `bot/universe.py` | `UNIVERSE_HIP3_ENABLE` (скип dex=xyz + regime force-include) + `UNIVERSE_SYMBOL_EXCLUDE` |
| 8 | `bot/trader.py` | review-fix 06-10: DRY-гейты в manage/_place_sl_with_retry/_ensure_flat/_emergency_close/attempt_entry (`_dry_block`) — DRY=0 ордеров enforced КОДОМ; eff_lev=min(LEVERAGE, asset.max_leverage) + реальный `update_leverage()` перед entry (abort при fail) + MM-cap на eff_lev |
| 9 | `bot/main.py` | review-fix 06-10: adopt скипает FOREIGN_SKIP_PREFIXES (код-гард, не процедура); adopt целиком скипается в DRY; startup-ассерт `XNN_EXPECT_EMPTY_DB=1` → open-строки в DB = refuse to start |
| 10 | `bot/exchange_hl.py` | review-fix 06-10: list_open_sl_orders/list_open_entry_trigger_orders — per-dex `_retry_429`, при фейле любого dex RAISE (не `[]`) → trader assume-live работает (анти-«429→false-naked→дубль-SL/emergency-close»); candles(): drop последней строки ТОЛЬКО если её ts в forming-баре; `invalidate_candles_cache()` |
| 11 | `bot/scanner.py` | review-fix 06-10: freshness-guard — decision-бар обязан == бару, закрывшему скан; stale → форс-refetch мимо кэша → если всё ещё stale, skip LOUD (никаких решений на баре N-1) |
| 12 | `bot/xnn_core.py` | review-fix 06-10: ATR14 ВЕРИФИЦИРОВАНА == bt-1 (precompute.py:60-68, TR max-of-3, ewm α=1/14 adjust=False); trail_stop ПЕРЕПИСАН verbatim с движка bt (engine.py:2101-2135 fractal-pivot + ratchet, up_to_idx=i-1) — старая uk-PM min-of-window формула РАСХОДИЛАСЬ с bt; `_strong` raise (не тихий False) при use_trend_gate без ema200 |

Проверка: `grep -rn "from bot.strategy_uk_v102 import" bot/*.py | grep -v bak` → **0 строк**; `python3 -m py_compile bot/*.py` → OK.

## 1. Новая директория на hl-bot VPS
```bash
ssh hl-bot   # доступ через ~/.ssh/bot_fleet (память reference_bt1_ssh_fleet_access)
mkdir -p /home/ubuntu/hl_xnn_bot
rsync -a --exclude '*.bak*' --exclude 'data/' --include '.env.xnn.template' --exclude '.env*' <локальная framework/> ubuntu@hl-bot:/home/ubuntu/hl_xnn_bot/
# ВАЖНО: --include '.env.xnn.template' ДО --exclude '.env*' (rsync: первый матч решает) —
# иначе exclude матчит basename и выкидывает сам template → шаг §3 (cp) падает.
cd /home/ubuntu/hl_xnn_bot && python3 -m venv venv
venv/bin/pip install -r /home/ubuntu/hl_bot_v2_dev/requirements.txt  # или pip freeze из старого venv
mkdir -p data
```

## 2. СВЕЖАЯ trades.db — КРИТИЧНО (риск R1 карты foreign-safety)
- `data/` НЕ копировать. `init_db()` создаст пустую `data/trades.db` (PROJECT_ROOT = новая dir → config.py:412).
- Старая DB со строками `status='open'` (xyz_*) → adopt подхватил бы ЧУЖИЕ позиции (main.py:165-174, `_IS_PRIMARY` адоптит любой TF) и стал бы ими УПРАВЛЯТЬ: trail re-place (trader.py:909-927), SL-hit → market-close (trader.py:875-880), re-anchor ±6% (trader.py:734-754). Пустая DB = реконсайлер не трогает ничего: adopt строго от DB-строк (main.py:127,156-169); closed_by_exchange — только tracked (trader.py:846-868).
- После первого старта: `sqlite3 data/trades.db "select count(*) from trades where status='open'"` → 0.

## 3. .env
```bash
cp bot/.env.xnn.template /home/ubuntu/hl_xnn_bot/.env.xnn
# РУКАМИ из /home/ubuntu/hl_bot_v2_dev/.env.a перенести ТОЛЬКО:
#   HYPERLIQUID_AGENT_PRIVATE_KEY, HYPERLIQUID_ACCOUNT_ADDRESS
chmod 600 .env.xnn && ln -sf .env.xnn .env
```
- `ln -sf .env.xnn .env` обязателен: config.py:24-26 dotenv-грузит `PROJECT_ROOT/.env` (hardcode имени); systemd EnvironmentFile тоже подаёт env (os.getenv приоритетнее dotenv) — симлинк страхует ручные запуски `venv/bin/python -m bot.main`.
- Обязательные значения (всё уже в template): `DRY_RUN=1`, `XNN_EXPECT_EMPTY_DB=1`, `RESTING_ORDERS_ENABLE=0`, `FOREIGN_SKIP_PREFIXES=xyz_`, `UNIVERSE_HIP3_ENABLE=0`, `UNIVERSE_SYMBOL_EXCLUDE=PAXG`, `UNIVERSE_TOP_N=0`, `CRYPTO_SHORT_ONLY=0`, `TP1_PARTIAL_FRAC=0`, `MAX_RUN_R=1000`, `VSTOP_BUFFER_PCT=0.15`, `VSTOP_PIVOT_WINDOW=3` (= bt engine default, engine.py:1207 — НЕ 8), `SCAN_CANDLES_LIMIT=3000`, `MAX_CONCURRENT=999` (non-binding — user-rule 06-10: MC=10 был uk_v102-легаси и душил XNN 41.8%→2.4% CAGR; MM50 = реальный safety), `ENTRY_LIMIT_CAP_PCT=0.0024`, `LEVERAGE=100` (ceiling → биндит per-asset MAX; контроль = RISK), `WORKING_TFS=1d,8h,4h`, `SHORT_TFS=4h`.

## 4. systemd unit `/etc/systemd/system/hl-xnn-bot.service` (образец: systemd_units.txt:1-20)
```ini
[Unit]
Description=HL XNN Bot (1d+8h+4h long, 4h short — xnn_long_stateless port, DRY)
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/hl_xnn_bot
EnvironmentFile=/home/ubuntu/hl_xnn_bot/.env.xnn
ExecStart=/home/ubuntu/hl_xnn_bot/venv/bin/python -m bot.main
Restart=always
RestartSec=15
StandardOutput=journal
StandardError=journal
SyslogIdentifier=hl-xnn-bot

[Install]
WantedBy=multi-user.target
```
`sudo systemctl daemon-reload && sudo systemctl enable --now hl-xnn-bot`

## 5. Архив старых hl_bot_v2* — tar, НЕ rm (там state 8 xyz позиций)
```bash
cd /home/ubuntu
tar czf backup_uk_v102_20260610.tar.gz hl_bot_v2_dev/   # + прочие hl_bot_v2* директории если есть
tar tzf backup_uk_v102_20260610.tar.gz | grep trades.db   # проверить что DB в архиве
# hl_bot_v2_dev ОСТАВИТЬ на диске: его trades.db = единственный маппинг 8 xyz поз → SL oid
sudo systemctl disable hl-bot-a hl-bot-b   # stopped 06-10; disable чтобы reboot не поднял uk_v102
```

## 6. 8 xyz позиций остаются ЧУЖИМИ — верификация foreign-safety
Шесть независимых механизмов, подтвердить каждый:
0. **КОД-гард adopt** (review-fix 06-10): `_adopt_db_open_positions` скипает `FOREIGN_SKIP_PREFIXES` с log.error + в DRY adopt выключен целиком + `XNN_EXPECT_EMPTY_DB=1` refuse-to-start при непустой DB. Защита больше НЕ только процедурная.
1. **Пустая trades.db** (п.2) → реконсайлер/adopt не видит их вообще.
2. **`FOREIGN_SKIP_PREFIXES=xyz_`** (патч main.py) → untracked-protect sweep НЕ поставит свой ±6% reduce-only SL на xyz даже если их статичный SL транзиентно невидим (риск R2 карты); в DRY sweep пропускается целиком (новый guard).
3. **`UNIVERSE_HIP3_ENABLE=0`** → xyz_* отсутствуют в universe → не сканируются, новые входы по ним невозможны (закрывает R5/R6: SKIP_COINS мёртв, xyz_VIX/DXY торгуемы были бы).
4. **account_coins dedup** (scanner.py:149-150, main.py:690) → вход по любой held-монете аккаунта заблокирован.
Проверка: journalctl без `PROTECTED untracked`/`untracked-protect` по xyz_*; `Restored 0 open positions`; Hyperliquid UI: 8 поз + статичные SL нетронуты (UI = source of truth).
Принятые side-effects: (а) чужой notional входит в MM-cap (trader.py:225-232) — учитывать при оценке свободного капа; (б) при LIVE-флипе startup_orphan_sweep отменит осиротевшие НЕ-reduce-only entry-триггеры старого бота по монетам БЕЗ позиций (resting_orders.py:342-353) — желаемое; reduce-only SL он не трогает (фильтр exchange_hl.py:1124-1126).

## 7. Верификация после старта (journalctl; память verify_after_deploy: active≠работает)
```bash
journalctl -u hl-xnn-bot -f
```
- [ ] `bot starting: exchange=hyperliquid dry_run=True`
- [ ] `Config: tfs=('1d', '8h', '4h')` + `SHORT enabled on TFs: ('4h',)`
- [ ] 3× `WARNING xnn_core: allow_ungated=True` на старте (msi=1 — ожидаемый маркер xnn-деплой-конфига, НЕ ошибка)
- [ ] `UNIVERSE_HIP3_ENABLE=0 — skipping HIP-3` ; `python -m bot.universe` → без xyz_*, без PAXG, все ≥$1M
- [ ] `XNN_EXPECT_EMPTY_DB=1: trades.db has 0 open rows — OK` + `DRY_RUN: position adopt/restore SKIPPED` (review-fix 06-10: в DRY adopt выключен кодом; «Restored N» в DRY быть не может)
- [ ] НЕТ `[DRY-RUN] BLOCKED` (trader.py guard: появление = где-то возникла позиция/ордер-путь в DRY — разбор)
- [ ] НЕТ `RESTING LONG/SHORT` (гейт работает), НЕТ untracked-protect действий
- [ ] На закрытии бара: `Scanning N coins tf=...` → сигналы только как `[DRY-RUN] Signal`
- [ ] 0 traceback (ImportError/AttributeError по Signal-полям = провал контракта)

## 8. DRY ≥ 2 полных скан-цикла до флипа
- ≥2 закрытия КАЖДОГО TF (4h ≥8ч, 8h ≥16ч, 1d ≥2 суток).
- Sanity сигнал-частоты: union-режим 1d/8h не должен сыпать десятки сигналов/день (если сыпет — гейты не работают, СТОП).
- `sqlite3 data/trades.db "select reason,count(*) from rejected_signals group by 1"` — профиль реджектов (price_gate_skip / liq_below_min_trade / stale_signal).
- **SIGNAL-PARITY до флипа** (память signal_parity_before_strategy_eval): DRY-сигналы сверить с bt-эмиттером на тех же закрытых барах (bt-1, живой HL-датасет). Расхождение = СТОП, разбор.

## 9. Открытые вопросы / зафиксированные расхождения с bt
| # | Вопрос | Статус |
|---|--------|--------|
| 1 | ATR14 | ✅ ЗАКРЫТ 06-10: формула harness прочитана на bt-1 (`scripts/indicators/precompute.py:60-68`, ATR_P=14): TR=max(H−L,\|H−pc\|,\|L−pc\|), `ewm_mean(alpha=1/14, adjust=False)` == `xnn_core.compute_xnn_atr14` 1:1. Residual: bt сеет EWM с полной истории, live с окна — затухает e^(−n/14), при 3000 барах ноль |
| 2 | Trail | ✅ ЗАКРЫТ 06-10: ветка bt прочитана (`engine.py:1795-1797` → `_trail_long_sl/_trail_short_sl` :2101-2135, fractal-pivot окно `vstop.pivot_window` default 3 :1207, буфер из конфига, ratchet, up_to_idx=i−1); xnn-конфиги pivot_window НЕ задают → все xnn-раны = 3. `xnn_core.trail_stop` переписан VERBATIM; `VSTOP_PIVOT_WINDOW=3` (старое 8 = uk-дефолт, расходилось). Остаточный delta: live df в manage = 50 баров → пивоты старше ~50 баров не видны (bt сканирует всю историю; ratchet делает это безвредным кроме экзотики «пивот глубже 50 баров сразу после входа») |
| 3 | Entry: bt fill = close сигнал-бара; live = continuation-gate [close, close×1.0024] TTL 30s, иначе skip (trader.py:590-613) — fill-распределение live≠bt | принято; skip-rate мерить в DRY |
| 4 | max_concurrent: bt=5 (внутренний гейт xnn:491 в порт НЕ перенесён) | 06-10: live=999 non-binding (user-rule, deployment-sim: peak 35 concurrent); ≠ bt=5 → портфельная траектория может отличаться от bt-цифр |
| 5 | ema_fit_window скейл 1d=60 / 8h=180 / 4h=360 (×3/×6) — «как в бэктесте/phase15», phase15-конфиг локально недоступен | сверить с bt-1 до флипа |
| 6 | Окно 3000 баров: для монет старше окна seed-residual 610-EMA ≤ ~4e-4; точная parity для монет моложе окна | принято; при parity-fail → SCAN_CANDLES_LIMIT=5000. ⚠️ ДОБАВКА 06-10: parity-эмиттер гонять ТОЛЬКО на живом HL-датасете с тем же стартом окна; ожидания сигнал-рейта из Binance-рана НЕВАЛИДНЫ (другая история → другой seed → residual до ~3% на 610-EMA рвёт union-гейты). Подтвердить что ДЕПЛОЙ-конфиг выбирался на HL-данных |
| 7 | LEVERAGE per-asset | ✅ ЗАКРЫТ 06-10: `update_leverage(coin, eff_lev)` вызывается перед КАЖДЫМ entry (abort при fail), eff_lev=min(LEVERAGE, asset.max_leverage) в sizing+MM-cap (trader.py attempt_entry, risk.py leverage_eff). Остаточное: margin СУЩЕСТВУЮЩИХ позиций в MM-cap моделируется тем же eff_lev нового входа и по entryPx (не mark) — модель, не exchange-факт; чужой xyz notional в кап входит |

## 10. Flip в LIVE — отдельное явное решение юзера (память: NO auto SETTINGS)
1. Gates п.8-9 закрыты (parity + sanity частоты; golden-sim gate №2 снят — формула trail доказана source-read 06-10, но прогон golden-sim всё равно желателен как regression).
2. `DRY_RUN=0` И `XNN_EXPECT_EMPTY_DB=0` (иначе первый же рестарт с собственной открытой позицией = refuse-to-start; guard нужен только на свежем деплое) → `sudo systemctl restart hl-xnn-bot`.
3. Первый вход: `ENTRY OK` + SL oid confirmed live + DB-строка со `sl_order_id` NOT NULL.
4. Re-audit через 24h (journalctl errors + trades-check + UI-сверка).

## 11. Cron-репоинт (bug-fix 2026-06-11 — класс: repoint splits per-dir state)
Бот НЕ регенерит snapshot в рантайме (reloader = только re-read файла, main.py:371-384; регенерация только при старте age>25h; _check_snapshot_age ждёт ВНЕШНИЙ daily cron — :353-368).
- [x] cron ubuntu@hl-bot: `5 0 * * * cd /home/ubuntu/hl_xnn_bot && set -a && . ./.env.xnn && set +a && venv/bin/python -m bot.liquidity_snapshot --force >> data/snapshot_cron.log 2>&1`
- [x] разовый `--force` прогон → real snapshot (spread/depth поля, не stub)
- Правило класса: при репоинте dir перенести ВСЕ cron/timers старой dir (`crontab -l`, `systemctl list-timers`).
