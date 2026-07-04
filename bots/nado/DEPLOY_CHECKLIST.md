# XNN → NADO framework: DRY deploy checklist (2026-06-11)

Источник: локальная копия `/Users/ak/Desktop/HL/xnn_fleet_prep/nado/framework/` (live-копия /root/nado_bot_v2 + xnn-патчи §0). Деплой = **НОВАЯ** директория `/root/nado_xnn_bot` (репоинт-дисциплина: per-dir trades.db/.env — память `feedback_deploy_repoint_splits_per_dir_state`). Образец процесса: HL-канон `/Users/ak/Desktop/HL/hl_xnn_prep/framework/DEPLOY_CHECKLIST.md`.

**СТАРЫЙ бот `nado-bot-v2.service` НЕ останавливать / НЕ disable до явного флипа** (он управляет живыми позициями; на 2026-06-11 trades.db open=0, но правило стоит).

## 0. Патчи этой копии vs live nado_bot_v2 (верифицировать ПОСЛЕ rsync, ДО старта)
| # | Файл | Патч | Статус |
|---|------|------|--------|
| 1 | `bot/xnn_core.py` | NEW — VERBATIM копия HL-канона (md5 == hl_xnn_prep/framework/bot/xnn_core.py, b1d90410…) — математика не правилась | ✅ |
| 2 | `bot/strategy_xnn.py` | NEW — адаптер контракта nado strategy_uk_v102 (Signal/Position/PM/scan_for_signal/scan_for_short_signal/compute_indicators/_estimate_tick); per-TF конфиг ВШИТ (XNN_TF_CONFIG, идентичен канону); kwarg #4 = `zigzag_length` (nado scanner.py:134,157; в HL-каноне `donchian_k`) | ✅ |
| 3 | `bot/scanner.py` | import → strategy_xnn; `SCAN_CANDLES_LIMIT` env вместо hardcode 300 | ✅ |
| 4 | `bot/main.py` | import → strategy_xnn (3 места: top + reconciler + manage-loop); DRY: adopt/restore И per-tick reconciler СКИПАЮТСЯ целиком; startup-ассерт `XNN_EXPECT_EMPTY_DB=1` (open-строки в DB → refuse to start); `DRY_RUN` env поддержан в `__main__` (systemd без флага) | ✅ |
| 5 | `bot/config.py` | NEW поле `dry_run` (env `DRY_RUN`, дефолт false) | ✅ |
| 6 | `bot/trader.py` | import → strategy_xnn; `_dry_block` — DRY=0 ордеров КОДОМ: attempt_entry / _place_sl_with_retry / _ensure_flat / manage_open_position / _emergency_close / _place_tp1_partial / _cancel_tp_if_any | ✅ |
| 7 | `bot/trader.py` | `_sl_live_on_exchange`: read-back exception = ИНДЕТЕРМИНИРОВАННО → assume-live (анти-дубль-SL; exchange-level `_confirm_trigger_live` уже подтвердил placement) | ✅ |
| 8 | `bot/exchange_nado.py` | exception-swallow fix (класс false-naked): `fetch_open_orders_ccxt_shape` query-fail → RAISE (было `[]`); `list_open_sl_orders` fail → RAISE (было `[]` = «нет SL») | ✅ |
| 9 | `bot/universe.py` | `UNIVERSE_SYMBOL_EXCLUDE` env (exact/base match) — foreign-position fencing на флипе | ✅ |
| 10 | resting/Donchian off-флаг | **SKIP** — на nado модуля resting_orders НЕТ, resting-пути нет вообще | — |
| 11 | per-asset max-lev (канон LEVERAGE=100 ceiling) | **SKIP** — `update_leverage` на Nado = NOOP (cross-margin per-subaccount, exchange_nado.py:826-830); LEVERAGE напрямую множит notional-cap (risk.py:44) и делит MM-margin (risk.py:78-81), биржевого биндинга нет → LEVERAGE=5 (live) + TODO в template | — |
| 12 | untracked-protect sweep + FOREIGN_SKIP_PREFIXES | **SKIP** — на nado protect-sweep в main.py ОТСУТСТВУЕТ (нечего гейтить); префиксной foreign-механики нет, замена = UNIVERSE_SYMBOL_EXCLUDE (§6) | — |
| 13 | scanner freshness-guard / candles invalidate (HL §0-11) | **SKIP (принято)** — nado candles() bar-aligned cache + drop-last-if-forming; ⚠️ до 06-11 drop-forming работал ТОЛЬКО на нативном TF — resample-путь 8h (2×4h) пропускал ЧАСТИЧНЫЙ форминг-бакет в scan_signal (рестарт во 2-й половине 8h-бара) — закрыто патчем §0-14 | — |
| 14 | `bot/exchange_nado.py` | AUDIT-FIX 06-11: forming-bucket drop в resample-пути candles() (8h/30m) — тот же boundary-compare что нативный путь; до фикса частичный 8h-бар (1 из 2 4h-сабов) шёл сигнальным баром | ✅ |
| 15 | `bot/main.py` | AUDIT-FIX 06-11: `--dry-run` CLI-флаг форсит ГЛОБАЛЬНЫЙ settings.dry_run (`object.__setattr__`); до фикса флаг гейтил только entry — adopt/reconciler/_dry_block читали env и при .env DRY_RUN=0 слали РЕАЛЬНЫЕ ордера | ✅ |
| 16 | `bot/main.py` | AUDIT-FIX 06-11: DRY-сигналы журналятся `insert_rejected(reason='dry_run_signal')` — §8 reject-профиль/частота меряются из DB (раньше DRY не писал НИЧЕГО — пустая таблица ≠ «нет сигналов») | ✅ |
| 17 | `bot/scanner.py` | AUDIT-FIX 06-11: depth-guards — one-time WARNING per TF если BTC-PERP < 0.9×SCAN_CANDLES_LIMIT (silent truncation = parity-риск); FAIL-LOUD log.error если candles пусты на ВСЕХ монетах TF (индексер down ≠ тихие гейты) | ✅ |
| 18 | `bot/config.py` | AUDIT-FIX 06-11: из FX_EXCLUDE убраны UK-SIG-PAUSE наследия (ONDO/NEAR/BNB) — молча сужали XNN-universe vs канон; coin-паузы теперь только через `UNIVERSE_SYMBOL_EXCLUDE` env | ✅ |

Проверка: `grep -rn "strategy_uk_v102" bot/*.py | grep import` → **0**; `python3 -m py_compile bot/*.py` → OK (обе выполнены 2026-06-11 локально).

## 1. Новая директория на nado VPS
```bash
ssh nado-bot
mkdir -p /root/nado_xnn_bot/logs /root/nado_xnn_bot/data
# с Mac:
rsync -a --exclude '*.bak*' --exclude 'data/' --exclude 'logs/' --exclude 'backups/' \
  --exclude '*.log' --include '.env.xnn.template' --exclude '.env*' \
  /Users/ak/Desktop/HL/xnn_fleet_prep/nado/framework/ root@<nado-bot>:/root/nado_xnn_bot/
# ВАЖНО: --include '.env.xnn.template' ДО --exclude '.env*' (первый матч решает).
cd /root/nado_xnn_bot && python3 -m venv venv
venv/bin/pip install -r requirements.txt   # или pip freeze из /root/nado_bot_v2/venv
```
⚠️ LESSON 06-11 (класс: fresh venv ≠ old venv): свежий py3.12 venv тянет setuptools>=82, где
`pkg_resources` УДАЛЁН → `eth_keyfile` (dep nado-protocol) падает ModuleNotFoundError на старте
(юнит crash-loop 3×). Fix = пин `setuptools<81` в requirements.txt (внесён). Smoke-тест ДО
systemd: `venv/bin/python -c "from nado_protocol.client import NadoClient"`.

## 2. СВЕЖАЯ trades.db — КРИТИЧНО
- `data/` НЕ копировать. `init_db()` создаст пустую `data/trades.db` (DB_PATH = PROJECT_ROOT/data, config.py).
- Старая DB со строками `status='open'` → reconciler/adopt подхватил бы их и СТАЛ БЫ УПРАВЛЯТЬ (re-place SL, market-close). Пустая DB + `XNN_EXPECT_EMPTY_DB=1` (код-ассерт, main.py) = защита и процедурой, и кодом.
- После первого старта: `sqlite3 data/trades.db "select count(*) from trades where status='open'"` → 0.

## 3. .env
```bash
cp bot/.env.xnn.template /root/nado_xnn_bot/.env.xnn
# РУКАМИ (on-VPS shell, НЕ через агент-вывод) из /root/nado_bot_v2/.env перенести ТОЛЬКО:
#   NADO_LINKED_SIGNER_PRIVATE_KEY, NADO_ACCOUNT_ADDRESS  (имена верифицированы live 06-11)
chmod 600 .env.xnn && ln -sf .env.xnn .env
```
- `ln -sf .env.xnn .env` ОБЯЗАТЕЛЕН: config.py:20-21 dotenv-грузит `PROJECT_ROOT/.env` (hardcode имени); cron liquidity_snapshot тоже работает через него.
- Ключевые значения (всё уже в template): `DRY_RUN=1`, `XNN_EXPECT_EMPTY_DB=1`, `RISK_PER_TRADE=0.005`, `LEVERAGE=5` (live-значение; НЕ канон-100 — §0 п.11), `MAX_CONCURRENT=999`, `MM_CAP_PCT=0.50`, `WORKING_TFS=1d,8h,4h`, `SHORT_TFS=4h`, `CRYPTO_SHORT_ONLY=0` (live=true — для XNN ЛОНГОВ обязан быть 0!), `TP1_PARTIAL_FRAC=0` (live=0.5), `MAX_RUN_R=1000` (live=5.0), `VSTOP_BUFFER_PCT=0.15` (live=0.003), `VSTOP_PIVOT_WINDOW=3` (код-дефолт 8), `SCAN_CANDLES_LIMIT=3000`, `ENTRY_LIMIT_CAP_PCT=0.01` (live nado fleet-norm; HL-канон 0.0024 — см. §9), `LIVENESS_MIN_PCT_TRADED=0.05` (код-дефолт 0.95!), `UNIVERSE_MIN_VOL_USD_24H=500000`, `UNIVERSE_SYMBOL_EXCLUDE=` (пусто в DRY; заполняется на флипе §6).

## 4. systemd unit `/etc/systemd/system/nado-xnn-bot.service` (образец: systemd_units.txt)
```ini
[Unit]
Description=Nado XNN Bot (1d+8h+4h long, 4h short — xnn_long_stateless port, DRY)
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=3

[Service]
Type=simple
User=root
WorkingDirectory=/root/nado_xnn_bot
EnvironmentFile=/root/nado_xnn_bot/.env.xnn
ExecStart=/root/nado_xnn_bot/venv/bin/python -m bot.main
Restart=on-failure
RestartSec=15
StandardOutput=journal
StandardError=journal
SyslogIdentifier=nado-xnn-bot
TimeoutStopSec=30
KillMode=process

[Install]
WantedBy=multi-user.target
```
`systemctl daemon-reload && systemctl enable --now nado-xnn-bot`
(EnvironmentFile + dotenv-симлинк = два независимых канала env; DRY_RUN работает в обоих.)

## 5. Старый бот — НЕ ТРОГАТЬ
- `nado-bot-v2.service` остаётся enabled+running до явного решения о флипе (NO auto SETTINGS).
- Его dir `/root/nado_bot_v2` НЕ удалять/НЕ переносить: trades.db = история и маппинг SL.
- Архив (tar, не rm) — на флипе, не сейчас.

## 6. Foreign-safety: ОДИН аккаунт/субаккаунт на двоих — главное отличие от HL
На HL чужие позы имели префикс `xyz_`; на Nado оба бота сидят на subaccount `default` —
позиции old-bot / ручные (`uk_signal_place.py`) выглядят как обычные `BTC-PERP` и Nado
**МЕРДЖИТ** одно-продуктовые позиции внутри субаккаунта.
- DRY: новый бот не тронет чужое КОДОМ — adopt/reconciler выключены в DRY (main.py патч), все ордер-пути за `_dry_block`, fresh DB + `XNN_EXPECT_EMPTY_DB=1`.
- На ФЛИПЕ обязательная процедура:
  1. `sqlite3 /root/nado_bot_v2/data/trades.db "select coin from trades where status='open'"` + live `open_positions()` по субаккаунту → список занятых монет.
  2. Каждую занятую монету → `UNIVERSE_SYMBOL_EXCLUDE=...` нового .env.xnn (код-фенс universe.py: не сканируется, входа нет).
  3. Ручные позиции (gold/BNB/BTC/SOL/JUP — если есть на этом венюе) — туда же.
  4. Если занятых монет 0 (как 06-11) — exclude пустой, но проверка ОБЯЗАТЕЛЬНА (live, не из памяти).
  5. ОПЦИЯ (предпочтительно, отдельное решение юзера): выделенный Nado-субаккаунт для XNN (`NADO_SUBACCOUNT=xnn` + перевод маржи) = полная изоляция margin-пула и позиций; снимает класс целиком.
- Принятый side-effect: чужой notional входит в MM-cap (trader.py `_current_notional` по exchange-positions) — консервативно, ок.

## 7. Верификация после старта (journalctl; память: active≠работает)
```bash
journalctl -u nado-xnn-bot -f
```
- [ ] `nado_bot_v2 starting (dry_run=True, once=False)` + `DRY-RUN mode: no orders will be placed`
- [ ] `Config: tfs=('1d', '8h', '4h')` (leverage=5x mm_cap=50% max_concurrent=999 risk=0.5%)
- [ ] 3× `allow_ungated=True` warning от xnn_core на импорте (msi=1 — ожидаемый маркер, НЕ ошибка)
- [ ] `XNN_EXPECT_EMPTY_DB=1: trades.db has 0 open rows — OK`
- [ ] `DRY_RUN: position adopt/restore SKIPPED`
- [ ] `Universe loaded: N tradeable` — без FX, всё ≥$500k (`venv/bin/python -m bot.universe --print`)
- [ ] Snapshot bootstrap прошёл (`Liquidity snapshot active: …`) — не `FATAL: snapshot still missing`
- [ ] Candle-depth probe (НЕ из памяти): `venv/bin/python -c "from bot.config import settings; from bot.exchange_nado import NadoClient as C; c=C(settings); print({tf: len(c.candles('BTC-PERP', tf, limit=3000)) for tf in ('1d','8h','4h')})"` — если индексер режет глубину < нужной для 610-EMA union → §9-1
- [ ] На закрытии бара: `Scanning N coins tf=...` → сигналы только как `[DRY-RUN] Signal ...`
- [ ] НЕТ `[DRY-RUN] BLOCKED` (появление = позиция/ордер-путь возник в DRY — разбор, это сигнал бага)
- [ ] НЕТ traceback (ImportError/AttributeError по Signal-полям = провал контракта)
- [ ] Старый бот: `systemctl status nado-bot-v2` остался active; его журнал без новых ошибок

## 8. DRY ≥ 2 полных скан-цикла до флипа
- ≥2 закрытия КАЖДОГО TF (4h ≥8ч, 8h ≥16ч, 1d ≥2 суток).
- Sanity сигнал-частоты: union 1d/8h не должен сыпать десятки сигналов/день (сыпет → гейты не работают, СТОП).
- `sqlite3 data/trades.db "select reason,count(*) from rejected_signals group by 1"` — профиль; DRY-сигналы видны как `dry_run_signal` (§0-16); частота/symbol/TF: `select tf,coin,count(*) from rejected_signals where reason='dry_run_signal' group by 1,2`.
- 8h forming-bar чек (§0-14 верификация): ни один `[DRY-RUN] Signal … 8h` не должен иметь bar_ts ≥ текущей 8h-границы (`select * from rejected_signals where tf='8h' and reason='dry_run_signal'` + journalctl bar_age); появление = drop-forming сломан, СТОП.
- **SIGNAL-PARITY до флипа** (память signal_parity_before_strategy_eval): DRY-сигналы сверить с bt-эмиттером на тех же закрытых барах, эмиттер гонять ТОЛЬКО на живом NADO-датасете с тем же стартом окна (Binance-ожидания НЕВАЛИДНЫ — другой seed/история). Расхождение = СТОП.
- 8h-парность отдельно: nado 8h = клиентский resample 2×4h — сверить bar-границы и OHLC агрегацию с bt-8h.

## 9. Открытые вопросы / расхождения с bt и HL-каноном
| # | Вопрос | Статус |
|---|--------|--------|
| 1 | Глубина Nado-индексера: отдаёт ли get_candlesticks 3000 баров на 1d/4h (и 6006×4h для 8h-resample)? Молодой венюе — короткая история сама по себе ок (seed-residual), молча-обрезанный ответ — нет | probe §7 ПЕРВЫМ действием после старта; код-страховка §0-17 (depth WARNING + all-empty FAIL-LOUD) |
| 2 | `ENTRY_LIMIT_CAP_PCT=0.01` (nado fleet-norm) vs HL-канон 0.0024 → шире окно fill vs bt | мерить skip/slip в DRY; решение на флипе |
| 3 | LEVERAGE=5: notional-cap (equity×5) может биндить сайзинг раньше HL-канона (там eff_lev per-asset) | принято для DRY; пересмотр mm_cap-математики = отдельная задача |
| 4 | 8h resample (2×4h client-side) vs bt-нативные 8h бары | §8 parity |
| 5 | Funding: nado funding_rate() = stub 0.0; «slip» на Nado = funding drift (память feedback_nado_slip_is_funding_drift) — в bt не моделится | принято; наблюдать в DRY/post-flip |
| 6 | Cубаккаунт-изоляция XNN (NADO_SUBACCOUNT=xnn) vs UNIVERSE_SYMBOL_EXCLUDE-фенс | решение юзера на флипе |
| 7 | max_concurrent: bt=5 внутренний vs live 999 (как HL-канон, user-rule 06-10) | принято; портфельная траектория ≠ bt |

## 10. Flip в LIVE — отдельное явное решение юзера (NO auto SETTINGS)
1. Gates §8 закрыты (parity + sanity частоты + reject-профиль).
2. §6 процедура foreign-фенса выполнена (live-проверка занятых монет).
3. `DRY_RUN=0` И `XNN_EXPECT_EMPTY_DB=0` (иначе первый рестарт с собственной открытой позицией = refuse-to-start) → `systemctl restart nado-xnn-bot`.
4. Только теперь решается судьба старого бота: `systemctl stop nado-bot-v2 && systemctl disable nado-bot-v2` + tar-архив `/root/nado_bot_v2` (DB в архиве проверить).
5. Первый вход: `ENTRY OK` + SL подтверждён read-back (`_confirm_trigger_live`) + DB-строка `sl_order_id` NOT NULL.
6. Re-audit через 24h (journalctl errors + trades-check + Nado UI-сверка; UI = source of truth).

## 11. Cron-репоинт (класс: repoint splits per-dir state; cron.txt)
Live-кроны старой dir: `5 0 * * *` liquidity_snapshot (cd /root/nado_bot_v2) и `*/5` check_naked sentinel (`nado=/root/nado_bot_v2/data/trades.db`).
- [ ] ДОБАВИТЬ (сразу при DRY-деплое; бот регенерит snapshot только на старте, в рантайме ждёт внешний cron):
      `5 0 * * * cd /root/nado_xnn_bot && ./venv/bin/python -m bot.liquidity_snapshot >> logs/liq_snapshot.log 2>&1`
- [ ] Разовый прогон руками → проверить `data/liquidity_snapshot.json` реальный (spread/depth, не stub).
- [ ] На ФЛИПЕ: к check_naked добавить `nado_xnn=/root/nado_xnn_bot/data/trades.db` (старый аргумент держать, пока old-bot dir жива).
- Правило класса: при репоинте dir перенести ВСЕ cron/timers старой dir (`crontab -l`, `systemctl list-timers`).
