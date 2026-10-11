#!/usr/bin/env python3
"""
Deriv Digit Over / Under bot – Render-ready version
- Runs the trading bot in a background thread
- Exposes a clean web UI on the port required by Render
"""

import json
import os
import time
import threading
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import datetime

from flask import Flask, jsonify, render_template_string
from websocket import (
    create_connection,
    WebSocketTimeoutException,
    WebSocketConnectionClosedException,
)

# ======================= CONFIG =======================
APP_ID = os.environ.get("DERIV_APP_ID")
TOKEN  = os.environ.get("DERIV_TOKEN")

if not APP_ID or not TOKEN:
    raise ValueError("Missing DERIV_APP_ID or DERIV_TOKEN environment variables")

ACCOUNT_TYPE      = "demo"          # "demo" or "real"
TRADING_ENABLED   = True

STAKE             = 1.0
DURATION_TICKS    = 1
COOLDOWN_SECONDS  = 3
MAX_TRADES        = 50
STOP_LOSS         = 3.0
TAKE_PROFIT       = 7.5

OVER_BARRIERS  = [0, 5, 5, 6, 6, 7, 7, 8, 8]
UNDER_BARRIERS = [9, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1]

NO_MATCH_LIMIT   = 5
MODE_CHECK_SECS  = 8

CANDIDATE_SYMBOLS = [
    "R_10", "R_25", "R_50", "R_75", "R_100",
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
]

DIGIT_WINDOW         = 100
MAX_01_FREQUENCY     = 0.10
MAX_89_FREQUENCY     = 0.10
MIN_TICKS_FOR_FILTER = 50
# ======================================================

def clean(s: str) -> str:
    return "".join(c for c in s if 33 <= ord(c) <= 126)

APP_ID = clean(APP_ID)
TOKEN  = clean(TOKEN)

class ConfigError(Exception):
    pass

BASE = "https://api.derivws.com"
UA   = ("Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
        "Chrome/120.0 Mobile Safari/537.36")

def rest(method: str, path: str):
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=b"" if method == "POST" else None,
        headers={
            "Deriv-App-ID": APP_ID,
            "Authorization": "Bearer " + TOKEN.strip(),
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": UA,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:400]
        msg  = f"HTTP {e.code} on {path}: {body}"
        if e.code in (400, 401, 403):
            raise ConfigError(msg + "\nCheck APP_ID / TOKEN (trade scope required).")
        raise RuntimeError(msg)

def find_list(obj):
    if isinstance(obj, list) and obj:
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            r = find_list(v)
            if r:
                return r
    return []

def find_wss(obj):
    if isinstance(obj, str) and obj.startswith("wss://"):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            r = find_wss(v)
            if r:
                return r
    if isinstance(obj, list):
        for v in obj:
            r = find_wss(v)
            if r:
                return r
    return None

def get_ws_url() -> str:
    data = rest("GET", "/trading/v1/options/accounts")
    accounts = [a for a in find_list(data) if isinstance(a, dict)]
    if not accounts:
        raise RuntimeError("No accounts found: " + json.dumps(data)[:300])

    chosen = accounts[0]
    for a in accounts:
        if ACCOUNT_TYPE in json.dumps(a).lower():
            chosen = a
            break

    account_id = None
    for key in ("account_id", "accountId", "id", "loginid"):
        if chosen.get(key):
            account_id = chosen[key]
            break
    if not account_id:
        raise RuntimeError("Could not find account id: " + json.dumps(chosen)[:300])

    print("Using account:", account_id)
    otp = rest("POST", f"/trading/v1/options/accounts/{account_id}/otp")
    url = find_wss(otp)
    if not url:
        raise RuntimeError("No WebSocket URL in OTP response: " + json.dumps(otp)[:300])
    return url

def last_digit(price) -> int:
    s = f"{float(price):.12f}".rstrip("0").rstrip(".")
    return int(s[-1])

class Bot:
    def __init__(self):
        self.mode            = "over"
        self.barrier_idx     = 0
        self.trades          = 0
        self.pending         = False
        self.last_trade_ts   = 0.0
        self.balance         = None
        self.start_balance   = None
        self.currency        = "USD"
        self.stop            = False
        self.open_contract   = None
        self.digit_history   = defaultdict(lambda: deque(maxlen=DIGIT_WINDOW))
        self.ticks_seen      = 0
        self.last_status     = {}
        self.no_match_streak = 0
        self.last_mode_check = 0.0
        self.last_message    = "Bot starting..."
        self.running         = False
        self.started_at      = None

    def barriers(self):
        return OVER_BARRIERS if self.mode == "over" else UNDER_BARRIERS

    def current_barrier(self) -> int:
        return self.barriers()[self.barrier_idx]

    def contract_type(self) -> str:
        return "DIGITOVER" if self.mode == "over" else "DIGITUNDER"

    def switch_mode(self):
        old = self.mode
        self.mode = "under" if self.mode == "over" else "over"
        self.barrier_idx = 0
        self.no_match_streak = 0
        msg = f"SWITCHED STRATEGY: {old.upper()} → {self.mode.upper()}"
        print(f"\n*** {msg} ***")
        self.last_message = msg

    def on_win(self):
        first = self.barriers()[0]
        print(f"  → WIN – prediction reset to {first}")
        self.barrier_idx = 0
        self.last_message = f"WIN – reset to barrier {first}"

    def on_loss(self):
        blist = self.barriers()
        if self.barrier_idx < len(blist) - 1:
            self.barrier_idx += 1
            print(f"  → LOSS – next prediction = {self.current_barrier()}")
            self.last_message = f"LOSS – next barrier {self.current_barrier()}"
        else:
            print(f"  → LOSS – end of list, reset to {blist[0]}")
            self.barrier_idx = 0
            self.last_message = f"LOSS – reset to {blist[0]}"

    def digit_freq(self, symbol: str, digits) -> float | None:
        hist = self.digit_history[symbol]
        if len(hist) < MIN_TICKS_FOR_FILTER:
            return None
        count = sum(1 for d in hist if d in digits)
        return count / len(hist)

    def is_eligible(self, symbol: str) -> bool:
        if self.mode == "over":
            freq = self.digit_freq(symbol, (0, 1))
            return freq is not None and freq < MAX_01_FREQUENCY
        else:
            freq = self.digit_freq(symbol, (8, 9))
            return freq is not None and freq < MAX_89_FREQUENCY

    def any_eligible(self) -> bool:
        return any(self.is_eligible(s) for s in CANDIDATE_SYMBOLS)

    def can_trade(self, symbol: str) -> bool:
        if self.pending or self.open_contract:
            return False
        if time.time() - self.last_trade_ts < COOLDOWN_SECONDS:
            return False
        if self.trades >= MAX_TRADES:
            print("Max trades reached – stopping.")
            self.stop = True
            return False
        return self.is_eligible(symbol)

    def try_trade(self, ws, symbol: str, price: float):
        if not self.can_trade(symbol):
            return

        barrier = str(self.current_barrier())
        ctype   = self.contract_type()

        if self.mode == "over":
            freq = self.digit_freq(symbol, (0, 1))
            label = f"P(0+1)={freq*100:.1f}%"
        else:
            freq = self.digit_freq(symbol, (8, 9))
            label = f"P(8+9)={freq*100:.1f}%"

        print(f"\n★ ELIGIBLE {symbol}  {label}  → {ctype} barrier={barrier}")
        self.last_message = f"Trading {symbol} {ctype} barrier={barrier}"

        if not TRADING_ENABLED:
            return

        self.pending = True
        ws.send(json.dumps({
            "proposal": 1,
            "amount": STAKE,
            "basis": "stake",
            "contract_type": ctype,
            "currency": self.currency,
            "duration": DURATION_TICKS,
            "duration_unit": "t",
            "underlying_symbol": symbol,
            "barrier": barrier,
        }))

    def maybe_switch_mode(self):
        now = time.time()
        if now - self.last_mode_check < MODE_CHECK_SECS:
            return
        self.last_mode_check = now

        if self.any_eligible():
            self.no_match_streak = 0
            return

        self.no_match_streak += 1
        print(f"  No eligible market for {self.mode.upper()} "
              f"(streak {self.no_match_streak}/{NO_MATCH_LIMIT})")
        if self.no_match_streak >= NO_MATCH_LIMIT:
            self.switch_mode()

    def handle(self, ws, msg: dict):
        if "error" in msg:
            err = msg["error"]
            print("API error:", err.get("message") or err)
            self.last_message = f"API error: {err.get('message') or err}"
            self.pending = False
            return

        t = msg.get("msg_type")

        if t == "tick" or "tick" in msg:
            tick = msg.get("tick") or msg
            symbol = (tick.get("symbol") or
                      tick.get("underlying_symbol") or
                      tick.get("s") or "?")
            try:
                price = float(tick["quote"])
            except (KeyError, TypeError, ValueError):
                return

            digit = last_digit(price)
            self.digit_history[symbol].append(digit)
            self.ticks_seen += 1
            self.maybe_switch_mode()

            eligible = self.is_eligible(symbol)
            if self.mode == "over":
                freq = self.digit_freq(symbol, (0, 1))
                freq_str = (f"P(0+1)={freq*100:5.1f}%" if freq is not None else "collecting")
            else:
                freq = self.digit_freq(symbol, (8, 9))
                freq_str = (f"P(8+9)={freq*100:5.1f}%" if freq is not None else "collecting")

            status_key = (symbol, eligible, self.mode,
                          round(freq or -1, 3) if freq is not None else None)
            if (status_key != self.last_status.get(symbol) or
                    self.ticks_seen % 40 == 0):
                flag = "✓ READY" if eligible else "  skip "
                print(f"[{self.mode.upper():5}] {symbol}: {price} d={digit}  "
                      f"{freq_str}  {flag}  barrier={self.current_barrier()}")
                self.last_status[symbol] = status_key

            self.try_trade(ws, symbol, price)

        elif t == "balance" or "balance" in msg:
            b = msg.get("balance")
            if isinstance(b, dict):
                amount = b.get("balance", b.get("amount"))
                self.currency = b.get("currency", self.currency)
            else:
                amount = b
            try:
                self.balance = float(amount)
            except (TypeError, ValueError):
                return

            if self.start_balance is None:
                self.start_balance = self.balance
                print(f"Starting balance: {self.balance:.2f} {self.currency}")

            profit = self.balance - self.start_balance
            print(f"BALANCE: {self.balance:.2f} {self.currency}  (P/L {profit:+.2f})")

            if TRADING_ENABLED:
                if profit <= -STOP_LOSS:
                    print(f"Stop-loss ${STOP_LOSS} hit – stopping.")
                    self.stop = True
                    self.last_message = f"Stop-loss hit (${STOP_LOSS})"
                elif profit >= TAKE_PROFIT:
                    print(f"Take-profit ${TAKE_PROFIT} hit – stopping.")
                    self.stop = True
                    self.last_message = f"Take-profit hit (${TAKE_PROFIT})"

        elif t == "proposal":
            prop = msg.get("proposal") or {}
            prop_id = prop.get("id")
            if not prop_id:
                print("Proposal missing id:", json.dumps(msg)[:250])
                self.pending = False
                return
            print(f"  Proposal OK – buying id={prop_id}")
            ws.send(json.dumps({"buy": prop_id, "price": STAKE}))

        elif t == "buy":
            buy = msg.get("buy") or {}
            contract_id = buy.get("contract_id")
            self.trades += 1
            self.pending = False
            self.last_trade_ts = time.time()
            self.open_contract = contract_id
            print(f"Trade #{self.trades} placed  ({self.mode.upper()})  "
                  f"contract_id={contract_id}  barrier={self.current_barrier()}")
            self.last_message = f"Trade #{self.trades} placed ({self.mode.upper()})"

            if contract_id:
                ws.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 1,
                }))

        elif t == "proposal_open_contract":
            poc = msg.get("proposal_open_contract") or {}
            if not (poc.get("is_sold") or poc.get("status") == "sold"):
                return

            profit = float(poc.get("profit", 0) or 0)
            cid = poc.get("contract_id")
            print(f"Contract {cid} closed  profit={profit:+.2f}")

            if profit > 0:
                self.on_win()
            else:
                self.on_loss()

            self.open_contract = None

        elif t in ("ping", "pong", "authorize", "time"):
            pass
        else:
            if getattr(self, "_unknown", 0) < 3:
                print("Other msg:", json.dumps(msg)[:200])
                self._unknown = getattr(self, "_unknown", 0) + 1

def run_session(bot: Bot):
    url = get_ws_url()
    ws  = create_connection(url, timeout=15, header=["User-Agent: " + UA])
    ws.settimeout(10)
    print("Connected.")
    bot.last_message = "Connected to Deriv"

    ws.send(json.dumps({"balance": 1, "subscribe": 1}))

    for sym in CANDIDATE_SYMBOLS:
        ws.send(json.dumps({"ticks": sym, "subscribe": 1}))
        print(f"  subscribed: {sym}")

    print(f"\nMode: {ACCOUNT_TYPE} | trading={'ON' if TRADING_ENABLED else 'OFF'}")
    print(f"Starting strategy: OVER (Over 0)")
    print(f"Stop-loss ${STOP_LOSS} | Take-profit ${TAKE_PROFIT}\n")

    last_ping    = time.time()
    last_balance = time.time()

    try:
        while not bot.stop:
            now = time.time()
            if now - last_ping > 25:
                ws.send(json.dumps({"ping": 1}))
                last_ping = now
            if now - last_balance > 45:
                ws.send(json.dumps({"balance": 1}))
                last_balance = now

            try:
                raw = ws.recv()
            except WebSocketTimeoutException:
                continue
            if raw:
                bot.handle(ws, json.loads(raw))
    finally:
        try:
            ws.close()
        except Exception:
            pass

def bot_runner(bot: Bot):
    """Background thread that keeps the trading bot alive."""
    bot.running = True
    bot.started_at = datetime.utcnow().isoformat() + "Z"
    delay = 3

    while not bot.stop:
        try:
            run_session(bot)
            delay = 3
        except ConfigError as e:
            print("Login problem:", e)
            bot.last_message = f"Login problem: {e}"
            bot.stop = True
            return
        except (WebSocketConnectionClosedException, ConnectionError, OSError) as e:
            print("Connection lost:", e)
            bot.last_message = f"Connection lost: {e}"
        except Exception as e:
            print("Error:", e)
            bot.last_message = f"Error: {e}"

        if bot.stop:
            break
        print(f"Reconnecting in {delay}s …\n")
        time.sleep(delay)
        delay = min(delay * 2, 60)

    bot.running = False
    print("Bot finished.")
    bot.last_message = "Bot stopped"

# ======================= Flask App =======================
app = Flask(__name__)
bot_instance = Bot()
bot_thread = None

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Deriv Digit Bot</title>
  <style>
    :root {
      --bg: #0f172a;
      --card: #1e293b;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --green: #22c55e;
      --red: #ef4444;
      --yellow: #eab308;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: system-ui, -apple-system, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      padding: 24px;
    }
    .container { max-width: 720px; margin: 0 auto; }
    h1 { font-size: 1.75rem; margin-bottom: 8px; }
    .subtitle { color: var(--muted); margin-bottom: 28px; }
    .card {
      background: var(--card);
      border-radius: 12px;
      padding: 20px 24px;
      margin-bottom: 16px;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }
    @media (max-width: 560px) { .grid { grid-template-columns: 1fr; } }
    .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .value { font-size: 1.25rem; font-weight: 600; margin-top: 4px; }
    .status-dot {
      display: inline-block;
      width: 10px; height: 10px;
      border-radius: 50%;
      margin-right: 8px;
    }
    .online { background: var(--green); }
    .offline { background: var(--red); }
    .btn {
      display: inline-block;
      padding: 10px 20px;
      border-radius: 8px;
      border: none;
      font-size: 0.95rem;
      font-weight: 600;
      cursor: pointer;
      margin-right: 10px;
      margin-top: 8px;
      text-decoration: none;
      color: white;
    }
    .btn-start { background: var(--green); }
    .btn-stop  { background: var(--red); }
    .btn-refresh { background: #334155; }
    .msg {
      margin-top: 16px;
      padding: 12px 16px;
      background: #0f172a;
      border-radius: 8px;
      font-family: ui-monospace, monospace;
      font-size: 0.9rem;
      color: var(--accent);
    }
    footer { margin-top: 32px; color: var(--muted); font-size: 0.8rem; text-align: center; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Deriv Digit Over / Under</h1>
    <p class="subtitle">Live status of your trading bot</p>

    <div class="card">
      <div style="margin-bottom: 16px;">
        <span class="status-dot {{ 'online' if running else 'offline' }}"></span>
        <strong>{{ 'Running' if running else 'Stopped' }}</strong>
      </div>

      <div class="grid">
        <div>
          <div class="label">Mode</div>
          <div class="value">{{ mode|upper }}</div>
        </div>
        <div>
          <div class="label">Barrier</div>
          <div class="value">{{ barrier }}</div>
        </div>
        <div>
          <div class="label">Trades</div>
          <div class="value">{{ trades }}</div>
        </div>
        <div>
          <div class="label">Ticks seen</div>
          <div class="value">{{ ticks }}</div>
        </div>
        <div>
          <div class="label">Balance</div>
          <div class="value">
            {% if balance is not none %}
              {{ '%.2f'|format(balance) }} {{ currency }}
            {% else %}
              —
            {% endif %}
          </div>
        </div>
        <div>
          <div class="label">P/L</div>
          <div class="value" style="color: {{ '#22c55e' if profit and profit >= 0 else '#ef4444' }}">
            {% if profit is not none %}
              {{ '%+.2f'|format(profit) }}
            {% else %}
              —
            {% endif %}
          </div>
        </div>
      </div>

      <div class="msg">{{ last_message }}</div>

      <div style="margin-top: 20px;">
        <a class="btn btn-start" href="/start">Start Bot</a>
        <a class="btn btn-stop"  href="/stop">Stop Bot</a>
        <a class="btn btn-refresh" href="/">Refresh</a>
      </div>
    </div>

    <footer>
      Started: {{ started_at or '—' }} · Account: {{ account_type }}
    </footer>
  </div>
</body>
</html>
"""

@app.route("/")
def index():
    b = bot_instance
    profit = None
    if b.balance is not None and b.start_balance is not None:
        profit = b.balance - b.start_balance
    return render_template_string(
        HTML,
        running=b.running,
        mode=b.mode,
        barrier=b.current_barrier(),
        trades=b.trades,
        ticks=b.ticks_seen,
        balance=b.balance,
        currency=b.currency,
        profit=profit,
        last_message=b.last_message,
        started_at=b.started_at,
        account_type=ACCOUNT_TYPE,
    )

@app.route("/start")
def start_bot():
    global bot_thread
    if bot_instance.running:
        return jsonify({"ok": False, "msg": "Bot already running"}), 400
    bot_instance.stop = False
    bot_thread = threading.Thread(target=bot_runner, args=(bot_instance,), daemon=True)
    bot_thread.start()
    return jsonify({"ok": True, "msg": "Bot started"})

@app.route("/stop")
def stop_bot():
    bot_instance.stop = True
    return jsonify({"ok": True, "msg": "Stop signal sent"})

@app.route("/status")
def status():
    b = bot_instance
    profit = None
    if b.balance is not None and b.start_balance is not None:
        profit = b.balance - b.start_balance
    return jsonify({
        "running": b.running,
        "mode": b.mode,
        "barrier": b.current_barrier(),
        "trades": b.trades,
        "ticks": b.ticks_seen,
        "balance": b.balance,
        "currency": b.currency,
        "profit": profit,
        "last_message": b.last_message,
        "started_at": b.started_at,
    })

if __name__ == "__main__":
    # Local development only; Render uses gunicorn under9:app
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
