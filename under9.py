#!/usr/bin/env python3
"""
Deriv Digit Over / Under bot – New Deriv API (App ID + pat_ token + OTP)

Two strategies
--------------
1. OVER  (default) – “Over 0 strategy”
   • DIGITOVER
   • Barriers on loss : 0 → 5 → 5 → 6 → 6 → 7 → 7 → 8 → 8
   • Filter           : recent frequency of digits 0 and 1 < 10 %
   • On WIN           : reset barrier to 0 and continue

2. UNDER (activated after 5 consecutive “no-match” scans)
   • DIGITUNDER
   • Barriers on loss : 9 → 5 → 5 → 4 → 4 → 3 → 3 → 2 → 2 → 1 → 1
   • Filter           : recent frequency of digits 8 and 9 < 10 %
   • On WIN           : reset barrier to 9 and continue

Common settings
---------------
• Stake $1, duration 1 tick
• Stop-loss $3, take-profit $7.50
• Scans all listed volatility indices
• Switches back and forth after 5 failed scans each

Install:  pip install websocket-client
"""

import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque

from websocket import (
    create_connection,
    WebSocketTimeoutException,
    WebSocketConnectionClosedException,
)

# ======================= CONFIG =======================
APP_ID = os.environ.get("DERIV_APP_ID", "34CtuujwUQPcjQL4fFDBO")
TOKEN  = os.environ.get("DERIV_TOKEN",  "‎pat_27473f73804a377d93a852cacba6f08614eae6289b32b853cf9c0b65716b6c2b")

ACCOUNT_TYPE      = "demo"          # "demo" or "real"
TRADING_ENABLED   = True

STAKE             = 1.0
DURATION_TICKS    = 1
COOLDOWN_SECONDS  = 3
MAX_TRADES        = 50
STOP_LOSS         = 3.0
TAKE_PROFIT       = 7.5

# ----- Strategy definitions -----
OVER_BARRIERS  = [0, 5, 5, 6, 6, 7, 7, 8, 8]
UNDER_BARRIERS = [9, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1]

# How many consecutive “no eligible market” checks before switching strategy
NO_MATCH_LIMIT   = 5
MODE_CHECK_SECS  = 8            # check eligibility every N seconds

CANDIDATE_SYMBOLS = [
    "R_10", "R_25", "R_50", "R_75", "R_100",
    "1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V",
]

DIGIT_WINDOW         = 100
# Over filter: frequency of digits 0 and 1 combined
MAX_01_FREQUENCY     = 0.10
# Under filter: frequency of digits 8 and 9 combined
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
        self.mode            = "over"          # "over" or "under"
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

    # ---------- mode helpers ----------
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
        print(f"\n*** SWITCHED STRATEGY: {old.upper()} → {self.mode.upper()} ***")
        print(f"    New barriers: {self.barriers()}")
        print(f"    Contract    : {self.contract_type()}\n")

    def on_win(self):
        first = self.barriers()[0]
        print(f"  → WIN – prediction reset to {first}  (continue)")
        self.barrier_idx = 0

    def on_loss(self):
        blist = self.barriers()
        if self.barrier_idx < len(blist) - 1:
            self.barrier_idx += 1
            print(f"  → LOSS – next prediction = {self.current_barrier()}")
        else:
            print(f"  → LOSS – end of list, reset to {blist[0]}")
            self.barrier_idx = 0

    # ---------- frequency filters ----------
    def digit_freq(self, symbol: str, digits) -> float | None:
        """Return frequency of the given digits in the recent window."""
        hist = self.digit_history[symbol]
        if len(hist) < MIN_TICKS_FOR_FILTER:
            return None
        count = sum(1 for d in hist if d in digits)
        return count / len(hist)

    def is_eligible(self, symbol: str) -> bool:
        if self.mode == "over":
            # Over 0 strategy: frequency of 0 and 1 combined must be < 10 %
            freq = self.digit_freq(symbol, (0, 1))
            return freq is not None and freq < MAX_01_FREQUENCY
        else:
            # Under 9 strategy: frequency of 8 and 9 combined must be < 10 %
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
            label = f"P(0 and 1)={freq*100:.1f}%"
        else:
            freq = self.digit_freq(symbol, (8, 9))
            label = f"P(8 and 9)={freq*100:.1f}%"

        print(f"\n★ ELIGIBLE {symbol}  {label}  "
              f"→ {ctype} barrier={barrier} @ {price}")

        if not TRADING_ENABLED:
            print("  [signals only] TRADING_ENABLED=False")
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
        """Called periodically. After NO_MATCH_LIMIT consecutive failures → switch."""
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
            self.pending = False
            return

        t = msg.get("msg_type")

        # ---- tick ----
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

            # periodic mode-switch check
            self.maybe_switch_mode()

            # status logging
            eligible = self.is_eligible(symbol)
            if self.mode == "over":
                freq = self.digit_freq(symbol, (0, 1))
                freq_str = (f"P(0 and 1)={freq*100:5.1f}%"
                            if freq is not None else "collecting")
            else:
                freq = self.digit_freq(symbol, (8, 9))
                freq_str = (f"P(8 and 9)={freq*100:5.1f}%"
                            if freq is not None else "collecting")

            status_key = (symbol, eligible, self.mode,
                          round(freq or -1, 3) if freq is not None else None)
            if (status_key != self.last_status.get(symbol) or
                    self.ticks_seen % 40 == 0):
                flag = "✓ READY" if eligible else "  skip "
                print(f"[{self.mode.upper():5}] {symbol}: {price} d={digit}  "
                      f"{freq_str}  {flag}  barrier={self.current_barrier()}")
                self.last_status[symbol] = status_key

            self.try_trade(ws, symbol, price)

        # ---- balance ----
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
                elif profit >= TAKE_PROFIT:
                    print(f"Take-profit ${TAKE_PROFIT} hit – stopping.")
                    self.stop = True

        # ---- proposal → buy ----
        elif t == "proposal":
            prop = msg.get("proposal") or {}
            prop_id = prop.get("id")
            if not prop_id:
                print("Proposal missing id:", json.dumps(msg)[:250])
                self.pending = False
                return
            print(f"  Proposal OK – buying id={prop_id}")
            ws.send(json.dumps({
                "buy": prop_id,
                "price": STAKE,
            }))

        # ---- buy confirmation ----
        elif t == "buy":
            buy = msg.get("buy") or {}
            contract_id = buy.get("contract_id")
            self.trades += 1
            self.pending = False
            self.last_trade_ts = time.time()
            self.open_contract = contract_id
            print(f"Trade #{self.trades} placed  ({self.mode.upper()})  "
                  f"contract_id={contract_id}  barrier={self.current_barrier()}")

            if contract_id:
                ws.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": contract_id,
                    "subscribe": 1,
                }))

        # ---- contract result ----
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

    ws.send(json.dumps({"balance": 1, "subscribe": 1}))
#!/usr/bin/env python3
"""
Deriv Digit Over / Under bot – New Deriv API (App ID + pat_ token + OTP)

Two strategies
--------------
1. OVER  (default) – “Over 0 strategy”
   • DIGITOVER
   • Barriers on loss : 0 → 5 → 5 → 6 → 6 → 7 → 7 → 8 → 8
   • Filter           : recent frequency of digits 0 and 1 < 10 %
   • On WIN           : reset barrier to 0 and continue

2. UNDER (activated after 5 consecutive “no-match” scans)
   • DIGITUNDER
   • Barriers on loss : 9 → 5 → 5 → 4 → 4 → 3 → 3 → 2 → 2 → 1 → 1
   • Filter           : recent frequency of digits 8 and 9 < 10 %
   • On WIN           : reset barrier to 9 and continue

Common settings
---------------
• Stake $1, duration 1 tick
• Stop-loss $3, take-profit $7.50
• Scans all listed volatility indices
• Switches back and forth after 5 failed scans each

Install:  pip install websocket-client
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque

from websocket import (
    create_connection,
    WebSocketTimeoutException,
    WebSocketConnectionClosedException,
)

# ======================= CONFIG =======================
# Credentials come from the environment only – never commit them.
#   export DERIV_APP_ID=...   export DERIV_TOKEN=pat_...
APP_ID = os.environ.get("DERIV_APP_ID", "")
TOKEN  = os.environ.get("DERIV_TOKEN", "")

