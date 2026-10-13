#!/usr/bin/env python3
"""
Deriv Digit Over / Under bot – New Deriv API (App ID + pat_ token + OTP)

Two strategies, scanned at the same time
-----------------------------------------
Every tick of every market is checked against BOTH strategies. The first
market that passes either filter is traded (if both pass on the same market,
the one with the lower frequency wins). One trade is open at a time.

1. OVER  – “Over 0 strategy”
   • DIGITOVER
   • Barriers on loss : 0 → 5 → 5 → 6 → 6 → 7 → 7 → 8 → 8
   • Filter           : recent frequency of digits 0 and 1 < 10 %
   • On WIN           : reset this strategy's barrier to 0

2. UNDER – “Under 9 strategy”
   • DIGITUNDER
   • Barriers on loss : 9 → 5 → 5 → 4 → 4 → 3 → 3 → 2 → 2 → 1 → 1
   • Filter           : recent frequency of digits 8 and 9 < 10 %
   • On WIN           : reset this strategy's barrier to 9

Each strategy keeps its own barrier ladder; a result only moves the ladder of
the strategy that placed the trade.

Common settings
---------------
• Stake $1, duration 1 tick
• Stop-loss $3, take-profit $7.50
• Scans all listed volatility indices

Install:  pip install websocket-client
"""

from __future__ import annotations

import hmac
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

ACCOUNT_TYPE      = "demo"          # "demo" or "real"
TRADING_ENABLED   = True

STAKE             = 1.0
DURATION_TICKS    = 1
COOLDOWN_SECONDS  = 3
MAX_TRADES        = 50
STOP_LOSS         = 3.0
TAKE_PROFIT       = 7.5

# After stop-loss or take-profit, automatically start a new session
# (resets session P/L counters and barrier ladders). Set to False to
# keep the old behaviour of staying stopped until the process restarts.
AUTO_RESTART_AFTER_SL_TP = True
RESTART_DELAY_SECS       = 45   # pause after SL/TP before the new session starts

# ----- Strategy definitions -----
OVER_BARRIERS  = [0, 5, 5, 6, 6, 7, 7, 8, 8]
UNDER_BARRIERS = [9, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1]

PENDING_TIMEOUT = 20           # give up on a trade request Deriv never answered (seconds)
OPEN_CHECK_SECS  = 30           # re-check a contract that has not settled after this long

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

# Both strategies are active at once.
STRATEGIES = {
    "over":  {"label": "Over 0",  "kind": "Over",  "contract": "DIGITOVER",
              "barriers": OVER_BARRIERS,  "digits": (0, 1), "max_freq": MAX_01_FREQUENCY},
    "under": {"label": "Under 9", "kind": "Under", "contract": "DIGITUNDER",
              "barriers": UNDER_BARRIERS, "digits": (8, 9), "max_freq": MAX_89_FREQUENCY},
}
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

# The account the bot is trading on (shown on the dashboard).
ACCOUNT = {"id": None, "type": ACCOUNT_TYPE}

def account_id_of(a: dict):
    for key in ("account_id", "accountId", "id", "loginid"):
        if a.get(key):
            return str(a[key])
    return None

def account_kind(a: dict):
    """Return "demo", "real" or None when it cannot be told.

    Looks at explicit fields first, then at the account id prefix
    (VRTC / DOT = demo, CR / ROT / MF / MLT / MX = real). It never searches
    the whole record for the word "demo", because a real account can carry a
    field such as "demo_account": false."""
    virtual = a.get("is_virtual")
    if virtual in (1, True, "1", "true"):
        return "demo"
    if virtual in (0, False, "0", "false"):
        return "real"
    for key in ("account_type", "accountType", "type"):
        v = a.get(key)
        if isinstance(v, str):
            lv = v.strip().lower()
            if lv in ("demo", "virtual"):
                return "demo"
            if lv in ("real", "live"):
                return "real"
    ident = (account_id_of(a) or "").upper()
    if ident.startswith(("VR", "DOT", "DEMO")):
        return "demo"
    if ident.startswith(("CR", "ROT", "MF", "MLT", "MX")):
        return "real"
    return None

def get_ws_url() -> str:
    data = rest("GET", "/trading/v1/options/accounts")
    accounts = [a for a in find_list(data) if isinstance(a, dict)]
    if not accounts:
        raise RuntimeError("No accounts found: " + json.dumps(data)[:300])

    # Never fall back to a different account type (e.g. real instead of demo).
    chosen = next((a for a in accounts if account_kind(a) == ACCOUNT_TYPE), None)
    if chosen is None:
        found = ", ".join(f"{account_id_of(a) or '?'}={account_kind(a) or 'unknown'}"
                          for a in accounts)
        raise ConfigError(f"No '{ACCOUNT_TYPE}' account found (accounts: {found})")

    account_id = account_id_of(chosen)
    if not account_id:
        raise RuntimeError("Could not find account id: " + json.dumps(chosen)[:300])

    ACCOUNT.update(id=account_id, type=ACCOUNT_TYPE)
    print("Using account:", account_id, f"({ACCOUNT_TYPE})")
    otp = rest("POST", f"/trading/v1/options/accounts/{account_id}/otp")
    url = find_wss(otp)
    if not url:
        raise RuntimeError("No WebSocket URL in OTP response: " + json.dumps(otp)[:300])
    return url

def decimals_needed(price) -> int:
    """Fewest decimals that represent this price exactly."""
    s = f"{float(price):.8f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0

class Bot:
    def __init__(self):
        self.idx             = {"over": 0, "under": 0}   # barrier ladder position per strategy
        self.open_strategy   = None            # strategy that placed the open trade
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
        self.decimals       = {}              # symbol -> quote precision
        self.connected       = False
        self.last_error      = None
        # ---- dashboard data ----
        self.lock            = threading.RLock()     # bot thread vs web thread
        self.events          = deque(maxlen=100)     # newest first
        self.trade_log       = deque(maxlen=500)     # oldest first
        self.balance_history = deque(maxlen=300)     # [timestamp, balance]
        self.last_price      = {}                    # symbol -> latest quote
        self.pending_trade   = None                  # trade waiting for its buy confirmation
        self.pending_since   = 0.0
        self.open_since      = 0.0
        self.stop_reason     = None
        self.restart_at      = None                  # when AUTO_RESTART: resume after this time
        self.session_count   = 0                     # how many sessions started this process

    # ---------- dashboard helpers ----------
    def note(self, level: str, msg: str):
        """Add a line to the event list shown on the dashboard."""
        with self.lock:
            self.events.appendleft({"level": level, "t": round(time.time(), 1),
                                    "msg": str(msg)[:300]})

    def request_stop(self, reason: str, msg: str | None = None, level: str = "info"):
        """Stop the bot. The first reason wins.
        For stop_loss / take_profit, optionally schedule an auto-restart."""
        with self.lock:
            if self.stop:
                return
            self.stop = True
            self.stop_reason = reason
            if msg:
                print(msg)
                self.note(level, msg)
            if (AUTO_RESTART_AFTER_SL_TP
                    and reason in ("stop_loss", "take_profit")):
                self.restart_at = time.time() + RESTART_DELAY_SECS
                self.note("info",
                          f"Auto-restart in {RESTART_DELAY_SECS}s "
                          f"(new session after {reason.replace('_', '-')}).")

    def reset_session(self):
        """Clear session P/L, barrier ladders and counters so a new run can start.
        Keeps digit history, connection state and the trade log for the dashboard."""
        with self.lock:
            self.idx = {"over": 0, "under": 0}
            self.open_strategy = None
            self.trades = 0
            self.pending = False
            self.pending_trade = None
            self.pending_since = 0.0
            self.open_contract = None
            self.open_since = 0.0
            self.last_trade_ts = 0.0
            self.start_balance = self.balance   # next session P/L starts from current balance
            self.stop = False
            self.stop_reason = None
            self.restart_at = None
            self.session_count += 1
            self.note("info", f"Session #{self.session_count} started "
                              f"(P/L counters and barrier ladders reset).")
            print(f"\n=== Session #{self.session_count} started "
                  f"(balance baseline {self.balance}) ===\n")

    def snapshot(self, running: bool, protected: bool) -> dict:
        """Everything the dashboard shows, as plain JSON-safe data."""
        with self.lock:
            closed = [r for r in self.trade_log if r["status"] != "open"]
            wins   = sum(1 for r in closed if r["status"] == "won")
            losses = len(closed) - wins
            net    = sum(r["profit"] or 0.0 for r in closed)
            session_pl = (round(self.balance - self.start_balance, 2)
                          if self.balance is not None and self.start_balance is not None
                          else None)

            scanner = []
            for sym in CANDIDATE_SYMBOLS:
                hist = list(self.digit_history.get(sym, ()))
                f01  = self.digit_freq(sym, (0, 1))
                f89  = self.digit_freq(sym, (8, 9))
                scanner.append({
                    "symbol": sym,
                    "digits": hist[-30:],
                    "ticks": len(hist),
                    "price": self.last_price.get(sym),
                    "decimals": self.decimals.get(sym),
                    "f01": f01, "f89": f89,
                    "collecting": f01 is None,
                    "ready_over": self.is_eligible(sym, "over"),
                    "ready_under": self.is_eligible(sym, "under"),
                })

            strategies = [{
                "key": key, "label": s["label"], "kind": s["kind"],
                "contract_type": s["contract"], "barriers": list(s["barriers"]),
                "barrier_idx": self.idx[key], "barrier": self.current_barrier(key),
                "digits": list(s["digits"]), "max_freq": s["max_freq"],
            } for key, s in STRATEGIES.items()]

            trades = [{
                "n": r["n"], "symbol": r["symbol"], "contract_type": r["contract_type"],
                "barrier": r["barrier"], "stake": r["stake"], "status": r["status"],
                "profit": None if r["profit"] is None else round(r["profit"], 2),
                "t_open": r["t_open"],
            } for r in reversed(self.trade_log)][:100]

            restart_in = None
            if self.restart_at and self.stop:
                restart_in = max(0, round(self.restart_at - time.time()))
            return {
                "bot_running": running,
                "connected": self.connected,
                "stopped": self.stop,
                "stop_reason": self.stop_reason,
                "restart_in": restart_in,
                "auto_restart": AUTO_RESTART_AFTER_SL_TP,
                "session_count": self.session_count,
                "last_error": self.last_error,
                "protected": protected,
                "trading_enabled": TRADING_ENABLED,
                "account": dict(ACCOUNT),
                "currency": self.currency,
                "balance": self.balance,
                "session_pl": session_pl,
                "balance_history": list(self.balance_history)[-120:],
                "limits": {"max_trades": MAX_TRADES, "stop_loss": STOP_LOSS,
                           "take_profit": TAKE_PROFIT, "stake": STAKE},
                "stats": {"trades": self.trades, "wins": wins, "losses": losses,
                          "win_rate": (wins / len(closed)) if closed else None,
                          "net": round(net, 2)},
                "trades": trades,
                "strategies": strategies,
                "filter": {"window": DIGIT_WINDOW, "min_ticks": MIN_TICKS_FOR_FILTER},
                "scanner": scanner,
                "events": list(self.events),
            }

    def tick_digit(self, symbol: str, tick: dict, price: float) -> int:
        """Last digit of the quote *including* trailing zeros (1234.50 -> 0).

        Uses pip_size from the tick when present; otherwise learns each
        symbol's precision as the most decimals seen so far."""
        dec = self.decimals.get(symbol, 0)
        pip = tick.get("pip_size")
        if isinstance(pip, int) and pip >= 0:
            dec = max(dec, pip)
        else:
            dec = max(dec, decimals_needed(price))
        self.decimals[symbol] = dec
        return int(f"{price:.{dec}f}"[-1])

    # ---------- strategy helpers ----------
    def barriers(self, key: str):
        return STRATEGIES[key]["barriers"]

    def current_barrier(self, key: str) -> int:
        return self.barriers(key)[self.idx[key]]

    def on_win(self, key: str):
        s = STRATEGIES[key]
        print(f"  → WIN ({s['label']}) – barrier reset to {s['barriers'][0]}")
        self.idx[key] = 0

    def on_loss(self, key: str):
        blist = self.barriers(key)
        label = STRATEGIES[key]["label"]
        if self.idx[key] < len(blist) - 1:
            self.idx[key] += 1
            print(f"  → LOSS ({label}) – next barrier = {self.current_barrier(key)}")
        else:
            print(f"  → LOSS ({label}) – end of list, reset to {blist[0]}")
            self.idx[key] = 0

    # ---------- frequency filters ----------
    def digit_freq(self, symbol: str, digits) -> float | None:
        """Return frequency of the given digits in the recent window."""
        hist = self.digit_history[symbol]
        if len(hist) < MIN_TICKS_FOR_FILTER:
            return None
        count = sum(1 for d in hist if d in digits)
        return count / len(hist)

    def strategy_freq(self, symbol: str, key: str) -> float | None:
        return self.digit_freq(symbol, STRATEGIES[key]["digits"])

    def is_eligible(self, symbol: str, key: str) -> bool:
        """Over: digits 0 and 1 combined < 10 %.  Under: digits 8 and 9 combined < 10 %."""
        freq = self.strategy_freq(symbol, key)
        return freq is not None and freq < STRATEGIES[key]["max_freq"]

    def pick_strategy(self, symbol: str) -> str | None:
        """Both strategies are checked at once. If both pass on this market,
        take the one furthest below its limit."""
        best, best_score = None, None
        for key, s in STRATEGIES.items():
            freq = self.strategy_freq(symbol, key)
            if freq is None or freq >= s["max_freq"]:
                continue
            score = freq / s["max_freq"]
            if best_score is None or score < best_score:
                best, best_score = key, score
        return best

    def can_trade(self, symbol: str) -> str | None:
        """Return the strategy to trade on this market now, or None."""
        if self.stop or self.pending or self.open_contract:
            return None
        if time.time() - self.last_trade_ts < COOLDOWN_SECONDS:
            return None
        if self.trades >= MAX_TRADES:
            self.request_stop("max_trades",
                              f"Trade limit of {MAX_TRADES} reached – stopping.")
            return None
        return self.pick_strategy(symbol)

    def try_trade(self, ws, symbol: str, price: float):
        key = self.can_trade(symbol)
        if key is None:
            return

        s       = STRATEGIES[key]
        barrier = str(self.current_barrier(key))
        ctype   = s["contract"]
        freq    = self.strategy_freq(symbol, key)
        label   = f"P({s['digits'][0]} and {s['digits'][1]})={freq*100:.1f}%"

        print(f"\n★ ELIGIBLE {symbol}  {label}  "
              f"→ {s['label']}: {ctype} barrier={barrier} @ {price}")

        if not TRADING_ENABLED:
            print("  [signals only] TRADING_ENABLED=False")
            return

        self.pending = True
        self.pending_since = time.time()
        self.pending_trade = {"symbol": symbol, "contract_type": ctype,
                              "barrier": int(barrier), "strategy": key}
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

    def handle(self, ws, msg: dict):
        with self.lock:
            self._handle(ws, msg)

    def watchdog(self, ws):
        """Unstick the bot if Deriv never answers a request or a result is missed."""
        now = time.time()
        with self.lock:
            if self.pending and now - self.pending_since > PENDING_TIMEOUT:
                self.pending = False
                self.pending_trade = None
                self.note("warn", "Deriv did not answer a trade request in time, trying again")
            cid = self.open_contract
            if cid and now - self.open_since > OPEN_CHECK_SECS:
                self.open_since = now
                self.note("warn", "Trade result is late, asking Deriv for it")
                ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": cid}))

    def _handle(self, ws, msg: dict):
        if "error" in msg:
            err = msg["error"]
            text = (err.get("message") if isinstance(err, dict) else None) or str(err)
            print("API error:", text)
            self.note("error", "Deriv rejected a request: " + text)
            self.pending = False
            self.pending_trade = None
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

            digit = self.tick_digit(symbol, tick, price)
            self.digit_history[symbol].append(digit)
            self.last_price[symbol] = price
            self.ticks_seen += 1

            # status logging: both strategies are scanned on every tick
            f01 = self.digit_freq(symbol, (0, 1))
            f89 = self.digit_freq(symbol, (8, 9))
            over_ok  = self.is_eligible(symbol, "over")
            under_ok = self.is_eligible(symbol, "under")

            status_key = (symbol, over_ok, under_ok)
            if (status_key != self.last_status.get(symbol) or
                    self.ticks_seen % 40 == 0):
                if f01 is None:
                    freq_str = "collecting"
                else:
                    freq_str = f"P(0,1)={f01*100:5.1f}%  P(8,9)={f89*100:5.1f}%"
                flag = " ".join(name for name, ok in (("OVER ✓", over_ok),
                                                      ("UNDER ✓", under_ok)) if ok) or "skip"
                print(f"[SCAN] {symbol}: {price} d={digit}  {freq_str}  {flag}  "
                      f"(Over {self.current_barrier('over')}, "
                      f"Under {self.current_barrier('under')})")
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

            if (not self.balance_history
                    or self.balance_history[-1][1] != self.balance):
                self.balance_history.append([round(time.time(), 1), self.balance])

            if TRADING_ENABLED:
                if profit <= -STOP_LOSS:
                    self.request_stop("stop_loss",
                                      f"Stop-loss ${STOP_LOSS:.2f} reached – stopping.",
                                      "warn")
                elif profit >= TAKE_PROFIT:
                    self.request_stop("take_profit",
                                      f"Take-profit ${TAKE_PROFIT:.2f} reached – stopping.")

        # ---- proposal → buy ----
        elif t == "proposal":
            if not self.pending:
                return          # late answer to a request that already timed out
            prop = msg.get("proposal") or {}
            prop_id = prop.get("id")
            if not prop_id:
                print("Proposal missing id:", json.dumps(msg)[:250])
                self.note("error", "Deriv sent a proposal without an id, trade skipped")
                self.pending = False
                self.pending_trade = None
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
            self.open_since = self.last_trade_ts
            self.open_contract = contract_id

            pt = self.pending_trade or {}
            self.pending_trade = None
            strategy = pt.get("strategy") or "over"
            self.open_strategy = strategy
            print(f"Trade #{self.trades} placed  ({STRATEGIES[strategy]['label']})  "
                  f"contract_id={contract_id}  barrier={pt.get('barrier')}")
            try:
                stake = float(buy.get("buy_price") or STAKE)
            except (TypeError, ValueError):
                stake = STAKE
            rec = {
                "n": self.trades, "contract_id": contract_id,
                "symbol": pt.get("symbol", "?"),
                "contract_type": pt.get("contract_type", STRATEGIES[strategy]["contract"]),
                "barrier": pt.get("barrier", self.current_barrier(strategy)),
                "strategy": strategy,
                "stake": stake, "status": "open", "profit": None,
                "t_open": time.time(), "t_close": None,
            }
            self.trade_log.append(rec)
            kind = "Over" if rec["contract_type"] == "DIGITOVER" else "Under"
            self.note("info", f"Trade #{rec['n']} placed: {rec['symbol']} {kind} {rec['barrier']}")

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

            try:
                profit = float(poc.get("profit", 0) or 0)
            except (TypeError, ValueError):
                profit = 0.0
            cid = poc.get("contract_id")
            rec = next((r for r in reversed(self.trade_log)
                        if r["contract_id"] == cid), None)
            if rec is not None and rec["status"] != "open":
                return          # repeat update for a trade already settled
            print(f"Contract {cid} closed  profit={profit:+.2f}")

            if rec is not None:
                rec["status"]  = "won" if profit > 0 else "lost"
                rec["profit"]  = profit
                rec["t_close"] = time.time()
                self.note("info", f"Trade #{rec['n']} {rec['status']}: {profit:+.2f}")

            # Only the strategy that placed this trade moves its own barrier ladder.
            key = (rec or {}).get("strategy") or self.open_strategy
            if key in STRATEGIES:
                if profit > 0:
                    self.on_win(key)
                else:
                    self.on_loss(key)

            self.open_contract = None
            self.open_strategy = None

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
    bot.connected = True
    bot.note("info", f"Connected to Deriv ({ACCOUNT['type']} account {ACCOUNT['id']})")
    with bot.lock:
        bot.pending = False            # an unanswered proposal died with the old connection
        bot.pending_trade = None
        stuck = bot.open_contract

    ws.send(json.dumps({"balance": 1, "subscribe": 1}))
    if stuck:
        # A trade was open when the connection dropped: ask for its result,
        # otherwise the bot would wait for it forever and never trade again.
        bot.note("warn", "Reconnected while a trade was open, checking its result")
        ws.send(json.dumps({"proposal_open_contract": 1,
                            "contract_id": stuck, "subscribe": 1}))

    for sym in CANDIDATE_SYMBOLS:
        ws.send(json.dumps({"ticks": sym, "subscribe": 1}))
        print(f"  subscribed: {sym}")

    print(f"\nMode: {ACCOUNT_TYPE} | trading={'ON' if TRADING_ENABLED else 'OFF'}")
    print("Strategies: OVER (Over 0) and UNDER (Under 9), scanned at the same time")
    print(f"  OVER  barriers: {OVER_BARRIERS}")
    print(f"         filter : P(0 and 1) < {MAX_01_FREQUENCY*100:.0f} %")
    print(f"  UNDER barriers: {UNDER_BARRIERS}")
    print(f"         filter : P(8 and 9) < {MAX_89_FREQUENCY*100:.0f} %")
    print(f"Stop-loss ${STOP_LOSS} | Take-profit ${TAKE_PROFIT}\n")

    last_ping    = time.time()
    last_balance = time.time()

    stop_seen = None
    try:
        while True:
            now = time.time()
            if bot.stop:
                # Let an open trade finish so its result is recorded.
                stop_seen = stop_seen or now
                if not bot.open_contract or now - stop_seen > 20:
                    break
            if now - last_ping > 25:
                ws.send(json.dumps({"ping": 1}))
                last_ping = now
            if now - last_balance > 45:
                ws.send(json.dumps({"balance": 1}))
                last_balance = now
            bot.watchdog(ws)

            try:
                raw = ws.recv()
            except WebSocketTimeoutException:
                continue
            if raw:
                bot.handle(ws, json.loads(raw))
    finally:
        bot.connected = False
        try:
            ws.close()
        except Exception:
            pass

def run_forever(bot: Bot):
    """Run sessions with auto-reconnect. On SL/TP, optionally restart a new session."""
    delay = 3

    while True:
        # Wait out a scheduled auto-restart (SL/TP) then reset session counters.
        if bot.stop and bot.restart_at and AUTO_RESTART_AFTER_SL_TP:
            remaining = bot.restart_at - time.time()
            if remaining > 0:
                print(f"Waiting {remaining:.0f}s before auto-restart …")
                time.sleep(min(remaining, 5))
                continue
            bot.reset_session()
            delay = 3
            continue

        if bot.stop:
            break

        try:
            run_session(bot)
            delay = 3
        except KeyboardInterrupt:
            bot.request_stop("user", "Stopped by user.")
            return
        except ConfigError as e:
            bot.last_error = f"Login problem: {e}"
            print(bot.last_error)
            bot.note("error", bot.last_error)
            bot.request_stop("login")
            return
        except (WebSocketConnectionClosedException, ConnectionError, OSError) as e:
            bot.last_error = f"Connection lost: {e}"
            print(bot.last_error)
            bot.note("warn", bot.last_error)
        except Exception as e:
            bot.last_error = f"Error: {e}"
            print(bot.last_error)
            bot.note("error", bot.last_error)

        if bot.stop and not (bot.restart_at and AUTO_RESTART_AFTER_SL_TP):
            break
        if bot.stop:
            continue
        print(f"Reconnecting in {delay}s …\n")
        time.sleep(delay)
        delay = min(delay * 2, 60)

    print("Bot finished.")

def main():
    if not APP_ID or not TOKEN:
        print("Set DERIV_APP_ID and DERIV_TOKEN environment variables first.")
        return
    run_forever(Bot())

# ================= Web wrapper (Render / gunicorn) =================
# Start command:  gunicorn under9:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120
# Keep --workers 1: each worker would run its own copy of the bot.
# Endpoints:  /           dashboard page (dashboard.html)
#             /health     tiny public check, for uptime pings
#             /api/state  everything the dashboard shows (also at /status);
#                         when STATUS_KEY is set it needs that key, sent as the
#                         X-Status-Key header or ?key=
# Running `python3 under9.py` directly does not need Flask.
DASHBOARD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "dashboard.html")
BOT         = Bot()
_thread     = None
_owner_pid  = None            # process that owns _thread (a forked copy does not)
_lock       = threading.Lock()
_keepalive  = None

# Render's free tier sleeps a service after ~15 minutes without incoming web
# traffic. Render sets RENDER_EXTERNAL_URL; the bot pings its own /health so it
# keeps trading. Set KEEPALIVE=0 to turn this off (an uptime monitor also works).
KEEPALIVE_SECS = 600

def bot_running() -> bool:
    """True while the bot thread is alive and either trading or waiting to auto-restart."""
    if not (_thread and _thread.is_alive() and _owner_pid == os.getpid()):
        return False
    if BOT.stop and not (BOT.restart_at and AUTO_RESTART_AFTER_SL_TP):
        return False
    return True

def _keepalive_loop(url: str):
    while True:
        time.sleep(KEEPALIVE_SECS)
        try:
            with urllib.request.urlopen(url + "/health", timeout=20) as r:
                r.read()
        except Exception:
            pass                      # a missed ping is harmless

def start_background_bot() -> bool:
    global _thread, _owner_pid, _keepalive
    with _lock:
        mine = _owner_pid == os.getpid()
        if _thread and _thread.is_alive() and mine:
            return False
        if not APP_ID or not TOKEN:
            msg = "Missing DERIV_APP_ID or DERIV_TOKEN"
            if BOT.last_error != msg:         # report once, not on every request
                BOT.last_error = msg
                print(msg)
                BOT.note("error", msg)
            return False
        if _thread is not None and mine:
            BOT.note("warn", "The bot thread had stopped, starting it again")
        # Fresh process / crashed thread: clear any leftover permanent stop.
        # If we are mid auto-restart window, reset_session will run inside run_forever.
        if BOT.stop and not (BOT.restart_at and AUTO_RESTART_AFTER_SL_TP):
            BOT.stop = False
            BOT.stop_reason = None
            BOT.restart_at = None
        BOT.last_error = None
        _thread = threading.Thread(target=run_forever, args=(BOT,),
                                   daemon=True, name="deriv-bot")
        _owner_pid = os.getpid()
        _thread.start()

        url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        if url and os.environ.get("KEEPALIVE", "1") != "0" and not (
                _keepalive and _keepalive.is_alive()):
            _keepalive = threading.Thread(target=_keepalive_loop, args=(url,),
                                          daemon=True, name="keepalive")
            _keepalive.start()
        return True

def ensure_bot() -> None:
    """Cheap check run on every web request: start the bot in THIS process if
    it is not running (first request, crashed thread, or a forked gunicorn
    worker). A bot stopped for SL/TP with auto-restart scheduled is left alone
    — the existing thread is waiting out RESTART_DELAY_SECS. Permanent stops
    (login, max trades, user) stay stopped."""
    if os.environ.get("DERIV_AUTOSTART", "1") != "1":
        return
    # Thread still alive (including waiting for auto-restart) — nothing to do.
    if _thread and _thread.is_alive() and _owner_pid == os.getpid():
        return
    # Permanent stop and the thread has already exited — do not resurrect.
    if BOT.stop and not (BOT.restart_at and AUTO_RESTART_AFTER_SL_TP):
        return
    start_background_bot()

# gunicorn --preload (or any os.fork) would copy the bot into the worker while
# the original keeps trading in the parent. Stop the parent's bot at fork time
# and start a fresh one in the child, so exactly one bot trades.
def _before_fork():
    _lock.acquire()
    BOT.lock.acquire()
    if _thread and _thread.is_alive() and _owner_pid == os.getpid():
        BOT.stop = True
        BOT.stop_reason = "forked"

def _after_fork_parent():
    BOT.lock.release()
    _lock.release()

def _after_fork_child():
    global _thread, _owner_pid, _lock, _keepalive
    _lock = threading.Lock()
    BOT.lock = threading.RLock()
    if BOT.stop_reason == "forked":       # only undo the stop caused by the fork itself
        BOT.stop = False
        BOT.stop_reason = None
    BOT.connected = False
    BOT.pending = False
    BOT.pending_trade = None
    _thread = None
    _owner_pid = None
    _keepalive = None
    t = threading.Timer(2.0, start_background_bot)
    t.daemon = True
    t.start()

if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before_fork,
                        after_in_parent=_after_fork_parent,
                        after_in_child=_after_fork_child)

try:
    from flask import Flask, Response, jsonify, request
except ImportError:      # plain local run without Flask
    app = None
else:
    app = Flask(__name__)

    def key_ok() -> bool:
        key = os.environ.get("STATUS_KEY", "")
        if not key:
            return True
        given = request.headers.get("X-Status-Key") or request.args.get("key") or ""
        return hmac.compare_digest(given.encode(), key.encode())

    @app.before_request
    def _keep_bot_alive():
        ensure_bot()

    @app.after_request
    def security_headers(resp):
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    @app.route("/")
    def index():
        try:
            with open(DASHBOARD_FILE, encoding="utf-8") as f:
                html = f.read()
        except OSError:
            return jsonify(ok=True, bot_running=bot_running(),
                           note="dashboard.html is missing from the repository")
        resp = Response(html, mimetype="text/html")
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    @app.route("/health")
    def health():
        return jsonify(ok=True, bot_running=bot_running())

    @app.route("/api/state")
    @app.route("/status")
    def api_state():
        if not key_ok():
            return jsonify(error="forbidden"), 403
        resp = jsonify(BOT.snapshot(bot_running(), bool(os.environ.get("STATUS_KEY"))))
        resp.headers["Cache-Control"] = "no-store"
        return resp

    # Under gunicorn the module is imported, not run as a script.
    if __name__ != "__main__" and os.environ.get("DERIV_AUTOSTART", "1") == "1":
        start_background_bot()

if __name__ == "__main__":
    main()