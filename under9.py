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

ACCOUNT_TYPE      = os.environ.get("ACCOUNT_TYPE", "demo").strip().lower() or "demo"  # "demo" or "real"
TRADING_ENABLED   = True

STAKE             = 1.0
DURATION_TICKS    = 1
COOLDOWN_SECONDS  = 3
MAX_TRADES        = 50
TRADE_LIMIT_REFRESH_SECS = 300   # auto-reset trade counter every 5 minutes
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
# After "Unknown contract proposal" (or similar buy failures) wait this long
# before trying again so the dashboard is not flooded with errors.
BUY_ERROR_COOLDOWN = 8

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

def log(msg: str) -> None:
    """Print with flush so gunicorn/Render shows bot lines immediately."""
    print(msg, flush=True)

BASE = "https://api.derivws.com"
UA   = ("Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
        "Chrome/120.0 Mobile Safari/537.36")

def rest(method: str, path: str, timeout: float = 15.0):
    """Call Deriv REST API. Raises ConfigError for auth problems."""
    if not APP_ID or not TOKEN:
        raise ConfigError("Missing DERIV_APP_ID or DERIV_TOKEN environment variables")
    log(f"REST {method} {path} (app_id={APP_ID[:4]}… token={TOKEN[:6]}…)")
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
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            data = json.loads(raw) if raw else {}
            log(f"REST {method} {path} -> ok ({len(raw)} bytes)")
            return data
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:400]
        msg  = f"HTTP {e.code} on {path}: {body}"
        log(msg)
        if e.code in (400, 401, 403):
            raise ConfigError(msg + " — check DERIV_APP_ID / DERIV_TOKEN (trade scope required).")
        raise RuntimeError(msg)
    except Exception as e:
        log(f"REST {method} {path} failed: {type(e).__name__}: {e}")
        raise RuntimeError(f"REST {method} {path} failed: {e}")

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

def _bot_note(level: str, msg: str) -> None:
    """Best-effort dashboard note during login (BOT may not exist yet)."""
    try:
        BOT.note(level, msg)
    except Exception:
        log(f"{level}: {msg}")


def get_ws_url() -> str:
    _bot_note("info", f"Fetching accounts (type={ACCOUNT_TYPE})…")
    data = rest("GET", "/trading/v1/options/accounts")
    accounts = [a for a in find_list(data) if isinstance(a, dict)]
    if not accounts:
        raise RuntimeError("No accounts found: " + json.dumps(data)[:300])

    # Never fall back to a different account type (e.g. real instead of demo).
    chosen = next((a for a in accounts if account_kind(a) == ACCOUNT_TYPE), None)
    if chosen is None:
        found = ", ".join(f"{account_id_of(a) or '?'}={account_kind(a) or 'unknown'}"
                          for a in accounts)
        raise ConfigError(
            f"No '{ACCOUNT_TYPE}' account found (accounts: {found}). "
            f"Set ACCOUNT_TYPE in under9.py to match your token, or use a {ACCOUNT_TYPE} token."
        )

    account_id = account_id_of(chosen)
    if not account_id:
        raise RuntimeError("Could not find account id: " + json.dumps(chosen)[:300])

    ACCOUNT.update(id=account_id, type=ACCOUNT_TYPE)
    log(f"Using account: {account_id} ({ACCOUNT_TYPE})")
    _bot_note("info", f"Using account {account_id} ({ACCOUNT_TYPE})")
    _bot_note("info", "Requesting WebSocket OTP…")
    otp = rest("POST", f"/trading/v1/options/accounts/{account_id}/otp")
    url = find_wss(otp)
    if not url:
        raise RuntimeError("No WebSocket URL in OTP response: " + json.dumps(otp)[:300])
    _bot_note("info", "OTP received — opening WebSocket")
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
        self.paused          = False                 # dashboard Pause: skip new entries
        self.reconnect_requested = False
        self.last_connect_attempt = 0.0
        self.thread_started_at = 0.0
        self._ws             = None
        self.manual_symbol   = None                  # optional one-shot force symbol
        self.manual_strategy = None                  # "over" | "under" | None (auto)
        self.manual_skip_filter = False              # manual trade: ignore digit filter once
        self.trades_window_started = time.time()     # when current trade-limit window began
        self.limit_refresh_count = 0                 # how many times limit was refreshed

    # ---------- dashboard helpers ----------

    def ensure_attrs(self):
        """Fill any attributes added in newer deploys (avoids AttributeError on hot reload)."""
        defaults = {
            "manual_symbol": None,
            "manual_strategy": None,
            "manual_skip_filter": False,
            "trades_window_started": time.time(),
            "limit_refresh_count": 0,
            "paused": False,
            "reconnect_requested": False,
            "last_connect_attempt": 0.0,
            "thread_started_at": 0.0,
            "_ws": None,
            "pending_trade": None,
            "pending_since": 0.0,
            "open_since": 0.0,
            "stop_reason": None,
            "restart_at": None,
            "session_count": 0,
            "last_error": None,
            "currency": "USD",
        }
        for k, v in defaults.items():
            if not hasattr(self, k):
                setattr(self, k, v)

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
            self.paused = False
            if msg:
                print(msg)
                self.note(level, msg)
            if (AUTO_RESTART_AFTER_SL_TP
                    and reason in ("stop_loss", "take_profit")):
                self.restart_at = time.time() + RESTART_DELAY_SECS
                self.note("info",
                          f"Auto-restart in {RESTART_DELAY_SECS}s "
                          f"(new session after {reason.replace('_', '-')}).")

    def set_paused(self, paused: bool):
        with self.lock:
            if self.stop:
                return False
            self.paused = bool(paused)
            self.note("info", "Paused – no new trades." if self.paused
                              else "Resumed – scanning for entries.")
            return True

    def force_next(self, symbol: str | None = None, strategy: str | None = None,
                   skip_filter: bool = False):
        """Clear cooldown and optionally prefer a symbol/strategy for the next entry.
        skip_filter=True (manual trade) places the next contract even if digit filter fails."""
        with self.lock:
            self.last_trade_ts = 0.0
            self.manual_symbol = symbol or None
            self.manual_strategy = strategy if strategy in STRATEGIES else None
            self.manual_skip_filter = bool(skip_filter)
            parts = []
            if symbol: parts.append(str(symbol))
            if strategy: parts.append(str(strategy))
            if skip_filter: parts.append("skip-filter")
            self.note("info", "Force trigger – cooldown cleared"
                      + (f" ({', '.join(parts)})" if parts else "."))
            return True


    def refresh_trade_limit(self, reason: str = "manual") -> dict:
        """Reset the session trade counter (MAX_TRADES window) without wiping history."""
        with self.lock:
            prev = self.trades
            self.trades = 0
            self.trades_window_started = time.time()
            self.limit_refresh_count = getattr(self, "limit_refresh_count", 0) + 1
            self._limit_warned = False
            cleared_stop = False
            if self.stop and self.stop_reason == "max_trades":
                self.stop = False
                self.stop_reason = None
                self.restart_at = None
                cleared_stop = True
            self.note(
                "info",
                f"Trade limit refreshed ({reason}): {prev}/{MAX_TRADES} → 0/{MAX_TRADES}"
                + (" · trading resumed" if cleared_stop else ""),
            )
            return {
                "ok": True,
                "message": f"Trade limit reset (was {prev}/{MAX_TRADES})",
                "previous": prev,
                "max_trades": MAX_TRADES,
                "resumed": cleared_stop,
            }

    def maybe_auto_refresh_trade_limit(self) -> None:
        """If the 5-minute window elapsed, reset the trade counter automatically."""
        started = getattr(self, "trades_window_started", None) or 0.0
        if started <= 0:
            with self.lock:
                self.trades_window_started = time.time()
            return
        if time.time() - started < TRADE_LIMIT_REFRESH_SECS:
            return
        self.refresh_trade_limit(reason="auto-5min")

    def control(self, action: str, **kwargs) -> dict:
        """Dashboard control actions. Returns {ok, message}."""
        self.ensure_attrs()
        action = (action or "").strip().lower()
        if action == "pause":
            ok = self.set_paused(True)
            return {"ok": ok, "message": "Paused" if ok else "Cannot pause while stopped"}
        if action == "resume":
            if self.stop and self.restart_at:
                # Cancel scheduled wait and restart session now
                self.reset_session()
                return {"ok": True, "message": "Session restarted"}
            if self.stop:
                self.stop = False
                self.stop_reason = None
                self.restart_at = None
                self.paused = False
                self.note("info", "Resumed after stop.")
                return {"ok": True, "message": "Resumed"}
            ok = self.set_paused(False)
            return {"ok": ok, "message": "Resumed"}
        if action == "stop":
            self.request_stop("user", "Stopped from dashboard.")
            return {"ok": True, "message": "Stopped"}
        if action in ("refresh_limit", "reset_trades", "refresh_trades"):
            return self.refresh_trade_limit(reason="dashboard")
        if action == "reset_stats":
            with self.lock:
                self.trade_log.clear()
                self.trades = 0
                self.trades_window_started = time.time()
                if self.balance is not None:
                    self.start_balance = self.balance
                self.note("info", "Stats reset from dashboard.")
            return {"ok": True, "message": "Stats reset"}
        if action == "force":
            skip = bool(kwargs.get("skip_filter") or kwargs.get("skipFilter"))
            self.force_next(kwargs.get("symbol"), kwargs.get("strategy"), skip_filter=skip)
            if self.paused:
                self.set_paused(False)
            return {"ok": True, "message": "Manual trade armed" if skip else "Force trigger armed"}
        if action == "manual":
            self.force_next(kwargs.get("symbol"), kwargs.get("strategy"), skip_filter=True)
            if self.paused:
                self.set_paused(False)
            return {"ok": True, "message": "Manual trade armed"}
        if action in ("reconnect", "restart_bot"):
            self.reconnect_requested = True
            self.last_error = None
            # Clear permanent stop so run_forever can start a new session
            if self.stop and self.stop_reason in ("login", "user", "max_trades", None):
                self.stop = False
                self.stop_reason = None
                self.restart_at = None
            self.note("info", "Reconnect requested from dashboard")
            ws = self._ws
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
            return {"ok": True, "message": "Reconnecting to Deriv…"}
        if action == "reset_session":
            self.reset_session()
            return {"ok": True, "message": "Session reset"}
        return {"ok": False, "message": f"Unknown action: {action}"}

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
        self.ensure_attrs()
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
                "offline_secs": (None if self.connected else (
                    round(time.time() - self.last_connect_attempt)
                    if self.last_connect_attempt else (
                        round(time.time() - self.thread_started_at)
                        if self.thread_started_at else None))),
                "stopped": self.stop,
                "paused": self.paused,
                "force_armed": bool(getattr(self, "manual_symbol", None)
                                     or getattr(self, "manual_strategy", None)
                                     or getattr(self, "manual_skip_filter", False)),
                "force": {
                    "symbol": getattr(self, "manual_symbol", None),
                    "strategy": getattr(self, "manual_strategy", None),
                    "skip_filter": bool(getattr(self, "manual_skip_filter", False)),
                },
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
                "limits": {
                    "max_trades": MAX_TRADES,
                    "stop_loss": STOP_LOSS,
                    "take_profit": TAKE_PROFIT,
                    "stake": STAKE,
                    "refresh_secs": TRADE_LIMIT_REFRESH_SECS,
                    "refresh_in": max(0, int(TRADE_LIMIT_REFRESH_SECS - (time.time() - (getattr(self, "trades_window_started", time.time()) or time.time())))),
                    "limit_refresh_count": getattr(self, "limit_refresh_count", 0),
                },
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
        self.ensure_attrs()
        if self.stop or self.paused or self.pending or self.open_contract:
            return None
        if self.manual_symbol and symbol != self.manual_symbol:
            return None
        if time.time() - self.last_trade_ts < COOLDOWN_SECONDS:
            return None
        # Auto-refresh trade window every TRADE_LIMIT_REFRESH_SECS (default 5 min)
        self.maybe_auto_refresh_trade_limit()
        if self.trades >= MAX_TRADES:
            # Soft block – do not kill the bot thread; wait for refresh button / auto timer
            if not getattr(self, "_limit_warned", False):
                self.note("warn", f"Trade limit {MAX_TRADES} reached – waiting for refresh (auto every {TRADE_LIMIT_REFRESH_SECS // 60} min)")
                self._limit_warned = True
            return None
        if self.manual_strategy:
            key = self.manual_strategy
            if getattr(self, 'manual_skip_filter', False) or self.is_eligible(symbol, key):
                return key
            return None
        if self.manual_symbol:
            key = self.pick_strategy(symbol)
            if key:
                return key
            if getattr(self, 'manual_skip_filter', False):
                return "over"
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
        self.manual_symbol = None
        self.manual_strategy = None
        self.manual_skip_filter = False
        # Buy with parameters directly (buy: 1). Avoids the short-lived
        # proposal-id race that causes "Unknown contract proposal" on 1-tick
        # digit contracts.
        ws.send(json.dumps({
            "buy": 1,
            "price": STAKE,
            "parameters": {
                "amount": STAKE,
                "basis": "stake",
                "contract_type": ctype,
                "currency": self.currency,
                "duration": DURATION_TICKS,
                "duration_unit": "t",
                "underlying_symbol": symbol,
                "barrier": barrier,
            },
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
            # Slow down after buy/proposal failures so we do not spam the API
            # and flood the Events list with the same error every 2 seconds.
            low = text.lower()
            if "unknown contract proposal" in low or "proposal" in low or "buy" in low:
                self.last_trade_ts = time.time() + (BUY_ERROR_COOLDOWN - COOLDOWN_SECONDS)
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

        # ---- legacy proposal reply (we now buy with parameters directly) ----
        elif t == "proposal":
            # Kept only for safety if a stale proposal reply arrives.
            # Direct-buy flow does not depend on proposal IDs.
            prop = msg.get("proposal") or {}
            prop_id = prop.get("id")
            if prop_id and self.pending:
                print(f"  (legacy) Proposal id={prop_id} – ignored; using direct buy")
            return

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
    bot.note("info", "Connecting to Deriv…")
    bot.last_connect_attempt = time.time()
    bot.connected = False
    ws = None
    if not APP_ID or not TOKEN:
        bot.last_error = "Missing DERIV_APP_ID or DERIV_TOKEN"
        bot.note("error", bot.last_error)
        raise ConfigError(bot.last_error)
    bot.note("info", f"Credentials present (app_id={APP_ID[:4]}…, token={TOKEN[:6]}…)")
    log("run_session: resolving account + OTP")
    try:
        url = get_ws_url()
    except Exception as e:
        bot.last_error = f"Login / account: {e}"
        bot.note("error", bot.last_error[:300])
        raise
    bot.note("info", f"Opening WebSocket (account {ACCOUNT.get('id') or '?'})")
    try:
        ws = create_connection(url, timeout=20, header=["User-Agent: " + UA])
    except Exception as e:
        bot.last_error = f"WebSocket connect failed: {e}"
        bot.note("error", bot.last_error)
        raise
    ws.settimeout(10)
    bot._ws = ws
    bot.reconnect_requested = False
    print("Connected.")
    bot.connected = True
    bot.last_error = None
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
            bot.maybe_auto_refresh_trade_limit()
            if bot.reconnect_requested:
                bot.note("info", "Closing socket for reconnect")
                break
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
        bot._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

def run_forever(bot: Bot):
    """Run sessions with auto-reconnect. On SL/TP, optionally restart a new session."""
    delay = 3
    bot.thread_started_at = time.time()
    log("Bot thread entered run_forever")
    bot.note("info", "Bot thread started")

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

        if bot.reconnect_requested:
            bot.reconnect_requested = False
            bot.connected = False
            bot.note("info", "Reconnecting now…")
            delay = 1
            time.sleep(0.5)
            continue

        if bot.stop and not (bot.restart_at and AUTO_RESTART_AFTER_SL_TP):
            break
        if bot.stop:
            continue
        bot.note("warn", f"Reconnecting in {delay}s…")
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
                log(msg)
                BOT.note("error", msg)
            return False
        log(f"Starting bot thread (APP_ID set={bool(APP_ID)}, TOKEN set={bool(TOKEN)}, "
            f"TOKEN prefix={(TOKEN[:6]+'…') if len(TOKEN)>=6 else '?'}, account_type={ACCOUNT_TYPE})")
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
    # Thread still alive (including waiting for auto-restart).
    if _thread and _thread.is_alive() and _owner_pid == os.getpid():
        # Stuck offline: never connected or disconnected for too long → nudge reconnect.
        if (not BOT.stop and not BOT.connected and not BOT.reconnect_requested):
            ref = BOT.last_connect_attempt or BOT.thread_started_at
            if ref and (time.time() - ref) > 90:
                log("ensure_bot: offline >90s — requesting reconnect")
                BOT.reconnect_requested = True
                BOT.note("warn", "Still offline after 90s — retrying Deriv connection")
                ws = BOT._ws
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
        return
    # Permanent stops that should not auto-resurrect without user action.
    if BOT.stop and BOT.stop_reason in ("login", "user"):
        return
    # SL/TP wait is handled inside a living thread; if thread is dead, start fresh.
    if BOT.stop and BOT.stop_reason == "max_trades":
        # Soft-limit era: clear and restart so auto-refresh can run
        BOT.stop = False
        BOT.stop_reason = None
        BOT.restart_at = None
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
    from flask import Flask, Response, jsonify, request, send_from_directory
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

    @app.route("/static/<path:filename>")
    def static_files(filename):
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        return send_from_directory(static_dir, filename)

    @app.route("/health")
    def health():
        return jsonify(ok=True, bot_running=bot_running())

    @app.route("/api/state")
    @app.route("/status")
    def api_state():
        if not key_ok():
            return jsonify(error="forbidden"), 403
        try:
            payload = BOT.snapshot(bot_running(), bool(os.environ.get("STATUS_KEY")))
        except Exception as e:
            log(f"snapshot error: {e}")
            payload = {
                "bot_running": bot_running(),
                "connected": False,
                "stopped": True,
                "last_error": f"snapshot error: {e}",
                "events": [{"t": time.time(), "level": "error", "msg": f"snapshot error: {e}"}],
                "trades": [],
                "scanner": [],
                "strategies": [],
                "stats": {"trades": 0, "wins": 0, "losses": 0, "win_rate": None, "net": 0},
                "limits": {"max_trades": MAX_TRADES, "stop_loss": STOP_LOSS,
                           "take_profit": TAKE_PROFIT, "stake": STAKE},
                "account": dict(ACCOUNT),
                "balance": None,
                "session_pl": None,
                "currency": "USD",
                "protected": bool(os.environ.get("STATUS_KEY")),
                "trading_enabled": TRADING_ENABLED,
                "paused": False,
                "force_armed": False,
                "force": {"symbol": None, "strategy": None, "skip_filter": False},
                "auto_restart": AUTO_RESTART_AFTER_SL_TP,
                "restart_in": None,
                "stop_reason": None,
                "balance_history": [],
                "filter": {"window": DIGIT_WINDOW, "min_ticks": MIN_TICKS_FOR_FILTER},
                "session_count": 0,
            }
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.route("/api/control", methods=["POST"])
    def api_control():
        """Dashboard auto-trade controls: pause, resume, stop, force, reset_stats, reset_session."""
        if not key_ok():
            return jsonify(error="forbidden"), 403
        data = request.get_json(silent=True) or {}
        # Also accept form fields
        action = (data.get("action") or request.form.get("action") or "").strip().lower()
        symbol = data.get("symbol") or request.form.get("symbol")
        strategy = data.get("strategy") or request.form.get("strategy")
        skip_filter = data.get("skip_filter", data.get("skipFilter",
                      request.form.get("skip_filter")))
        if isinstance(skip_filter, str):
            skip_filter = skip_filter.strip().lower() in ("1", "true", "yes", "on")
        result = BOT.control(action, symbol=symbol, strategy=strategy,
                             skip_filter=bool(skip_filter))
        if action in ("reconnect", "restart_bot", "refresh_limit", "reset_trades", "refresh_trades", "resume"):
            # If the bot thread exited, start it again so trading can continue.
            ensure_bot()
            if not bot_running():
                start_background_bot()
        status = 200 if result.get("ok") else 400
        resp = jsonify(result)
        resp.headers["Cache-Control"] = "no-store"
        return resp, status

    # Under gunicorn the module is imported, not run as a script.
    if __name__ != "__main__" and os.environ.get("DERIV_AUTOSTART", "1") == "1":
        start_background_bot()

if __name__ == "__main__":
    main()