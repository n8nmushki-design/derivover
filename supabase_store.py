"""
Supabase persistence for Derivover (PostgREST over HTTPS).

Uses only the standard library so Render does not need extra packages.
Enabled by default when credentials are set:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_SECRET_KEY=eyJ...          # preferred (service_role / secret)
  # or SUPABASE_SERVICE_ROLE_KEY=...  # legacy name
  # SUPABASE_ENABLED=0                # only way to force off

If URL/key are missing, methods no-op (bot still trades). Demo vs real
are stored on every row via account_type.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _iso(ts: float | None = None) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class SupabaseStore:
    def __init__(self) -> None:
        """Supabase is ON by default whenever URL + key are present.

        Set SUPABASE_ENABLED=0 only if you want to force it off.
        """
        self.url = _env("SUPABASE_URL").rstrip("/")
        # Key names (new Supabase dashboard + legacy):
        #   SUPABASE_SECRET_KEY / SERVICE_ROLE_KEY  → server (full access)  ← preferred
        #   SUPABASE_PUBLISHABLE_KEY / ANON_KEY     → fallback (needs RLS policies)
        secret = (
            _env("SUPABASE_SECRET_KEY")
            or _env("SUPABASE_SERVICE_ROLE_KEY")
            or _env("SUPABASE_KEY")
        )
        self.publishable = _env("SUPABASE_PUBLISHABLE_KEY") or _env("SUPABASE_ANON_KEY")
        self.key = secret or self.publishable
        self.using_publishable = bool(self.key and not secret and self.publishable)

        # Default: enabled when credentials exist. Only SUPABASE_ENABLED=0 turns it off.
        flag = _env("SUPABASE_ENABLED").lower()
        force_off = flag in ("0", "false", "no", "off")
        force_on = flag in ("1", "true", "yes", "on")
        self.enabled = (not force_off) and bool(self.url and self.key)
        if force_on and not self.enabled:
            # User asked to enable but creds missing – stay off, log clearly
            print(
                "[supabase] SUPABASE_ENABLED=1 but URL/key missing – cannot enable",
                flush=True,
            )

        self._lock = threading.Lock()
        self.current_session_id: str | None = None
        self._last_error: str | None = None

        if self.enabled:
            kind = "publishable/anon" if self.using_publishable else "secret/service_role"
            print(f"[supabase] ENABLED by default → {self.url} (key={kind})", flush=True)
            if self.using_publishable:
                print(
                    "[supabase] warning: using PUBLISHABLE key. "
                    "If inserts fail, set SUPABASE_SECRET_KEY (service_role) instead.",
                    flush=True,
                )
        elif force_off:
            print("[supabase] disabled via SUPABASE_ENABLED=0", flush=True)
        else:
            print(
                "[supabase] waiting for credentials "
                "(set SUPABASE_URL + SUPABASE_SECRET_KEY to enable automatically)",
                flush=True,
            )

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def _request(
        self,
        method: str,
        path: str,
        body: dict | list | None = None,
        *,
        prefer: str | None = None,
        extra_headers: dict | None = None,
    ) -> Any:
        if not self.enabled:
            return None
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        if extra_headers:
            headers.update(extra_headers)
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            f"{self.url}/rest/v1/{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                raw = resp.read().decode()
                self._last_error = None
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()[:400]
            self._last_error = f"HTTP {e.code}: {err_body}"
            print(f"[supabase] {method} {path} failed: {self._last_error}", flush=True)
            return None
        except Exception as e:
            self._last_error = str(e)
            print(f"[supabase] {method} {path} error: {e}", flush=True)
            return None

    def start_session(
        self,
        *,
        account_type: str,
        account_id: str | None,
        session_no: int,
        start_balance: float | None,
        currency: str = "USD",
        strategy_mode: str = "digits",
    ) -> str | None:
        """Insert a new open session; returns session uuid or None."""
        if not self.enabled:
            return None
        # Close any previous open session for this account type (best effort)
        self._close_open_sessions(account_type)
        row = {
            "account_type": account_type if account_type in ("demo", "real") else "demo",
            "account_id": account_id,
            "session_no": session_no,
            "started_at": _iso(),
            "start_balance": start_balance,
            "currency": currency or "USD",
            "status": "open",
            "strategy_mode": strategy_mode if strategy_mode in ("digits", "shark") else "digits",
        }
        with self._lock:
            data = self._request(
                "POST",
                "trading_sessions",
                row,
                prefer="return=representation",
            )
            if isinstance(data, list) and data:
                sid = data[0].get("id")
                self.current_session_id = sid
                print(f"[supabase] session started {sid} ({account_type})", flush=True)
                return sid
            return None

    def _close_open_sessions(self, account_type: str) -> None:
        # Mark leftover open sessions closed (process restart)
        self._request(
            "PATCH",
            f"trading_sessions?account_type=eq.{account_type}&status=eq.open",
            {
                "status": "closed",
                "ended_at": _iso(),
                "stop_reason": "process_restart",
            },
            prefer="return=minimal",
        )

    def end_session(
        self,
        *,
        end_balance: float | None,
        session_pl: float | None,
        trades_count: int,
        wins: int,
        losses: int,
        stop_reason: str | None,
    ) -> None:
        if not self.enabled or not self.current_session_id:
            return
        payload = {
            "ended_at": _iso(),
            "end_balance": end_balance,
            "session_pl": session_pl,
            "trades_count": trades_count,
            "wins": wins,
            "losses": losses,
            "stop_reason": stop_reason,
            "status": "closed",
        }
        with self._lock:
            self._request(
                "PATCH",
                f"trading_sessions?id=eq.{self.current_session_id}",
                payload,
                prefer="return=minimal",
            )
            print(
                f"[supabase] session ended {self.current_session_id} pl={session_pl}",
                flush=True,
            )
            self.current_session_id = None

    def update_session_pl(
        self,
        *,
        end_balance: float | None,
        session_pl: float | None,
        trades_count: int | None = None,
        wins: int | None = None,
        losses: int | None = None,
    ) -> None:
        """Periodic / on-trade update of the open session P/L."""
        if not self.enabled or not self.current_session_id:
            return
        payload: dict[str, Any] = {
            "end_balance": end_balance,
            "session_pl": session_pl,
        }
        if trades_count is not None:
            payload["trades_count"] = trades_count
        if wins is not None:
            payload["wins"] = wins
        if losses is not None:
            payload["losses"] = losses
        with self._lock:
            self._request(
                "PATCH",
                f"trading_sessions?id=eq.{self.current_session_id}",
                payload,
                prefer="return=minimal",
            )

    def save_trade_open(self, rec: dict, *, account_type: str, account_id: str | None, currency: str) -> None:
        if not self.enabled:
            return
        row = {
            "session_id": self.current_session_id,
            "account_type": account_type if account_type in ("demo", "real") else "demo",
            "account_id": account_id,
            "contract_id": rec.get("contract_id"),
            "trade_no": rec.get("n"),
            "symbol": rec.get("symbol"),
            "strategy": rec.get("strategy"),
            "strategy_mode": rec.get("strategy_mode") or "digits",
            "side": rec.get("side"),
            "setup_meta": rec.get("setup_meta") or {},
            "contract_type": rec.get("contract_type"),
            "barrier": rec.get("barrier"),
            "stake": rec.get("stake"),
            "profit": None,
            "status": "open",
            "opened_at": _iso(rec.get("t_open")),
            "currency": currency or "USD",
        }
        with self._lock:
            # Plain insert; unique (account_type, contract_id) prevents duplicates
            self._request(
                "POST",
                "trades",
                row,
                prefer="return=minimal",
            )

    def save_trade_close(self, rec: dict, *, account_type: str) -> None:
        if not self.enabled:
            return
        cid = rec.get("contract_id")
        if cid is None:
            return
        payload = {
            "profit": rec.get("profit"),
            "status": rec.get("status"),
            "closed_at": _iso(rec.get("t_close")),
        }
        at = account_type if account_type in ("demo", "real") else "demo"
        with self._lock:
            self._request(
                "PATCH",
                f"trades?account_type=eq.{at}&contract_id=eq.{cid}",
                payload,
                prefer="return=minimal",
            )


    def save_shark_signal(self, sig, *, account_type: str, account_id: str | None = None) -> None:
        if not self.enabled:
            return
        row = {
            "account_type": account_type if account_type in ("demo", "real") else "demo",
            "account_id": account_id,
            "session_id": self.current_session_id,
            "symbol": getattr(sig, "symbol", None),
            "side": getattr(sig, "side", None),
            "entry_price": getattr(sig, "entry", None),
            "stop_loss": getattr(sig, "sl", None),
            "take_profit_1": getattr(sig, "tp1", None),
            "take_profit_2": getattr(sig, "tp2", None),
            "confidence": getattr(sig, "confidence", None),
            "reason": getattr(sig, "reason", None),
            "meta": getattr(sig, "meta", {}) or {},
            "traded": False,
        }
        with self._lock:
            self._request("POST", "shark_signals", row, prefer="return=minimal")

    def list_sessions(self, *, account_type: str | None = None, limit: int = 30) -> list:
        if not self.enabled:
            return []
        limit = max(1, min(int(limit or 30), 100))
        q = f"trading_sessions?select=*&order=started_at.desc&limit={limit}"
        if account_type in ("demo", "real"):
            q += f"&account_type=eq.{account_type}"
        data = self._request("GET", q)
        return data if isinstance(data, list) else []

    def list_trades(self, *, account_type: str | None = None, session_id: str | None = None, limit: int = 50) -> list:
        if not self.enabled:
            return []
        limit = max(1, min(int(limit or 50), 200))
        q = f"trades?select=*&order=opened_at.desc&limit={limit}"
        if account_type in ("demo", "real"):
            q += f"&account_type=eq.{account_type}"
        if session_id:
            q += f"&session_id=eq.{session_id}"
        data = self._request("GET", q)
        return data if isinstance(data, list) else []


# Singleton used by under9.py
STORE = SupabaseStore()
