"""
Structure + Order Block + Harmonic Shark — multi-timeframe

Practical combination (from strategy guide):
  • 1H  — market structure / BOS bias
  • 15M — Order Block + premium/discount setup
  • 5M  — Shark 0.886 entry + Fibonacci targets

Signals CALL (buy) or PUT (sell) on Deriv synthetics (default R_75).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# Deriv granularity (seconds)
TF_LABELS = {
    60: "1M",
    300: "5M",
    900: "15M",
    3600: "1H",
}
TF_BIAS = 3600
TF_SETUP = 900
TF_ENTRY = 300


@dataclass
class SharkSignal:
    side: str
    symbol: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float
    reason: str
    meta: dict = field(default_factory=dict)


class SharkEngine:
    """Multi-TF Structure + OB + Shark confluence."""

    def __init__(self, symbol: str = "R_75", max_bars: int = 200):
        self.symbol = symbol
        self.max_bars = max_bars
        # granularity -> list of {t,o,h,l,c}
        self.candles: Dict[int, List[dict]] = {
            60: [],
            300: [],
            900: [],
            3600: [],
        }
        self.last_signal: Optional[SharkSignal] = None
        self.last_eval_reason: str = "waiting for 1H / 15M / 5M candles"
        self.levels: dict = {}
        self.targets: dict = {}  # live fib targets for UI
        self.bias: Optional[str] = None  # bullish | bearish

    # ---------- candle ingest ----------
    def set_candles(self, granularity: int, rows: List[dict]) -> None:
        g = int(granularity)
        if g not in self.candles:
            self.candles[g] = []
        cleaned = []
        for r in rows:
            try:
                cleaned.append({
                    "t": int(r.get("epoch") or r.get("t") or 0),
                    "o": float(r["open"] if "open" in r else r["o"]),
                    "h": float(r["high"] if "high" in r else r["h"]),
                    "l": float(r["low"] if "low" in r else r["l"]),
                    "c": float(r["close"] if "close" in r else r["c"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        cleaned.sort(key=lambda x: x["t"])
        self.candles[g] = cleaned[-self.max_bars:]

    def upsert_ohlc(self, granularity: int, candle: dict) -> Optional[SharkSignal]:
        """Merge a live OHLC update; re-evaluate on entry TF."""
        g = int(granularity)
        if g not in self.candles:
            self.candles[g] = []
        try:
            row = {
                "t": int(candle.get("epoch") or candle.get("t") or 0),
                "o": float(candle.get("open", candle.get("o"))),
                "h": float(candle.get("high", candle.get("h"))),
                "l": float(candle.get("low", candle.get("l"))),
                "c": float(candle.get("close", candle.get("c"))),
            }
        except (TypeError, ValueError, KeyError):
            return None
        bars = self.candles[g]
        if bars and bars[-1]["t"] == row["t"]:
            bars[-1] = row
        else:
            bars.append(row)
            if len(bars) > self.max_bars:
                self.candles[g] = bars[-self.max_bars:]
        # Also update from tick price on entry TF
        return self.evaluate() if g in (TF_ENTRY, TF_SETUP, TF_BIAS) else None

    def on_tick(self, price: float) -> Optional[SharkSignal]:
        """Fallback: update last 5M candle close with tick (if any)."""
        try:
            p = float(price)
        except (TypeError, ValueError):
            return None
        bars = self.candles.get(TF_ENTRY) or []
        if bars:
            bars[-1]["c"] = p
            bars[-1]["h"] = max(bars[-1]["h"], p)
            bars[-1]["l"] = min(bars[-1]["l"], p)
        return self.evaluate()

    # ---------- structure helpers ----------
    @staticmethod
    def _swing_highs(highs: List[float], left: int = 2, right: int = 2) -> List[int]:
        out = []
        for i in range(left, len(highs) - right):
            window = highs[i - left: i + right + 1]
            if highs[i] == max(window) and window.count(highs[i]) == 1:
                out.append(i)
        return out

    @staticmethod
    def _swing_lows(lows: List[float], left: int = 2, right: int = 2) -> List[int]:
        out = []
        for i in range(left, len(lows) - right):
            window = lows[i - left: i + right + 1]
            if lows[i] == min(window) and window.count(lows[i]) == 1:
                out.append(i)
        return out

    @staticmethod
    def _order_block(bars: List[dict], bullish: bool) -> Optional[Tuple[float, float]]:
        if len(bars) < 8:
            return None
        for i in range(len(bars) - 3, 3, -1):
            b = bars[i]
            body = b["c"] - b["o"]
            if bullish:
                if body < 0 and bars[i + 1]["c"] > b["h"]:
                    return (b["l"], b["h"])
            else:
                if body > 0 and bars[i + 1]["c"] < b["l"]:
                    return (b["l"], b["h"])
        b = bars[-5]
        return (b["l"], b["h"])

    def _bos(self, bars: List[dict]) -> Optional[str]:
        if len(bars) < 20:
            return None
        highs = [b["h"] for b in bars]
        lows = [b["l"] for b in bars]
        sh = self._swing_highs(highs)
        sl = self._swing_lows(lows)
        if len(sh) < 2 or len(sl) < 2:
            return None
        last_sh, prev_sh = sh[-1], sh[-2]
        last_sl, prev_sl = sl[-1], sl[-2]
        if highs[last_sh] > highs[prev_sh] and lows[last_sl] > lows[prev_sl]:
            return "bullish"
        if highs[last_sh] < highs[prev_sh] and lows[last_sl] < lows[prev_sl]:
            return "bearish"
        return None

    # ---------- multi-TF evaluate ----------
    def evaluate(self) -> Optional[SharkSignal]:
        h1 = self.candles.get(TF_BIAS) or []
        m15 = self.candles.get(TF_SETUP) or []
        m5 = self.candles.get(TF_ENTRY) or []

        if len(h1) < 20:
            self.last_eval_reason = f"1H bias: need candles ({len(h1)}/20)"
            return None
        if len(m15) < 25:
            self.last_eval_reason = f"15M setup: need candles ({len(m15)}/25)"
            return None
        if len(m5) < 30:
            self.last_eval_reason = f"5M entry: need candles ({len(m5)}/30)"
            return None

        # Filter 1 — 1H structure bias
        bias = self._bos(h1)
        self.bias = bias
        if not bias:
            self.last_eval_reason = "1H: no clear BOS — wait"
            self._publish_levels(m5, m15, bias=None)
            return None

        # Filter 2 — 15M OB in premium/discount
        # Impulse from 15M relative to bias
        highs15 = [b["h"] for b in m15]
        lows15 = [b["l"] for b in m15]
        sh15 = self._swing_highs(highs15)
        sl15 = self._swing_lows(lows15)
        if len(sh15) < 1 or len(sl15) < 1:
            self.last_eval_reason = "15M: not enough swings"
            return None

        if bias == "bullish":
            leg_lo = lows15[sl15[-1]]
            leg_hi = highs15[sh15[-1]]
        else:
            leg_hi = highs15[sh15[-1]]
            leg_lo = lows15[sl15[-1]]
        if leg_hi <= leg_lo:
            self.last_eval_reason = "15M: invalid impulse leg"
            return None

        mid = leg_lo + 0.5 * (leg_hi - leg_lo)
        price = m5[-1]["c"]
        fib_886 = (
            leg_hi - 0.886 * (leg_hi - leg_lo)
            if bias == "bearish"
            else leg_lo + 0.886 * (leg_hi - leg_lo)
        )
        # CD-style fib targets from entry zone (guide: TP1 0.5 CD, TP2 0.886 CD)
        rng = leg_hi - leg_lo
        if bias == "bullish":
            tp1 = price + 0.5 * rng
            tp2 = price + 0.886 * rng
            sl_preview = None
        else:
            tp1 = price - 0.5 * rng
            tp2 = price - 0.886 * rng
            sl_preview = None

        ob = self._order_block(m15, bullish=(bias == "bullish"))
        if ob is None:
            self.last_eval_reason = f"15M: no OB for {bias} bias"
            self._publish_levels(m5, m15, bias=bias, mid=mid, fib_886=fib_886,
                                 leg_lo=leg_lo, leg_hi=leg_hi, tp1=tp1, tp2=tp2)
            return None
        ob_lo, ob_hi = ob

        in_discount = price <= mid
        in_premium = price >= mid
        if bias == "bullish" and not in_discount:
            self.last_eval_reason = "15M: bullish bias but price not in discount"
            self._publish_levels(m5, m15, bias=bias, mid=mid, fib_886=fib_886,
                                 leg_lo=leg_lo, leg_hi=leg_hi, ob_lo=ob_lo, ob_hi=ob_hi,
                                 tp1=tp1, tp2=tp2)
            return None
        if bias == "bearish" and not in_premium:
            self.last_eval_reason = "15M: bearish bias but price not in premium"
            self._publish_levels(m5, m15, bias=bias, mid=mid, fib_886=fib_886,
                                 leg_lo=leg_lo, leg_hi=leg_hi, ob_lo=ob_lo, ob_hi=ob_hi,
                                 tp1=tp1, tp2=tp2)
            return None

        # Filter 3 — 5M Shark 0.886 near OB
        near_886 = abs(price - fib_886) <= 0.12 * rng
        near_ob = (ob_lo - 0.05 * rng) <= price <= (ob_hi + 0.05 * rng)

        self._publish_levels(
            m5, m15, bias=bias, mid=mid, fib_886=fib_886,
            leg_lo=leg_lo, leg_hi=leg_hi, ob_lo=ob_lo, ob_hi=ob_hi,
            tp1=tp1, tp2=tp2, entry=price,
        )

        if not (near_886 and near_ob):
            self.last_eval_reason = (
                f"5M: waiting 0.886 + OB confluence "
                f"(bias={bias}, near_886={near_886}, near_ob={near_ob})"
            )
            return None

        if bias == "bullish":
            sl = ob_lo - 0.05 * rng
            sig = SharkSignal(
                side="buy", symbol=self.symbol, entry=price, sl=sl,
                tp1=tp1, tp2=tp2, confidence=0.78,
                reason="1H bullish BOS + 15M discount OB + 5M 0.886 Shark",
                meta={
                    "bos": "bullish", "zone": "discount",
                    "ob": [ob_lo, ob_hi], "fib_886": fib_886, "mid": mid,
                    "tf": {"bias": "1H", "setup": "15M", "entry": "5M"},
                    "targets": {"tp1": tp1, "tp2": tp2, "sl": sl},
                },
            )
        else:
            sl = ob_hi + 0.05 * rng
            sig = SharkSignal(
                side="sell", symbol=self.symbol, entry=price, sl=sl,
                tp1=tp1, tp2=tp2, confidence=0.78,
                reason="1H bearish BOS + 15M premium OB + 5M 0.886 Shark",
                meta={
                    "bos": "bearish", "zone": "premium",
                    "ob": [ob_lo, ob_hi], "fib_886": fib_886, "mid": mid,
                    "tf": {"bias": "1H", "setup": "15M", "entry": "5M"},
                    "targets": {"tp1": tp1, "tp2": tp2, "sl": sl},
                },
            )
        self.last_signal = sig
        self.last_eval_reason = sig.reason
        self.targets = {"entry": price, "sl": sl, "tp1": tp1, "tp2": tp2, "side": sig.side}
        return sig

    def _publish_levels(self, m5, m15, **kw):
        price = m5[-1]["c"] if m5 else None
        # Independent BOS on each TF (same method, different bars)
        m5b = list(self.candles.get(TF_ENTRY) or [])
        m15b = list(self.candles.get(TF_SETUP) or [])
        h1b = list(self.candles.get(TF_BIAS) or [])
        bos_5m = self._bos(m5b) if len(m5b) >= 20 else None
        bos_15m = self._bos(m15b) if len(m15b) >= 20 else None
        bos_1h = self._bos(h1b) if len(h1b) >= 20 else (kw.get("bias") or self.bias)
        self.levels = {
            "price": price,
            "bos": kw.get("bias") or bos_1h,  # primary = 1H strategy bias
            "bos_1h": bos_1h,
            "bos_15m": bos_15m,
            "bos_5m": bos_5m,
            "mid": kw.get("mid"),
            "fib_886": kw.get("fib_886"),
            "leg_lo": kw.get("leg_lo"),
            "leg_hi": kw.get("leg_hi"),
            "ob_lo": kw.get("ob_lo"),
            "ob_hi": kw.get("ob_hi"),
            "tp1": kw.get("tp1"),
            "tp2": kw.get("tp2"),
            "entry": kw.get("entry"),
            "tf_bias": "1H",
            "tf_setup": "15M",
            "tf_entry": "5M",
        }
        if kw.get("tp1") is not None:
            self.targets = {
                "entry": kw.get("entry") or price,
                "sl": None,
                "tp1": kw.get("tp1"),
                "tp2": kw.get("tp2"),
                "side": "buy" if kw.get("bias") == "bullish" else (
                    "sell" if kw.get("bias") == "bearish" else None
                ),
            }

    def status(self) -> dict:
        def n(g):
            return len(self.candles.get(g) or [])

        return {
            "symbol": self.symbol,
            "bars": n(TF_ENTRY),
            "ticks": n(TF_ENTRY),  # UI compat
            "counts": {
                "1m": n(60), "5m": n(TF_ENTRY), "15m": n(TF_SETUP), "1h": n(TF_BIAS),
            },
            "reason": self.last_eval_reason,
            "bias": self.bias,
            "bos_1h": (self.levels or {}).get("bos_1h") or self.bias,
            "bos_15m": (self.levels or {}).get("bos_15m"),
            "bos_5m": (self.levels or {}).get("bos_5m"),
            "levels": dict(self.levels or {}),
            "targets": dict(self.targets or {}),
            "candles": {
                str(g): list(rows[-80:])
                for g, rows in self.candles.items()
            },
            "closes": [b["c"] for b in (self.candles.get(TF_ENTRY) or [])[-80:]],
            "last_signal": None
            if not self.last_signal
            else {
                "side": self.last_signal.side,
                "entry": self.last_signal.entry,
                "sl": self.last_signal.sl,
                "tp1": self.last_signal.tp1,
                "tp2": self.last_signal.tp2,
                "confidence": self.last_signal.confidence,
                "reason": self.last_signal.reason,
                "meta": self.last_signal.meta,
            },
        }
