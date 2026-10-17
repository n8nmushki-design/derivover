"""
Structure + Order Block + Harmonic Shark (simplified automation)

Aligned with the unified confluence rules:
  1) Market structure (BOS bias)
  2) Order block in premium (sell) / discount (buy)
  3) Shark-style 0.886 completion near the order block

Works on Deriv synthetic indices (default R_75 / V75) using recent ticks
aggregated into synthetic bars. Signals CALL (buy) or PUT (sell).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple


@dataclass
class SharkSignal:
    side: str              # "buy" | "sell"
    symbol: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float      # 0..1
    reason: str
    meta: dict


class SharkEngine:
    """Tick → bar → structure/OB/shark confluence."""

    def __init__(
        self,
        symbol: str = "R_75",
        bar_ticks: int = 20,
        max_bars: int = 120,
        max_ticks: int = 4000,
    ):
        self.symbol = symbol
        self.bar_ticks = max(5, bar_ticks)
        self.max_bars = max_bars
        self.ticks: Deque[float] = deque(maxlen=max_ticks)
        self.bars: List[dict] = []  # {o,h,l,c,n}
        self._buf: List[float] = []
        self.last_signal: Optional[SharkSignal] = None
        self.last_eval_reason: str = "collecting data"
        self.levels: dict = {}

    def on_tick(self, price: float) -> Optional[SharkSignal]:
        try:
            p = float(price)
        except (TypeError, ValueError):
            return None
        self.ticks.append(p)
        self._buf.append(p)
        if len(self._buf) < self.bar_ticks:
            self.last_eval_reason = f"building bar {len(self._buf)}/{self.bar_ticks}"
            return None
        o, h, l, c = self._buf[0], max(self._buf), min(self._buf), self._buf[-1]
        self.bars.append({"o": o, "h": h, "l": l, "c": c, "n": len(self._buf)})
        self._buf = []
        if len(self.bars) > self.max_bars:
            self.bars = self.bars[-self.max_bars:]
        return self.evaluate()

    def evaluate(self) -> Optional[SharkSignal]:
        bars = self.bars
        if len(bars) < 30:
            self.last_eval_reason = f"need more bars ({len(bars)}/30)"
            return None

        # Swings: local extremes
        highs = [b["h"] for b in bars]
        lows = [b["l"] for b in bars]
        closes = [b["c"] for b in bars]

        sh_idx = self._swing_highs(highs, left=2, right=2)
        sl_idx = self._swing_lows(lows, left=2, right=2)
        if len(sh_idx) < 2 or len(sl_idx) < 2:
            self.last_eval_reason = "not enough swing points"
            return None

        last_sh = sh_idx[-1]
        last_sl = sl_idx[-1]
        prev_sh = sh_idx[-2]
        prev_sl = sl_idx[-2]

        # Structure / BOS
        bullish_bos = highs[last_sh] > highs[prev_sh] and lows[last_sl] > lows[prev_sl]
        bearish_bos = highs[last_sh] < highs[prev_sh] and lows[last_sl] < lows[prev_sl]
        if not bullish_bos and not bearish_bos:
            self.last_eval_reason = "no clear BOS"
            return None

        # Impulse leg for premium/discount (0.5)
        if bullish_bos:
            leg_lo = lows[prev_sl]
            leg_hi = highs[last_sh]
        else:
            leg_hi = highs[prev_sh]
            leg_lo = lows[last_sl]
        if leg_hi <= leg_lo:
            self.last_eval_reason = "invalid impulse leg"
            return None
        mid = leg_lo + 0.5 * (leg_hi - leg_lo)
        price = closes[-1]
        level_886_preview = (
            leg_hi - 0.886 * (leg_hi - leg_lo) if bearish_bos
            else leg_lo + 0.886 * (leg_hi - leg_lo)
        )
        self.levels = {
            "leg_lo": leg_lo, "leg_hi": leg_hi, "mid": mid,
            "fib_886": level_886_preview,
            "bos": "bullish" if bullish_bos else "bearish",
            "price": price,
        }
        in_discount = price <= mid
        in_premium = price >= mid

        # Order block approximation: last opposite candle before impulse
        ob = self._order_block(bars, bullish=bullish_bos)
        if ob is None:
            self.last_eval_reason = "no unmitigated order block"
            return None
        ob_lo, ob_hi = ob
        self.levels["ob_lo"] = ob_lo
        self.levels["ob_hi"] = ob_hi
        # Mitigation: price closed through OB
        if bullish_bos and closes[-1] < ob_lo:
            self.last_eval_reason = "bullish OB mitigated"
            return None
        if bearish_bos and closes[-1] > ob_hi:
            self.last_eval_reason = "bearish OB mitigated"
            return None

        # Shark-style 0.886 of XC (use impulse as XC proxy)
        level_886 = leg_hi - 0.886 * (leg_hi - leg_lo) if bearish_bos else leg_lo + 0.886 * (leg_hi - leg_lo)
        # For bullish shark completion near discount OB; bearish near premium OB
        # Price should be near 0.886 and inside/near OB
        near_886 = abs(price - level_886) <= 0.15 * (leg_hi - leg_lo)
        near_ob = (ob_lo - 0.05 * (leg_hi - leg_lo)) <= price <= (ob_hi + 0.05 * (leg_hi - leg_lo))

        if bullish_bos:
            if not in_discount:
                self.last_eval_reason = "bullish BOS but not in discount"
                return None
            if not (near_886 and near_ob):
                self.last_eval_reason = "waiting for 0.886 + OB confluence (buy)"
                return None
            # SL below OB, TP along CD proxy (impulse height)
            rng = leg_hi - leg_lo
            sig = SharkSignal(
                side="buy",
                symbol=self.symbol,
                entry=price,
                sl=ob_lo - 0.05 * rng,
                tp1=price + 0.5 * rng,
                tp2=price + 0.886 * rng,
                confidence=0.72,
                reason="Bullish BOS + discount OB + 0.886 Shark",
                meta={
                    "bos": "bullish",
                    "zone": "discount",
                    "ob": [ob_lo, ob_hi],
                    "fib_886": level_886,
                    "mid": mid,
                },
            )
            self.last_signal = sig
            self.last_eval_reason = sig.reason
            return sig

        # bearish
        if not in_premium:
            self.last_eval_reason = "bearish BOS but not in premium"
            return None
        if not (near_886 and near_ob):
            self.last_eval_reason = "waiting for 0.886 + OB confluence (sell)"
            return None
        rng = leg_hi - leg_lo
        sig = SharkSignal(
            side="sell",
            symbol=self.symbol,
            entry=price,
            sl=ob_hi + 0.05 * rng,
            tp1=price - 0.5 * rng,
            tp2=price - 0.886 * rng,
            confidence=0.72,
            reason="Bearish BOS + premium OB + 0.886 Shark",
            meta={
                "bos": "bearish",
                "zone": "premium",
                "ob": [ob_lo, ob_hi],
                "fib_886": level_886,
                "mid": mid,
            },
        )
        self.last_signal = sig
        self.last_eval_reason = sig.reason
        return sig

    @staticmethod
    def _swing_highs(highs: List[float], left: int = 2, right: int = 2) -> List[int]:
        out = []
        for i in range(left, len(highs) - right):
            window = highs[i - left : i + right + 1]
            if highs[i] == max(window) and window.count(highs[i]) == 1:
                out.append(i)
        return out

    @staticmethod
    def _swing_lows(lows: List[float], left: int = 2, right: int = 2) -> List[int]:
        out = []
        for i in range(left, len(lows) - right):
            window = lows[i - left : i + right + 1]
            if lows[i] == min(window) and window.count(lows[i]) == 1:
                out.append(i)
        return out

    @staticmethod
    def _order_block(bars: List[dict], bullish: bool) -> Optional[Tuple[float, float]]:
        """Last opposing candle before the most recent impulsive run."""
        if len(bars) < 8:
            return None
        # Look back for a strong move
        for i in range(len(bars) - 3, 3, -1):
            b = bars[i]
            body = b["c"] - b["o"]
            if bullish:
                # bearish candle then up move
                if body < 0 and bars[i + 1]["c"] > b["h"]:
                    return (b["l"], b["h"])
            else:
                if body > 0 and bars[i + 1]["c"] < b["l"]:
                    return (b["l"], b["h"])
        # Fallback: use a recent bar range as zone
        b = bars[-5]
        return (b["l"], b["h"])

    def status(self) -> dict:
        closes = [b["c"] for b in self.bars[-80:]] if self.bars else []
        return {
            "symbol": self.symbol,
            "bars": len(self.bars),
            "ticks": len(self.ticks),
            "reason": self.last_eval_reason,
            "levels": dict(self.levels or {}),
            "closes": closes,
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
