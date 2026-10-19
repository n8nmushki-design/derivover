"""
Digit Matches / Difference strategy for Deriv synthetics.

Local analysis (no external site dependency):
  - Build last-digit histogram over a rolling window
  - Prefer DIGITMATCH on the hottest digit (frequency edge)
  - Optionally also DIGITDIFF on the coldest digit ("Both" mode)

Default symbols: Volatility 10 (1s) and Volatility 100 (1s).
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


MATCH_SYMBOLS = ("1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V", "R_10", "R_25", "R_50", "R_75", "R_100")


@dataclass
class MatchSignal:
    symbol: str
    digit: int
    side: str  # "match" | "diff"
    contract_type: str  # DIGITMATCH | DIGITDIFF
    confidence: float
    reason: str
    meta: dict = field(default_factory=dict)


class MatchEngine:
    def __init__(
        self,
        window: int = 60,
        min_ticks: int = 25,
        both: bool = True,
        symbols: Tuple[str, ...] = MATCH_SYMBOLS,
    ):
        self.window = max(20, window)
        self.min_ticks = min_ticks
        self.both = both
        self.symbols = symbols
        self.digits: Dict[str, Deque[int]] = {s: deque(maxlen=self.window) for s in symbols}
        self.last_signal: Optional[MatchSignal] = None
        self.last_reason: str = "collecting digits"
        self.status_by_symbol: Dict[str, dict] = {}

    def on_digit(self, symbol: str, digit: int) -> Optional[List[MatchSignal]]:
        if symbol not in self.digits:
            self.digits[symbol] = deque(maxlen=self.window)
        if digit < 0 or digit > 9:
            return None
        self.digits[symbol].append(int(digit))
        hist = list(self.digits[symbol])
        n = len(hist)
        if n < self.min_ticks:
            self.last_reason = f"{symbol}: collecting {n}/{self.min_ticks}"
            self.status_by_symbol[symbol] = {"ticks": n, "ready": False}
            return None

        cnt = Counter(hist)
        # hottest / coldest
        hot = max(range(10), key=lambda d: (cnt.get(d, 0), -d))
        cold = min(range(10), key=lambda d: (cnt.get(d, 0), d))
        hot_f = cnt.get(hot, 0) / n
        cold_f = cnt.get(cold, 0) / n
        # edge vs uniform 10%
        hot_edge = hot_f - 0.10
        cold_edge = 0.10 - cold_f

        self.status_by_symbol[symbol] = {
            "ticks": n,
            "ready": True,
            "hot": hot,
            "cold": cold,
            "hot_freq": round(hot_f, 4),
            "cold_freq": round(cold_f, 4),
            "counts": {str(i): cnt.get(i, 0) for i in range(10)},
        }

        # Need a mild edge to trade
        signals: List[MatchSignal] = []
        if hot_edge >= 0.02:
            signals.append(
                MatchSignal(
                    symbol=symbol,
                    digit=hot,
                    side="match",
                    contract_type="DIGITMATCH",
                    confidence=min(0.9, 0.5 + hot_edge * 3),
                    reason=f"Hot digit {hot} freq {hot_f:.1%} (edge +{hot_edge:.1%})",
                    meta={"freq": hot_f, "edge": hot_edge, "window": n},
                )
            )
        if self.both and cold_edge >= 0.02:
            signals.append(
                MatchSignal(
                    symbol=symbol,
                    digit=cold,
                    side="diff",
                    contract_type="DIGITDIFF",
                    confidence=min(0.9, 0.5 + cold_edge * 3),
                    reason=f"Cold digit {cold} freq {cold_f:.1%} (edge +{cold_edge:.1%})",
                    meta={"freq": cold_f, "edge": cold_edge, "window": n},
                )
            )

        if not signals:
            self.last_reason = f"{symbol}: no edge (hot {hot}@{hot_f:.1%}, cold {cold}@{cold_f:.1%})"
            return None

        self.last_signal = signals[0]
        self.last_reason = signals[0].reason
        return signals

    def status(self) -> dict:
        return {
            "reason": self.last_reason,
            "both": self.both,
            "symbols": list(self.symbols),
            "by_symbol": dict(self.status_by_symbol),
            "last_signal": None
            if not self.last_signal
            else {
                "symbol": self.last_signal.symbol,
                "digit": self.last_signal.digit,
                "side": self.last_signal.side,
                "contract_type": self.last_signal.contract_type,
                "confidence": self.last_signal.confidence,
                "reason": self.last_signal.reason,
            },
        }
