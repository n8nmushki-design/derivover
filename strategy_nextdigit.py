"""
Next-digit prediction for Deriv synthetics.

Predicts the most likely *next* last-digit (0–9) using:
  1) First-order transition counts  P(next | last)
  2) Fallback to global frequency when the last digit is rare in the window

Trades DIGITMATCH on the predicted digit (1-tick).
Default symbols: 1s volatility indices preferred for digit work.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


NEXT_DIGIT_SYMBOLS = ("1HZ10V", "1HZ25V", "1HZ50V", "1HZ75V", "1HZ100V", "R_10", "R_25", "R_50", "R_75", "R_100")


@dataclass
class NextDigitSignal:
    symbol: str
    digit: int
    confidence: float
    reason: str
    meta: dict = field(default_factory=dict)


class NextDigitEngine:
    def __init__(
        self,
        window: int = 120,
        min_ticks: int = 50,
        min_confidence: float = 0.14,  # above uniform 0.10
        symbols: Tuple[str, ...] = NEXT_DIGIT_SYMBOLS,
    ):
        self.window = max(30, window)
        self.min_ticks = min_ticks
        self.min_confidence = min_confidence
        self.symbols = symbols
        self.digits: Dict[str, Deque[int]] = {
            s: deque(maxlen=self.window) for s in symbols
        }
        self.last_signal: Optional[NextDigitSignal] = None
        self.last_reason: str = "collecting digits"
        self.status_by_symbol: Dict[str, dict] = {}

    def on_digit(self, symbol: str, digit: int) -> Optional[NextDigitSignal]:
        if digit < 0 or digit > 9:
            return None
        if symbol not in self.digits:
            self.digits[symbol] = deque(maxlen=self.window)
        hist = self.digits[symbol]
        hist.append(int(digit))
        n = len(hist)
        if n < self.min_ticks:
            self.last_reason = f"{symbol}: collecting {n}/{self.min_ticks}"
            self.status_by_symbol[symbol] = {
                "ticks": n,
                "ready": False,
                "prediction": None,
            }
            return None

        # Transition matrix: from -> counts of next
        trans: Dict[int, List[int]] = {i: [0] * 10 for i in range(10)}
        freq = [0] * 10
        seq = list(hist)
        for d in seq:
            freq[d] += 1
        for a, b in zip(seq, seq[1:]):
            trans[a][b] += 1

        last = seq[-1]
        row = trans[last]
        row_n = sum(row)
        probs = [0.0] * 10
        if row_n >= 5:
            probs = [c / row_n for c in row]
            source = "transition"
        else:
            # sparse row → blend with global frequency
            probs = [f / n for f in freq]
            source = "frequency"

        pred = max(range(10), key=lambda d: (probs[d], freq[d], -d))
        conf = float(probs[pred])

        ranking = sorted(range(10), key=lambda d: (-probs[d], -freq[d], d))
        top3 = [(d, round(probs[d], 4)) for d in ranking[:3]]

        self.status_by_symbol[symbol] = {
            "ticks": n,
            "ready": True,
            "last_digit": last,
            "prediction": pred,
            "confidence": round(conf, 4),
            "source": source,
            "top3": top3,
            "freq": {str(i): freq[i] for i in range(10)},
        }

        if conf < self.min_confidence:
            self.last_reason = (
                f"{symbol}: weak edge digit {pred} @ {conf:.1%} (need ≥{self.min_confidence:.0%})"
            )
            return None

        sig = NextDigitSignal(
            symbol=symbol,
            digit=pred,
            confidence=min(0.95, conf),
            reason=f"Next digit {pred} ({source}) conf {conf:.1%} after {last}",
            meta={
                "last": last,
                "probs": [round(p, 4) for p in probs],
                "source": source,
                "top3": top3,
                "window": n,
            },
        )
        self.last_signal = sig
        self.last_reason = sig.reason
        return sig

    def status(self) -> dict:
        return {
            "reason": self.last_reason,
            "symbols": list(self.symbols),
            "by_symbol": dict(self.status_by_symbol),
            "last_signal": None
            if not self.last_signal
            else {
                "symbol": self.last_signal.symbol,
                "digit": self.last_signal.digit,
                "confidence": self.last_signal.confidence,
                "reason": self.last_signal.reason,
                "meta": self.last_signal.meta,
            },
        }
