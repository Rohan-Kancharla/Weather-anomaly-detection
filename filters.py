from collections import deque
from typing import Optional

"""
Streaming Data Analytics (SDA) Filters

Provides simple streaming utilities:
- Moving Average (windowed)
- Exponential Weighted Moving Average (EWMA)

These are stateful utilities designed for online/streaming usage.
"""


class StreamingMovingAverage:
    def __init__(self, window_size: int = 3):
        self.window_size = max(1, int(window_size))
        self.window = deque(maxlen=self.window_size)

    def update(self, value: float) -> float:
        """Add new value and return current moving average."""
        self.window.append(float(value))
        return sum(self.window) / len(self.window)


class StreamingEWMA:
    def __init__(self, alpha: float = 0.3):
        self.alpha = float(alpha)
        self._prev: Optional[float] = None

    def update(self, value: float) -> float:
        """Update EWMA with new value and return smoothed value."""
        x = float(value)
        if self._prev is None:
            self._prev = x
            return x
        self._prev = self.alpha * x + (1.0 - self.alpha) * self._prev
        return self._prev


def moving_average(data, window_size: int = 3):
    """Batch moving average over a list-like input."""
    window_size = max(1, int(window_size))
    out = []
    dq = deque(maxlen=window_size)
    for x in data:
        dq.append(float(x))
        out.append(sum(dq) / len(dq))
    return out


def exponential_smoothing(data, alpha: float = 0.3):
    """Batch EWMA smoothing over a list-like input."""
    out = []
    prev = None
    for x in data:
        x = float(x)
        if prev is None:
            prev = x
        else:
            prev = alpha * x + (1 - alpha) * prev
        out.append(prev)
    return out