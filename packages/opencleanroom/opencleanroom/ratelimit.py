# SPDX-License-Identifier: Apache-2.0
"""Token bucket per key hash for the single gateway process (ADR 0006); one global bucket for keys that do not resolve."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict

GLOBAL = "*"  # bucket for requests whose key does not resolve: a flood of bad keys is limited as one consumer


class RateLimiter:
    """`rate` is "<requests>/<seconds>": a bucket holds `requests` tokens and refills them evenly over `seconds`.

    Buckets live in an LRU of `max_keys` entries; an evicted key starts full again.
    """

    def __init__(self, rate: str = "60/60", max_keys: int = 10_000, clock=time.monotonic):
        try:
            requests, seconds = (int(x) for x in rate.split("/"))
        except ValueError:
            raise ValueError(f"rate must look like 60/60, got {rate!r}") from None
        if requests <= 0 or seconds <= 0:
            raise ValueError(f"rate must be positive, got {rate!r}")
        self.capacity = float(requests)
        self.per_second = requests / seconds
        self.max_keys = max_keys
        self._clock = clock
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()  # key -> (tokens, last refill)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.pop(key, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - last) * self.per_second)
            ok = tokens >= 1.0
            if ok:
                tokens -= 1.0
            self._buckets[key] = (tokens, now)
            while len(self._buckets) > self.max_keys:
                self._buckets.popitem(last=False)
            return ok
