# SPDX-License-Identifier: Apache-2.0
"""In-memory budget ledger: one per dataset, shared by every session on the same policy.

ε is reserved before a query runs and committed after noise is drawn. A
query the backend fails is charged too (up to the consumer's per-query
maximum), because a data-dependent failure is information. The dataset budget bounds every consumer
together (ADR 0004); a consumer cap bounds one consumer. The platform uses a persistent ledger behind
the same interface; this one serves library-only use and tests.
"""
from __future__ import annotations

import threading
import weakref
from fractions import Fraction

from .policy import Policy


class BudgetExhaustedException(Exception):
    pass


class Ledger:
    _by_policy: weakref.WeakKeyDictionary[Policy, Ledger] = weakref.WeakKeyDictionary()
    _registry_lock = threading.Lock()

    @classmethod
    def for_policy(cls, policy: Policy) -> Ledger:
        """The one ledger of this policy object: every session on it shares the dataset budget (ADR 0004)."""
        with cls._registry_lock:  # two first sessions at once must not get two ledgers
            ledger = cls._by_policy.get(policy)
            if ledger is None:
                ledger = cls._by_policy[policy] = cls(policy)
            return ledger

    def __init__(self, policy: Policy):
        self._lock = threading.Lock()  # reserve is check-then-set; sessions on several threads share one ledger
        self._spent: dict[str, Fraction] = {}
        self._reserved: dict[int, tuple[str, Fraction]] = {}
        self._last_reservation = 0
        self.update_policy(policy)

    def update_policy(self, policy: Policy) -> None:
        """Take a re-applied policy's budget and caps; keep every spent and reserved amount (ADR 0004).

        A budget lowered below what is spent leaves `remaining` negative, so
        every query is blocked until the admin starts a new epoch.
        """
        with self._lock:
            self.budget = policy.dataset.budget
            self.caps = {name: c.cap for name, c in policy.consumers.items()}
            self.rules = dict(policy.consumers)  # current ConsumerPolicy per consumer: sessions read rules from here
            for name in policy.consumers:
                self._spent.setdefault(name, Fraction(0))

    @property
    def spent_total(self) -> Fraction:
        return sum(self._spent.values(), Fraction(0)) + sum((e for _, e in self._reserved.values()), Fraction(0))

    def spent(self, consumer: str) -> Fraction:
        """Spent plus reserved by this consumer; a consumer dropped by a re-apply keeps its history."""
        return self._spent.get(consumer, Fraction(0)) + sum((e for c, e in self._reserved.values() if c == consumer), Fraction(0))

    def remaining(self, consumer: str) -> Fraction:
        """What this consumer may still spend: the dataset budget left, or their cap left if smaller."""
        left = self.budget - self.spent_total
        cap = self.caps.get(consumer)
        if cap is not None:
            left = min(left, cap - self.spent(consumer))
        return left

    def reserve(self, consumer: str, epsilon: Fraction) -> int:
        if not isinstance(epsilon, Fraction) or epsilon <= 0:
            raise ValueError(f"epsilon must be a positive Fraction, got {epsilon!r}")
        with self._lock:
            if epsilon > self.remaining(consumer):
                raise BudgetExhaustedException(f"requested {epsilon}, remaining {self.remaining(consumer)} for {consumer}")
            self._last_reservation += 1
            self._reserved[self._last_reservation] = (consumer, epsilon)
            return self._last_reservation

    def charge_up_to(self, consumer: str, amount: Fraction) -> None:
        """Spend `amount` more if it fits, else everything the consumer has left (never below zero)."""
        with self._lock:
            extra = min(amount, self.remaining(consumer))
            if extra > 0:
                self._spent[consumer] = self._spent.get(consumer, Fraction(0)) + extra

    def commit(self, reservation: int) -> None:
        with self._lock:
            consumer, epsilon = self._reserved.pop(reservation)
            self._spent[consumer] = self._spent.get(consumer, Fraction(0)) + epsilon

    def release(self, reservation: int) -> None:
        with self._lock:
            self._reserved.pop(reservation)
