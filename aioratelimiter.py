# SPDX-License-Identifier: MIT
# Copyright (c) 2022 Bar Harel
# Licensed under the MIT license as detailed in LICENSE.txt
"""AsyncIO rate limiters.

This module provides different rate limiters for asyncio.

    - `Limiter`: Limits by requests per second and takes into account CPU heavy
    tasks or other delays that can occur while the process is sleeping.
    - `LeakyBucketLimiter`: Limits by requests per second according to the
    leaky bucket algorithm. Has a maximum capacity an initial burst of requests.
    - `StrictLimiter`: Limits by requests per second, without taking CPU or other
    process sleeps into account. There are no bursts and the resulting rate will
    always be a less than the set limit.

If you don't know which of these to choose, go for the regular Limiter.
"""

from abc import ABC as _ABC, abstractmethod as _abstractmethod
import asyncio as _asyncio
from collections import deque as _deque
from collections.abc import Awaitable as _Awaitable
import functools as _functools
from typing import TypeVar as _TypeVar

__all__ = ['Limiter', 'StrictLimiter', 'LeakyBucketLimiter']
__version__ = "0.1.0"
__author__ = "Bar Harel"
__license__ = "MIT"
__copyright__ = "Copyright (c) 2022 Bar Harel"


_T = _TypeVar('_T')


def _pop_pending(futures: _deque[_asyncio.Future]) -> _asyncio.Future | None:
    """Pop until the first pending future is found and return it.

    If all futures are done, or deque is empty, return None.

    Args:
        futures: A deque of futures.

    Returns:
        The first pending future, or None if all futures are done.
    """
    while futures:
        waiter = futures.popleft()
        if not waiter.done():
            return waiter
    return None


class _BaseLimiter(_ABC):
    """Base class for all limiters."""
    @_abstractmethod
    async def wait(self) -> None:  # pragma: no cover # ABC
        """Wait for the limiter to let us through.

        Main function of the limiter. Blocks if limit has been reached, and 
        lets us through once time passes.
        """
        pass

    @_abstractmethod
    def cancel(self) -> None:  # pragma: no cover # ABC
        pass

    @_abstractmethod
    def breach(self) -> None:  # pragma: no cover # ABC
        """Let all calls through"""
        pass

    @_abstractmethod
    def reset(self) -> None:  # pragma: no cover # ABC
        pass

    def wrap(self, coro: _Awaitable[_T]) -> _Awaitable[_T]:
        """Wrap a coroutine with the limiter.

        This will wait for the limiter to be unlocked, and then schedule the
        coroutine.
        """
        @_functools.wraps(coro)
        async def _wrapper() -> None:
            await self.wait()
            return await coro
        return _wrapper()


class _CommonLimiterMixin(_BaseLimiter):
    """Some common attributes a limiter might need.

    Includes:
        _waiters: A deque of futures waiting for the limiter to be unlocked.
        _locked: Whether the limiter is locked.
        _breached: Whether the limiter has been breached.
        _wakeup_handle: An asyncio.TimerHandle for the next scheduled wakeup.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._locked = False
        self._waiters: _deque[_asyncio.Future] = _deque()
        self._wakeup_handle: _asyncio.TimerHandle = None
        self._breached = False

    async def wait(self) -> None:
        """Wait for the limiter to be unlocked."""
        if self._breached:
            return

        if not self._locked:
            self._maybe_lock()
            return
        fut = _asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        await fut

    def cancel(self) -> None:
        """Cancel all waiting calls.

        This will cancel all currently waiting calls.
        Limiter is reusable afterwards, and new calls will wait as usual.
        """
        while self._waiters:
            self._waiters.popleft().cancel()

    def breach(self) -> None:
        """Let all calls through."""
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)
        self._cancel_wakeup()
        self._breached = True
        self._locked = False

    def _cancel_wakeup(self) -> None:
        if self._wakeup_handle is not None:
            self._wakeup_handle.cancel()
            self._wakeup_handle = None

    def reset(self) -> None:
        """Reset the limiter.

        This will cancel all waiting calls and reset all internal timers.
        Limiter is reusable afterwards, and the next call will be 
        immediately scheduled.
        """
        self.cancel()
        self._cancel_wakeup()

        self._locked = False
        self._breached = False

    @_abstractmethod
    def _maybe_lock(self):  # pragma: no cover # ABC
        pass

    def __del__(self):
        # No need to touch wakeup, as wakeup holds a strong reference and
        # __del__ won't be called.
        try:
            # Technically this should never happen, where there are waiters
            # without a wakeup scheduled. Means there was a bug in the code.
            waiters = self._waiters

        # Error during initialization before _waiters exists.
        except AttributeError:  # pragma: no cover # Technically a bug.
            return

        any_waiting = False
        for fut in waiters:  # pragma: no cover # Technically a bug.
            if not fut.done():
                fut.cancel()
                any_waiting = True

        # Alert for the bug.
        assert not any_waiting, "__del__ was called with waiters still waiting"

    def close(self) -> None:
        """Close the limiter.

        This will cancel all waiting calls. Limiter is unusable afterwards.
        """
        self.cancel()
        self._cancel_wakeup()


class Limiter(_CommonLimiterMixin):
    """Regular, with max burst for backlog."""
    max_burst: int = 5
    rate: float

    def __init__(self, rate: float) -> None:
        """Create a new limiter.

        Args:
            rate: The rate (calls per second) at which calls can pass through.
        """
        super().__init__()
        self.rate = rate

    def _maybe_lock(self):
        self._locked = True
        self._schedule_wakeup()

    def _schedule_wakeup(self, at: float | None = None, *, _loop=None) -> None:
        """Schedule the next wakeup to be unlocked."""
        loop = _loop or _asyncio.get_running_loop()
        if at is None:
            at = loop.time() + 1 / self.rate
        self._wakeup_handle = loop.call_at(at, self._wakeup)
        # Saving next wakeup and not this wakeup to account for fractions
        # of rate passed. See leftover_time under _wakeup.
        self._next_wakeup = at

    def _wakeup(self) -> None:
        def _unlock() -> None:
            self._wakeup_handle = None
            self._locked = False
            return
        loop = _asyncio.get_running_loop()
        waiters = self._waiters
        # Short circuit if there are no waiters
        if not waiters:
            _unlock()
            return

        this_wakeup = self._next_wakeup
        current_time = loop.time()
        # We woke up early. Damn event loop!
        if current_time < this_wakeup:
            missed_wakeups = 0
            # We have a negative leftover bois. Increase the next sleep!
            leftover_time = current_time - this_wakeup
            # More than 1 tick early. Great success.
            # Technically the higher the rate, the more likely the event loop
            # should be late. If we came early on 2 ticks, that's really bad.
            assert -leftover_time < 1/self.rate, (
                f"Event loop is too fast. Woke up {-leftover_time*self.rate} "
                f"ticks early.")

        else:
            # We woke up too late!
            # Missed wakeups can happen in case of heavy CPU-bound activity,
            # or high event loop load.
            # Check if we overflowed longer than a single call-time.
            missed_wakeups, leftover_time = divmod(
                current_time - this_wakeup, 1 / self.rate)

        # Attempt to wake up only the missed wakeups and ones that were
        # inserted while we missed the original wakeup.
        to_wakeup = min(int(missed_wakeups)+1, self.max_burst)

        while to_wakeup and self._waiters:
            waiter = self._waiters.popleft()
            if waiter.done():  # Might have been cancelled.
                continue
            waiter.set_result(None)
            to_wakeup -= 1

        # All of the waiters were cancelled or we missed wakeups and we're out
        # of waiters. Free to accept traffic.
        if to_wakeup:
            _unlock()

        # If we still have waiters, we need to schedule the next wakeup.
        # If we're out of waiters we still need to wait before
        # unlocking in case a new waiter comes in, as we just
        # let a call through.
        else:
            self._schedule_wakeup(
                at=current_time + 1 / self.rate - leftover_time, _loop=loop)


class LeakyBucketLimiter(_CommonLimiterMixin):
    """Leaky bucket compliant with bursts."""
    rate: float
    capacity: int

    def __init__(self, rate: float, *, capacity: int = 10) -> None:
        """Create a new limiter.

        Args:
            rate: The rate (calls per second) at which calls can pass through
            (or bucket drips).
            capacity: The capacity of the bucket. At full capacity calls to
            wait() will block until the bucket drips.
        """
        super().__init__()
        self.rate = rate
        self.capacity = capacity
        self._level = 0

    def _maybe_lock(self):
        self._level += 1

        if self._wakeup_handle is None:
            self._schedule_wakeup()

        if self._level >= self.capacity:
            self._locked = True
            return

    def _schedule_wakeup(self, at: float | None = None, *, _loop=None) -> None:
        """Schedule the next wakeup to be unlocked."""
        loop = _loop or _asyncio.get_running_loop()
        if at is None:
            at = loop.time() + 1 / self.rate
        self._wakeup_handle = loop.call_at(at, self._wakeup)
        self._next_wakeup = at

    def reset(self) -> None:
        """Reset the limiter.

        This will cancel all calls and reset the bucket to its initial state.
        """
        super().reset()
        self._level = 0

    def _wakeup(self) -> None:
        loop = _asyncio.get_running_loop()
        this_wakeup = self._next_wakeup
        current_time = loop.time()

        # We woke up early. Damn event loop!
        if current_time < this_wakeup:
            missed_drains = 0
            # We have a negative leftover bois. Increase the next sleep!
            leftover_time = current_time - this_wakeup
            # More than 1 tick early. Great success.
            # Technically the higher the rate, the more likely the event loop
            # should be late. If we came early on 2 ticks, that's really bad.
            assert -leftover_time < 1/self.rate, (
                f"Event loop is too fast. Woke up {-leftover_time*self.rate} "
                f"ticks early.")

        else:
            # We woke up too late!
            # Missed wakeups can happen in case of heavy CPU-bound activity,
            # or high event loop load.
            # Check if we overflowed longer than a single call-time.
            missed_drains, leftover_time = divmod(
                current_time - this_wakeup, 1 / self.rate)

        capacity = self.capacity
        level = self._level
        # There are no waiters if level is not == capacity.
        # We can decrease without accounting for current level.
        level = max(0, level - missed_drains - 1)
        while level < capacity and (
                waiter := _pop_pending(self._waiters)) is not None:
            waiter.set_result(None)
            level += 1

        # We have no more waiters
        if level < capacity:
            self._locked = False
            self._level = level
            if level == 0:
                return

        time_to_next_drain = 1 / self.rate - leftover_time
        self._schedule_wakeup(at=current_time + time_to_next_drain)


class StrictLimiter(_CommonLimiterMixin):
    """At MOST rate. (Removing all backlogged)"""
    rate: float

    def __init__(self, rate: float) -> None:
        """Create a new limiter.

        Args:
            rate: The rate (calls per second) at which calls can pass through.
        """
        super().__init__()
        self.rate = rate

    def _maybe_lock(self):
        self._locked = True
        self._schedule_wakeup()

    def _schedule_wakeup(self):
        loop = _asyncio.get_running_loop()
        self._wakeup_handle = loop.call_at(
            loop.time() + 1 / self.rate, self._wakeup)

    def _wakeup(self):
        waiter = _pop_pending(self._waiters)
        if waiter is not None:
            waiter.set_result(None)
            self._schedule_wakeup()
        else:
            self._locked = False
            self._wakeup_handle = None
