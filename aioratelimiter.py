import asyncio
from collections import deque
from collections.abc import Awaitable
import functools
from typing import TypeVar
from abc import ABC, abstractmethod

__all__ = ['Limiter', 'StrictLimiter', 'LeakyBucketLimiter']

_T = TypeVar('_T')

def _pop_not_done(waiters: deque[asyncio.Future]) -> asyncio.Future | None:
    """Pop until the first non done future is found and return it.
    
    If all futures are done, or deque is empty, return None.
    """
    while waiters:
        waiter = waiters.popleft()
        if not waiter.done():
            return waiter
    return None


class _BaseLimiter(ABC):
    @abstractmethod
    async def wait(self) -> None:
        pass
    @abstractmethod
    async def cancel(self) -> None:
        pass
    @abstractmethod
    async def breach(self) -> None:
        """Let all calls through"""
        pass

    def wrap(self, coro: Awaitable[_T]) -> Awaitable[_T]:
        """Wrap a coroutine with the limiter.
        
        This will wait for the limiter to be unlocked, and then schedule the
        coroutine.
        """
        @functools.wraps(coro)
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
        self._waiters = deque()
        self._wakeup_handle: asyncio.TimerHandle = None
        self._breached = False

    async def wait(self) -> None:
        """Wait for the limiter to be unlocked."""
        if self._breached:
            return

        if not self._locked:
            self._maybe_lock()
            return
        fut = asyncio.get_running_loop().create_future()
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

    @abstractmethod
    def _maybe_lock(self):
        pass

    def __del__(self):
        # No need to touch wakeup, as wakeup holds a strong reference and
        # __del__ won't be called.
        try:
            # Technically this should never happen, where there are waiters
            # without a wakeup scheduled. Means there was a bug in the code.
            waiters = self._waiters

        # Error during initialization before _waiters exists.
        except AttributeError:  # pragma: no cover
            return

        for fut in waiters:  # pragma: no cover
            if not fut.done():
                fut.cancel()
        
        # Alert for the bug.
        assert not waiters, "__del__ was called with waiters still waiting"
    
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
        loop = _loop or asyncio.get_running_loop()
        if at is None:
            at = loop.time() + 1 / self.rate
        self._wakeup_handle = loop.call_at(at, self._wakeup)
        self._next_wakeup = at
    
    
    def _wakeup(self) -> None:
        def _unlock() -> None:
            self._wakeup_handle = None
            self._locked = False
            return
        loop = asyncio.get_running_loop()
        waiters = self._waiters
        # Short circuit if there are no waiters
        if not waiters:
            _unlock()
            return

        this_wakeup = self._next_wakeup
        current_time = loop.time()
        # Missed wakeups can happen in case of heavy CPU-bound activity.
        # The scheduled wakeup did not occur in time and overflowed longer than
        # a single call-time.
        missed_wakeups, leftover_time = divmod(
            current_time - this_wakeup, 1 / self.rate)
        assert missed_wakeups.is_integer() and missed_wakeups >= 0

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
                at = current_time + 1 / self.rate - leftover_time, _loop=loop)

class LeakyBucketLimiter(_CommonLimiterMixin):
    """Leaky bucket compliant with bursts."""
    rate: float
    capacity: int
    def __init__(self, rate: float, capacity: int = 10) -> None:
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
        loop = _loop or asyncio.get_running_loop()
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
        loop = asyncio.get_running_loop()
        current_time = loop.time()
        count, leftover_time = divmod(
            current_time - self._next_wakeup, 1 / self.rate)
        count += 1
        while count and (waiter := _pop_not_done(self._waiters)) is not None:
            waiter.set_result(None)
            count -= 1
        
        # We have no more waiters
        if count:
            self._locked = False
            level = self._level = max(0, self._level - count)
            if level == 0:
                return

        self._schedule_wakeup(current_time + 1 / self.rate - leftover_time)


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
        loop = asyncio.get_running_loop()
        self._wakeup_handle = loop.call_at(
            loop.time() + 1 / self.rate, self._wakeup)
        
    def _wakeup(self):
        waiter = _pop_not_done(self._waiters)
        if waiter is not None:
            waiter.set_result(None)
            self._schedule_wakeup()
        else:
            self._locked = False
            self._wakeup_handle = None
