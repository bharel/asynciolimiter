import asyncio
from typing import Awaitable, List
from unittest import IsolatedAsyncioTestCase, skipUnless
from unittest.mock import patch, Mock, ANY
from asynciolimiter import Limiter, StrictLimiter, LeakyBucketLimiter
import asynciolimiter
from types import SimpleNamespace


class PatchLoopMixin(IsolatedAsyncioTestCase):
    """Patch the loop scheduling functions"""
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        asyncio_mock = Mock(wraps=asyncio)
        patcher = patch("asynciolimiter._asyncio", asyncio_mock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.loop = SimpleNamespace()
        asyncio_mock.get_running_loop.return_value = self.loop
        self.loop.time = Mock(return_value=0)
        self.timer_handler = Mock()
        self.loop.call_at = Mock(return_value=self.timer_handler)
        real_loop = asyncio.get_running_loop()
        self.loop.create_future = real_loop.create_future

    def get_scheduled_functions(self):
        return [call[0][1] for call in self.loop.call_at.call_args_list]

    def get_scheduled_function(self):
        return self.get_scheduled_functions()[-1]


class CommonTestsMixin(PatchLoopMixin, IsolatedAsyncioTestCase):
    limiter: asynciolimiter._BaseLimiter

    def setUp(self) -> None:
        self.waiters_finished = 0
        self.waiters: List[Awaitable] = []
        return super().setUp()

    def call_wakeup(self):
        self.get_scheduled_function()()

    def add_waiter(self):
        def cb(fut):
            self.waiters_finished += 1
        task = asyncio.create_task(self.limiter.wait())
        task.add_done_callback(cb)
        self.waiters.append(task)

    def set_time(self, time: float):
        self.loop.time.return_value = time

    def assert_call_at(self, time: float):
        self.assertEqual(self.loop.call_at.call_args_list[-1][0][0], time)

    async def advance_loop(self, count: int = 5):
        for _ in range(count):
            await asyncio.sleep(0)

    def assert_finished(self, count: int):
        self.assertEqual(self.waiters_finished, count)


class LimiterTestCase(CommonTestsMixin, PatchLoopMixin,
                      IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.limiter = Limiter(1 / 3, max_burst=3)

    async def test_wait(self):
        await self.limiter.wait()
        self.loop.call_at.assert_called_once_with(3, ANY)

    async def test_rate_setter(self):
        self.limiter.rate = 1 / 2
        self.assertEqual(self.limiter.rate, 1 / 2)
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(2)

    async def test_max_burst_setter(self):
        self.limiter.max_burst = 10
        self.assertEqual(self.limiter.max_burst, 10)

    async def test_repr(self):
        self.assertEqual(eval(repr(self.limiter)).rate, self.limiter.rate)

    async def test_wait_multiple(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(2)
        await self.advance_loop()
        self.assert_call_at(6)
        self.set_time(6)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(3)

    async def test_wait_multiple_cpu_heavy(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.set_time(8)  # Two were supposed to run
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(3)
        self.assert_call_at(9)
        self.set_time(9)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(4)

    async def test_early_wakeups(self):
        """Event loop can sometimes wake us too early!

        Expected behavior is to send it a the current pending request a bit
        earlier (only a few microseconds usually) but delay the next one but
        the complementing factor. Should round to a correct wakeup time.
        """
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.set_time(2)  # Beh.
        self.call_wakeup()  # Wakey wakey!
        await self.advance_loop()
        self.assert_finished(2)
        self.assert_call_at(6)

    @skipUnless(__debug__, "Debug mode only")
    async def test_too_early_wakeups(self):
        """When the wakeup is way too early. Should never happen.

        Fails only on __debug__. Attempts to recover in real time.
        """
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.set_time(3)  # So far so good
        self.call_wakeup()
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(2)
        self.assert_call_at(6)
        self.set_time(2)  # Time just went backwards.
        with self.assertRaises(AssertionError):
            self.call_wakeup()

    async def test_wait_multiple_max_burst(self):
        for i in range(5):
            self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.set_time(3 * 10 + 1)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(4)
        self.assert_call_at(3 * 11)
        self.set_time(3 * 11 + 2)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(5)
        self.assert_call_at(3 * 12)
        self.set_time(3 * 12)
        self.call_wakeup()  # Unlocke the limiter
        await self.advance_loop()
        self.assert_finished(5)
        self.add_waiter()  # Unlocked, should immediately finish
        await self.advance_loop()
        self.assert_finished(6)

    async def test_cancelled_waiters(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.waiters[1].cancel()
        self.waiters[2].cancel()
        self.add_waiter()
        await self.advance_loop()
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(4)

    async def test_cancel(self):
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.limiter.cancel()
        await self.advance_loop()
        assert self.waiters[1].cancelled()

    async def test_breach(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        # Make sure one is cancelled to test breach handing done futures.
        self.waiters[-1].cancel()
        await self.advance_loop(2)
        self.limiter.breach()
        await self.advance_loop()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(6)

    async def test_reset(self):
        self.limiter.breach()
        self.limiter.reset()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(1)  # Not breached
        self.limiter.reset()  # Reset cancells all waiters
        await self.advance_loop()
        assert self.waiters[-1].cancelled()
        # Handler cancelled
        self.loop.call_at.return_value.cancel.assert_called_once_with()

    async def test_wrap(self):
        async def coro():
            return 123
        task = asyncio.create_task(self.limiter.wrap(coro()))
        await self.advance_loop()
        self.assertEqual((await task), 123)
        task = asyncio.create_task(self.limiter.wrap(coro()))
        await self.advance_loop()
        self.assertFalse(task.done())
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.assertEqual((await task), 123)

    async def test_close(self):
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.limiter.close()
        await self.advance_loop()
        assert self.waiters[-1].cancelled()
        # Handler cancelled
        self.loop.call_at.return_value.cancel.assert_called_once_with()

    async def test_waiters_cancelled_unlock(self):
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.waiters[1].cancel()
        self.set_time(3)
        self.call_wakeup()
        self.add_waiter()
        # No more wakeups, no waiting on wait() due to unlock
        await self.advance_loop()
        self.assert_finished(3)


class StrictLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.limiter = StrictLimiter(1 / 3)

    async def test_repr(self):
        self.assertEqual(eval(repr(self.limiter)).rate, self.limiter.rate)

    async def test_wait(self):
        self.add_waiter()
        await self.advance_loop()
        self.loop.call_at.assert_called_once_with(3, ANY)
        self.set_time(3)
        self.call_wakeup()
        # Unlocked.
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(2)

    async def test_wait_multiple(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.set_time(3 * 10 + 1)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(2)
        # Whether 1 second CPU delay or not, always schedule 3 seconds
        # afterwards
        self.assert_call_at(3 * 11 + 1)
        self.set_time(3 * 11 + 2)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(3)


class LeakyBucketLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.limiter = LeakyBucketLimiter(1 / 3, capacity=3)

    def test_repr(self):
        self.assertEqual(eval(repr(self.limiter)).__dict__,
                         self.limiter.__dict__)

    async def test_rate_setter(self):
        self.limiter.rate = 1 / 2
        self.assertEqual(self.limiter.rate, 1 / 2)
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(2)

    async def test_wait(self):
        await self.limiter.wait()
        self.loop.call_at.assert_called_once_with(3, ANY)
        self.set_time(3)
        self.call_wakeup()
        # Should be empty
        await self.advance_loop()
        # Wasn't rescheduled.
        self.loop.call_at.called_once()

    async def test_wait_multiple(self):
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.waiters[-1].cancel()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(4)
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_call_at(3 * 2)
        self.assert_finished(5)
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(5)  # Still blocked, bucket hasn't drained
        self.add_waiter()
        await self.advance_loop()
        # Bucket drained twice, with 1 second to spare
        self.set_time(3 * 3 + 1)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(7)
        self.assert_call_at(3 * 4)
        self.set_time(3 * 10)  # Bucket fully drained
        self.call_wakeup()
        # Make sure it didn't underflow
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_finished(10)  # Last one is queued on a full bucket.
        self.waiters[-1].cancel()

    async def test_wait_max_burst(self):
        for i in range(10):
            self.add_waiter()
        await self.advance_loop()
        self.assert_finished(3)
        # Very slow CPU operation. So slow the bucket was supposed to be
        # emptied twice
        self.set_time(3 * 10)
        self.call_wakeup()
        await self.advance_loop()
        # Bucket did not empty twice. We kept max to the capacity.
        self.assert_finished(6)
        self.assert_call_at(3 * 11)
        self.set_time(3 * 11)
        self.call_wakeup()
        await self.advance_loop()
        # Continued draining from full.
        self.assert_finished(7)
        self.limiter.cancel()

    async def test_bucket_reset(self):
        for i in range(2):
            for i in range(4):
                self.add_waiter()
            await self.advance_loop()
            self.limiter.reset()
        await self.advance_loop()

        # 8 finished, out of them 2 due to reset.
        self.assert_finished(8)
        self.assertEqual(sum(fut.cancelled() for fut in self.waiters), 2)

    async def test_bucket_empty(self):
        """Doesn't reschedule when empty"""
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.loop.call_at.assert_called_once()  # Wasn't called again.

    async def test_bucket_drain_once(self):
        """Drains the bucket once, make sure it reschedules for next."""
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.set_time(3)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_call_at(3 * 2)

    async def test_early_wakeups(self):
        """Event loop can sometimes wake us too early!

        Expected behavior is to drain a bit earlier (only a few microseconds
        usually) but delay the next one but the complementing factor. Should
        round to a correct drain time.
        """
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.set_time(2)  # Beh.
        self.call_wakeup()  # Wakey wakey!
        await self.advance_loop()
        self.assert_finished(2)
        self.assert_call_at(6)

    @skipUnless(__debug__, "Debug mode only")
    async def test_too_early_wakeups(self):
        """When the wakeup is way too early. Should never happen.

        Fails only on __debug__. Attempts to recover in real time.
        """
        self.add_waiter()
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.set_time(3)  # So far so good
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(2)
        self.assert_call_at(6)
        self.set_time(2)  # Time just went backwards.
        with self.assertRaises(AssertionError):
            self.call_wakeup()
