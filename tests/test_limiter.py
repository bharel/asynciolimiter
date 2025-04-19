import asyncio
import typing
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, skipUnless
from unittest.mock import ANY, Mock, patch

import asynciolimiter
from asynciolimiter import LeakyBucketLimiter, Limiter, StrictLimiter

if typing.TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable


class PatchLoopMixin(IsolatedAsyncioTestCase):
    """Patches asyncio loop functions for testing time-based behavior."""

    async def asyncSetUp(self) -> None:
        """Set up the mock asyncio loop."""
        await super().asyncSetUp()

        # Mock the asyncio module used by the limiter
        asyncio_mock = Mock(wraps=asyncio)
        patcher = patch("asynciolimiter._asyncio", asyncio_mock)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Create a simple object to act as the mock loop
        self.loop = SimpleNamespace()
        asyncio_mock.get_running_loop.return_value = self.loop

        # Mock loop time and scheduling functions
        self.loop.time = Mock(return_value=0.0)
        self.timer_handler = Mock()  # Mock the handle returned by call_at
        self.loop.call_at = Mock(return_value=self.timer_handler)

        # Use the real loop's create_future for compatibility
        real_loop = asyncio.get_running_loop()
        self.loop.create_future = real_loop.create_future

    def get_scheduled_functions(self) -> list[typing.Callable]:
        """Return a list of functions scheduled with loop.call_at."""
        # call_args_list is a list of tuples, where each tuple is (args, kwargs)
        # For call_at(when, callback, *args), the callback is args[1]
        return [
            mock_call.args[1] for mock_call in self.loop.call_at.call_args_list
        ]

    def get_scheduled_function(self) -> typing.Callable:
        """Return the most recently scheduled function."""
        return self.get_scheduled_functions()[-1]

    def clear_scheduled_functions(self) -> None:
        """Reset the mock for loop.call_at."""
        self.loop.call_at.reset_mock()


class CommonTestsMixin(PatchLoopMixin, IsolatedAsyncioTestCase):
    """Provides common test utilities and assertions for limiter tests."""

    limiter: asynciolimiter._BaseLimiter
    waiters: list[asyncio.Task]
    waiters_finished: int

    def setUp(self) -> None:
        """Initialize common test attributes."""
        super().setUp()
        self.waiters_finished = 0
        self.waiters = []

    def call_wakeup(self) -> None:
        """Execute the most recently scheduled timer callback."""
        # Get the callback function scheduled by the limiter
        scheduled_callback = self.get_scheduled_function()
        # Clear the mock so we can assert future calls correctly
        self.clear_scheduled_functions()
        # Execute the callback (e.g., to release a waiting task)
        scheduled_callback()

    def add_waiter(self) -> None:
        """Create a task that waits on the limiter and track its completion."""

        def _increment_finished_count(_: asyncio.Future) -> None:
            """Callback to count finished waiter tasks."""
            self.waiters_finished += 1

        # Create a task that calls limiter.wait()
        waiter_task = asyncio.create_task(self.limiter.wait())
        # Add a callback to track when the task completes
        waiter_task.add_done_callback(_increment_finished_count)
        self.waiters.append(waiter_task)

    def set_time(self, time: float) -> None:
        """Set the mock loop's current time."""
        self.loop.time.return_value = time

    def assert_call_at(self, expected_time: float) -> None:
        """Assert the time argument of the most recent call_at."""
        # Check the 'when' argument (index 0) of the last call
        last_call_args = self.loop.call_at.call_args_list[-1][0]
        actual_time = last_call_args[0]
        self.assertEqual(
            actual_time,
            expected_time,
            f"Expected call_at({expected_time}), got call_at({actual_time})",
        )

    async def advance_loop(self, iterations: int = 5) -> None:
        """Advance the event loop by yielding control multiple times.

        This allows pending tasks and callbacks (like those scheduled with
        call_at or futures completing) to be processed. Necessary when
        testing interactions involving multiple steps in the event loop.

        Args:
            iterations: Number of times to yield control with asyncio.sleep(0).
        """
        for _ in range(iterations):
            await asyncio.sleep(0)

    def assert_finished(self, expected_count: int) -> None:
        """Assert the number of waiter tasks that have completed."""
        self.assertEqual(
            self.waiters_finished,
            expected_count,
            f"Expected {expected_count} finished waiters, "
            f"found {self.waiters_finished}",
        )


class LimiterTestCase(
    CommonTestsMixin, PatchLoopMixin, IsolatedAsyncioTestCase
):
    def setUp(self):
        super().setUp()
        self.limiter = Limiter(1 / 3, max_burst=3)

    def test_init_keyword_only(self):
        assert Limiter(1.0, max_burst=5).max_burst == 5
        with self.assertRaises(TypeError):
            Limiter(1.0, 5)

    async def test_wait(self):
        await self.limiter.wait()
        self.loop.call_at.assert_called_once_with(3, ANY)

    async def test_rate_setter(self):
        self.limiter.rate = 1 / 2
        self.assertEqual(self.limiter.rate, 1 / 2)
        self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(2)

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
        with self.assertWarns(ResourceWarning):
            self.call_wakeup()

    async def test_wait_multiple_max_burst(self):
        for _ in range(5):
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
        self.assertEqual(
            eval(repr(self.limiter)).__dict__, self.limiter.__dict__
        )

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
        self.loop.call_at.assert_not_called()

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
        for _ in range(10):
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
        for _ in range(2):
            for _ in range(4):
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
        self.loop.call_at.assert_not_called()

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
        with self.assertWarns(ResourceWarning):
            self.call_wakeup()

    async def test_gh17(self):
        """Test for GH-17, not scheduling wakeup after bucket is empty."""
        self.add_waiter()
        await self.advance_loop()
        self.call_wakeup()
        await self.advance_loop()
        # Wakeup called, bucket is now empty again.

        self.add_waiter()
        self.add_waiter()
        self.add_waiter()

        # Bucket is full - this one is blocking in the background.
        self.add_waiter()
        self.set_time(10)
        await self.advance_loop()

        # One is waiting, rest finished
        self.assert_finished(4)

        # Wakeup again. GH-17 did not schedule a new wakeup.
        self.call_wakeup()
        await self.advance_loop()

        # All done
        self.assert_finished(5)
