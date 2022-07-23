import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, Mock, ANY
from aioratelimiter import Limiter, StrictLimiter, LeakyBucketLimiter
import aioratelimiter
from types import SimpleNamespace


class PatchLoopMixin:    
    """Patch the loop scheduling functions"""
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        asyncio_mock = Mock(wraps=asyncio)
        patcher = patch("aioratelimiter.asyncio", asyncio_mock)
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
    limiter: aioratelimiter._BaseLimiter

    def setUp(self) -> None:
        self.waiters_finished = 0
        self.waiters = []
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

class LimiterTestCase(CommonTestsMixin, PatchLoopMixin, IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.limiter = Limiter(1/3)

    async def test_wait(self):
        await self.limiter.wait()
        self.loop.call_at.assert_called_once_with(3, ANY)

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
    
    async def test_wait_multiple_max_burst(self):
        self.limiter.max_burst = 3
        for i in range(5):
            self.add_waiter()
        await self.advance_loop()
        self.assert_call_at(3)
        self.assert_finished(1)
        self.set_time(3*10 + 1)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(4)
        self.assert_call_at(3*11)
        self.set_time(3*11+2)
        self.call_wakeup()
        await self.advance_loop()
        self.assert_finished(5)
        self.assert_call_at(3*12)
        self.set_time(3*12)
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




class StrictLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    pass

class LeakyBucketLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    pass
