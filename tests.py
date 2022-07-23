import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, Mock, ANY
from aioratelimiter import Limiter, StrictLimiter, LeakyBucketLimiter

class PatchLoopMixin:    
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        loop = self.loop = asyncio.get_running_loop()
        self.loop_time = Mock()
        self.loop_time.return_value = 0
        self.call_at = Mock()
        patcher = patch.multiple(
            loop, time=self.loop_time, call_at=self.call_at)
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

class CommonTestsMixin(IsolatedAsyncioTestCase):
    pass

class LimiterTestCase(PatchLoopMixin, CommonTestsMixin, IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.limiter = Limiter(1/3)
    

    async def test_wait(self):
        self.loop_time.return_value = 0
        await self.limiter.wait()
        self.call_at.assert_called_once_with(3, ANY)

class StrictLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    pass

class LeakyBucketLimiterTestCase(CommonTestsMixin, IsolatedAsyncioTestCase):
    pass