
import asyncio as asyncio
from typing import Any, Awaitable
import inspect
import time
from asynciolimiter import LeakyBucketLimiter, Limiter, StrictLimiter, _CommonLimiterMixin

class FastFunc:
    def __init__(self, limiter: _CommonLimiterMixin) -> None:
        self.calls = 0
        self.limiter = limiter
        assert limiter
        self.queue: asyncio.Queue[Awaitable[Any]]
    
    async def perform_quick_call(self) -> None:
        await asyncio.sleep(0.1)
        await self.limiter.wait()
        await asyncio.sleep(0.1)
        self.calls += 1
        await asyncio.sleep(0.1)


async def perform_test(to_be_called: FastFunc, num_calls: int, expected_duration: float) -> None:
    start = time.time()
    
    loop = asyncio.get_event_loop()
    futures = []
    
    async def to_call():
        await to_be_called.perform_quick_call()

    for _ in range(num_calls):
        futures.append(asyncio.ensure_future(to_call(), loop=loop))

    await asyncio.gather(*futures)
    duration = time.time() - start
    assert to_be_called.calls == num_calls
    # More or less 0.2 sec, sounds reasonable
    assert abs(duration - expected_duration) < 0.5, f"duration was {duration}s, but expected around {expected_duration}s"

def test_LeakyBucketLimiter():
    async def my_test() -> None:
        to_be_called = FastFunc(LeakyBucketLimiter(300, capacity=3))
        await perform_test(to_be_called, num_calls=300, expected_duration=1.0)
    asyncio.run(my_test())


def test_StrictLimiter():
    async def my_test() -> None:
        to_be_called = FastFunc(StrictLimiter(3))
        await perform_test(to_be_called, num_calls=6, expected_duration=2.0)
    asyncio.run(my_test())


def test_Limiter():
    async def my_test() -> None:
        to_be_called = FastFunc(Limiter(3, max_burst=3))
        await perform_test(to_be_called, num_calls=6, expected_duration=2.0)
    asyncio.run(my_test())