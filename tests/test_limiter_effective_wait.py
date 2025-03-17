import asyncio as asyncio
import signal
import time
from contextlib import contextmanager
from typing import Any, Awaitable, Union

from asynciolimiter import (
    LeakyBucketLimiter,
    Limiter,
    StrictLimiter,
    _CommonLimiterMixin,
)


class FastFunc:
    def __init__(self, limiter: _CommonLimiterMixin) -> None:
        self.calls = 0
        self.limiter = limiter
        assert limiter
        self.queue: asyncio.Queue[Union[Any, Awaitable[Any]]] = asyncio.Queue()

    async def perform_quick_call(self) -> None:
        await asyncio.sleep(0.1)
        self.calls += 1


STOP_FETCHING = object()


async def consume_updates(
    rate_limiter: _CommonLimiterMixin,
    q: asyncio.Queue[Union[object, Awaitable[Any]]],
) -> None:
    to_await = await q.get()
    while to_await is not STOP_FETCHING:
        assert isinstance(to_await, Awaitable)
        await to_await
        q.task_done()
        await rate_limiter.wait()
        to_await = await q.get()
    q.task_done()


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


async def perform_test(
    to_be_called: FastFunc, num_calls: int, expected_duration: float
) -> None:
    start = time.time()

    async def to_call():
        await to_be_called.perform_quick_call()

    for _i in range(num_calls):
        await to_be_called.queue.put(to_call())
    await to_be_called.queue.put(STOP_FETCHING)

    await asyncio.gather(
        asyncio.create_task(
            consume_updates(
                rate_limiter=to_be_called.limiter, q=to_be_called.queue
            )
        ),
        return_exceptions=True,
    )
    duration = time.time() - start
    assert to_be_called.calls == num_calls
    # More or less 0.2 sec, sounds reasonable
    assert (
        abs(duration - expected_duration) < 0.5
    ), f"duration was {duration}s, but expected around {expected_duration}s"


def test_LeakyBucketLimiterWorking():
    # For some reason this is working
    num_calls = 6

    async def my_test() -> None:
        to_be_called = FastFunc(LeakyBucketLimiter(num_calls, capacity=1))
        await perform_test(
            to_be_called, num_calls=num_calls, expected_duration=1.0
        )

    asyncio.run(my_test())


def test_LeakyBucketLimiterBroken():
    # For some reason, this statically fails
    num_calls = 9
    with time_limit(3):

        async def my_test() -> None:
            to_be_called = FastFunc(LeakyBucketLimiter(num_calls, capacity=1))
            await perform_test(
                to_be_called, num_calls=num_calls, expected_duration=1.0
            )

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
