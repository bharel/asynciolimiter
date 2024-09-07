# asynciolimiter
A simple yet efficient Python AsyncIO rate limiter.

[![GitHub branch checks state](https://img.shields.io/github/checks-status/bharel/asynciolimiter/master)](https://github.com/bharel/asynciolimiter/actions)
[![PyPI](https://img.shields.io/pypi/v/asynciolimiter)](https://pypi.org/project/asynciolimiter/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/asynciolimiter)](https://pypi.org/project/asynciolimiter/)
[![codecov](https://codecov.io/gh/bharel/asynciolimiter/branch/master/graph/badge.svg?token=BJBL909NH3)](https://codecov.io/gh/bharel/asynciolimiter)

## Installation
`pip install asynciolimiter`

## Sample Usage

```python
# Limit to 10 requests per 5 second (equiv to 2 requests per second)
>>> limiter = asynciolimiter.Limiter(10/5)
>>> async def main():
...     await limiter.wait() # Wait for a slot to be available.
...     pass # do stuff

>>> limiter = Limiter(1/3)
>>> async def request():
...     await limiter.wait()
...     print("Request")  # Do stuff
...
>>> async def main():
...     # Schedule 1 request every 3 seconds.
...     await asyncio.gather(*(request() for _ in range(10)))
```

## Available Limiter flavors

- `Limiter`: Limits by requests per second and takes into account CPU heavy
    tasks or other delays that can occur while the process is sleeping.
- `LeakyBucketLimiter`: Limits by requests per second according to the
    [leaky bucket algorithm](https://en.wikipedia.org/wiki/Leaky_bucket). Has a maximum capacity and an initial burst of
    requests.
- `StrictLimiter`: Limits by requests per second, without taking CPU or other
    process sleeps into account. There are no bursts and the resulting rate will
    always be a less than the set limit.

## Documentation

Full documentation available on [Read the Docs](https://asynciolimiter.readthedocs.io/en/latest/).

## License

Licensed under the MIT License.

## Contribution
See [contributing.md](CONTRIBUTING.md).
