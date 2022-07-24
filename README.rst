# aioratelimiter
A simple Python AsyncIO rate limiter.

## Installation
Install the plugin using `pip install aioratelimiter`.

## Sample Usage

    # Limit to 10 requests per 5 second (equiv to 2 requests per second)
    >>> limiter = aioratelimiter.Limiter(10/5)
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

## Available Limiter flavors

    - `Limiter`: Limits by requests per second and takes into account CPU heavy
    tasks or other delays that can occur while the process is sleeping.
    - `LeakyBucketLimiter`: Limits by requests per second according to the
    leaky bucket algorithm. Has a maximum capacity and an initial burst of
    requests.
    - `StrictLimiter`: Limits by requests per second, without taking CPU or other
    process sleeps into account. There are no bursts and the resulting rate will
    always be a less than the set limit.

## Documentation

    Full documentation available at Read the Docs.

## License

Licensed under the MIT License.

