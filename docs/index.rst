.. You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aioratelimiter - An AsyncIO Rate Limiter for Python
----------------------------------------------------

test
----

.. toctree::
   :maxdepth: 2
   :caption: Contents:


This module provides different rate limiters for asyncio.

    - :class:`Limiter`: Limits by requests per second and takes into account
        CPU heavy tasks or other delays that can occur while the
        process is sleeping.
    - :class:`LeakyBucketLimiter`: Limits by requests per second according to the
        leaky bucket algorithm. Has a maximum
        capacity and an initial burst of requests.
    - :class:`StrictLimiter`: Limits by requests per second, without taking CPU or
        other process sleeps into account. There are no bursts and
        the resulting rate will always be a less than the set limit.

Example usage:
================

.. code::
    
    import asyncio
    from aioratelimiter import Limiter

    # Limit to 10 requests per 5 second (equiv to 2 requests per second)
    rate_limiter = Limiter(10/5)

    async def request():
        await rate_limiter.wait() # Wait for a slot to be available.
        print("hello world") # do stuff

    async def main():
        await asyncio.gather(*(request() for _ in range(10)))

    asyncio.run(main())


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
