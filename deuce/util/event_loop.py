import asyncio
import functools


def get_event_loop(func):
    """
    Gets the event loop from asyncio implicitly through a decorator
    """
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(*args, **kwargs))
    return wrap
