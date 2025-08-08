import asyncio
import time
import functools
import logging
import random

def retry_with_backoff(max_attempts=5, base_delay=1, exceptions=(Exception,), logger: logging.Logger = None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    if logger:
                        logger.warning(f"Retry {attempt + 1}/{max_attempts} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
            if logger:
                logger.error(f"Operation failed after {max_attempts} attempts.")
            raise RuntimeError(f"Operation failed after {max_attempts} attempts.")

        return wrapper

    return decorator


def async_retry_with_backoff(max_attempts=5, base_delay=1, exceptions=(Exception,), logger: logging.Logger = None):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    if logger:
                        logger.warning(f"Retry {attempt + 1}/{max_attempts} failed: {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
            if logger:
                logger.error(f"Operation failed after {max_attempts} attempts.")
            raise RuntimeError(f"Operation failed after {max_attempts} attempts.")

        return wrapper

    return decorator
