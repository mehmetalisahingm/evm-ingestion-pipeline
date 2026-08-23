import asyncio


def calculate_backoff(
    attempt: int,
    *,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
) -> float:
    delay = base_delay * (2 ** attempt)

    return min(delay, max_delay)


async def wait_before_retry(
    attempt: int,
) -> None:
    delay = calculate_backoff(attempt)

    await asyncio.sleep(delay)