import random


def calculate_backoff(
    retry_number: int,
    base_delay: float = 1.0,
) -> float:
    exponential_delay = base_delay * (2 ** retry_number)
    jitter = random.uniform(0, 0.99)

    return exponential_delay + jitter