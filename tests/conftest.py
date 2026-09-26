import pytest

from amazon_ads_pipeline.backoff import RetryPolicy


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def fast_retry(sleeps: list[float]) -> RetryPolicy:
    """The default policy, recording its delays instead of sleeping (no jitter)."""
    return RetryPolicy(sleep=sleeps.append, jitter=lambda delay: delay)
