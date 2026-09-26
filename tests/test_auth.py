from urllib.parse import parse_qs

import httpx
import pytest

from amazon_ads_pipeline.auth import AuthError, LwaTokenProvider
from amazon_ads_pipeline.backoff import RetryPolicy
from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.regions import REGIONS

from support import CLIENT_ID, Script, api_sample


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def provider(script: Script, clock: Clock | None = None) -> LwaTokenProvider:
    return LwaTokenProvider(
        client_id=CLIENT_ID,
        client_secret="secret",
        refresh_token="Atzr|refresh",
        token_url=REGIONS["EU"].token_url,
        http=httpx.Client(transport=httpx.MockTransport(script)),
        clock=clock or Clock(),
    )


def token_response(access_token: str = "Atza|first", expires_in: int = 3600) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            **api_sample("token_response"),
            "access_token": access_token,
            "expires_in": expires_in,
        },
    )


def test_refresh_token_grant_is_posted_to_the_regional_endpoint() -> None:
    script = Script(token_response())
    assert provider(script).token() == "Atza|first"

    [request] = script.requests
    assert str(request.url) == "https://api.amazon.co.uk/auth/o2/token"
    assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["refresh_token"],
        "refresh_token": ["Atzr|refresh"],
        "client_id": [CLIENT_ID],
        "client_secret": ["secret"],
    }


def test_token_is_reused_until_a_minute_before_expiry() -> None:
    clock = Clock()
    script = Script(token_response("Atza|first"), token_response("Atza|second"))
    tokens = provider(script, clock)

    assert tokens.token() == "Atza|first"
    clock.now += 3600 - 61
    assert tokens.token() == "Atza|first"
    clock.now += 2  # now inside the one-minute safety margin
    assert tokens.token() == "Atza|second"
    assert len(script.requests) == 2


def test_invalidate_forces_a_new_token() -> None:
    script = Script(token_response("Atza|first"), token_response("Atza|second"))
    tokens = provider(script)
    tokens.token()
    tokens.invalidate()
    assert tokens.token() == "Atza|second"


def test_a_revoked_refresh_token_is_a_clear_permanent_error() -> None:
    script = Script(
        httpx.Response(
            400,
            json={"error": "invalid_grant", "error_description": "The refresh token is invalid"},
        )
    )
    with pytest.raises(AuthError, match="invalid_grant: The refresh token is invalid") as error:
        provider(script).token()
    assert not error.value.transient


def test_an_lwa_outage_is_retried_by_the_client(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    script = Script(
        httpx.Response(503, text="Service Unavailable"),
        token_response(),
        httpx.Response(200, json=[]),
    )
    http = httpx.Client(transport=httpx.MockTransport(script))
    client = AmazonAdsClient.create(
        region=REGIONS["NA"],
        client_id=CLIENT_ID,
        client_secret="secret",
        refresh_token="Atzr|refresh",
        http=http,
        retry=fast_retry,
    )
    assert client.list_profiles() == []
    assert sleeps == [5.0]
    assert script.requests[-1].headers["Authorization"] == "Bearer Atza|first"
