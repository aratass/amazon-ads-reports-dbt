"""Shared test helpers: fixture locations, a scripted HTTP transport and token stubs."""

from __future__ import annotations

import gzip
import importlib.util
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from amazon_ads_pipeline.backoff import RetryPolicy
from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.regions import REGIONS

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "amazon_ads"
CLIENT_ID = "amzn1.application-oa2-client.demo"
US_PROFILE, UK_PROFILE, AGENCY_PROFILE = 3000000000000001, 3000000000000002, 3000000000000009


def api_sample(name: str) -> dict[str, Any]:
    """A documented-shape API response from tests/fixtures/amazon_ads/api."""
    return json.loads((FIXTURES / "api" / f"{name}.json").read_text())


def gzip_json(payload: Any) -> bytes:
    return gzip.compress(json.dumps(payload).encode())


class StubTokens:
    """Hands out token-1, token-2, ... ; a new one after each invalidate()."""

    def __init__(self) -> None:
        self.issued = 1

    def token(self) -> str:
        return f"token-{self.issued}"

    def invalidate(self) -> None:
        self.issued += 1


class Script:
    """An httpx transport that answers from a list and records every request."""

    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self._responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
    retry: RetryPolicy,
    region: str = "NA",
    tokens: StubTokens | None = None,
) -> AmazonAdsClient:
    return AmazonAdsClient(
        region=REGIONS[region],
        client_id=CLIENT_ID,
        tokens=tokens or StubTokens(),
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        retry=retry,
    )


def load_script(name: str):
    """Import a file from scripts/ as a module."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look their module up while being created
    spec.loader.exec_module(module)
    return module
