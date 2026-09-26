"""Amazon Ads API client for the reporting v3 flow: profiles, create report, poll, download."""

from __future__ import annotations

import gzip
import json
import logging
import re
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from amazon_ads_pipeline.auth import AuthError, LwaTokenProvider, TokenProvider
from amazon_ads_pipeline.backoff import RetryPolicy
from amazon_ads_pipeline.regions import Region

log = logging.getLogger(__name__)

CREATE_REPORT_MEDIA_TYPE = "application/vnd.createasyncreportrequest.v3+json"
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
GZIP_MAGIC = b"\x1f\x8b"
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)


class AmazonAdsApiError(RuntimeError):
    def __init__(self, status_code: int, detail: str, request_id: str | None = None) -> None:
        super().__init__(
            f"Amazon Ads API returned {status_code}: {detail} (request id {request_id})"
        )
        self.status_code = status_code
        self.detail = detail
        self.request_id = request_id

    @classmethod
    def from_response(cls, response: httpx.Response) -> AmazonAdsApiError:
        try:
            payload = response.json()
            detail = payload.get("detail") or payload.get("message") or response.text
        except ValueError:
            detail = response.text
        return cls(
            response.status_code, str(detail)[:500], response.headers.get("x-amzn-RequestId")
        )


def retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse Retry-After (seconds or an HTTP date). Amazon does not always send it."""
    value = response.headers.get("Retry-After")
    if not value:
        return None
    if value.strip().isdigit():
        return float(value)
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (moment - datetime.now(timezone.utc)).total_seconds())


Headers = Mapping[str, str] | Callable[[], Mapping[str, str]]


class AmazonAdsClient:
    """One region's API host, authenticated with Login with Amazon tokens.

    Every call is retried on network errors, 429 and 5xx: after the server's Retry-After
    when it sends one, otherwise with exponential backoff. A 401 triggers one token refresh.
    """

    def __init__(
        self,
        *,
        region: Region,
        client_id: str,
        tokens: TokenProvider,
        http: httpx.Client,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.region = region
        self._client_id = client_id
        self._tokens = tokens
        self._http = http
        self._retry = retry or RetryPolicy()

    @classmethod
    def create(
        cls,
        *,
        region: Region,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        http: httpx.Client | None = None,
        retry: RetryPolicy | None = None,
    ) -> AmazonAdsClient:
        http = http or httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0))
        tokens = LwaTokenProvider(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            token_url=region.token_url,
            http=http,
        )
        return cls(region=region, client_id=client_id, tokens=tokens, http=http, retry=retry)

    def list_profiles(self) -> list[dict[str, Any]]:
        """Advertiser profiles in this region that the refresh token can access."""
        return self._api("GET", "/v2/profiles").json()

    def create_report(self, profile_id: int, body: Mapping[str, Any]) -> str:
        """Request a report and return its reportId.

        Amazon answers 425 when the same report is already being generated. The existing
        report's ID is in the message, so the run carries on with that report.
        """
        try:
            response = self._api(
                "POST",
                "/reporting/reports",
                profile_id=profile_id,
                body=body,
                content_type=CREATE_REPORT_MEDIA_TYPE,
            )
        except AmazonAdsApiError as error:
            duplicate = _UUID.search(error.detail) if error.status_code == 425 else None
            if duplicate is None:
                raise
            log.info("Report is a duplicate of %s, still processing; reusing it", duplicate[0])
            return duplicate[0]
        return response.json()["reportId"]

    def get_report(self, profile_id: int, report_id: str) -> dict[str, Any]:
        """Report status: PENDING, PROCESSING, COMPLETED (with a download url) or FAILED."""
        return self._api("GET", f"/reporting/reports/{report_id}", profile_id=profile_id).json()

    def download_report(self, url: str) -> list[dict[str, Any]]:
        """Download a GZIP_JSON report. Money stays exact: JSON decimals become Decimal."""
        # The url is a pre-signed S3 link: sending the API's Authorization header breaks it.
        response = self._send("GET", url, headers={}, refresh_on_401=False)
        body = response.content
        # The file is gzipped JSON. If the object is served with Content-Encoding: gzip,
        # httpx has already decompressed it, so only gunzip what still is gzip.
        if body[:2] == GZIP_MAGIC:
            body = gzip.decompress(body)
        return json.loads(body, parse_float=Decimal)

    def _api(
        self,
        method: str,
        path: str,
        *,
        profile_id: int | None = None,
        body: Mapping[str, Any] | None = None,
        content_type: str | None = None,
    ) -> httpx.Response:
        def headers() -> dict[str, str]:
            values = {
                "Amazon-Advertising-API-ClientId": self._client_id,
                "Authorization": f"Bearer {self._tokens.token()}",
            }
            if profile_id is not None:
                values["Amazon-Advertising-API-Scope"] = str(profile_id)
            if content_type is not None:
                values["Content-Type"] = content_type
            return values

        content = None if body is None else json.dumps(body).encode()
        return self._send(method, self.region.api_url + path, headers=headers, content=content)

    def _send(
        self,
        method: str,
        url: str,
        *,
        headers: Headers,
        content: bytes | None = None,
        refresh_on_401: bool = True,
    ) -> httpx.Response:
        loggable_url = url.split("?")[0]  # pre-signed URL signatures stay out of the logs
        attempt = 1
        refreshed = False
        while True:
            try:
                resolved = headers() if callable(headers) else headers
                response = self._http.request(method, url, headers=resolved, content=content)
            except (httpx.TransportError, AuthError) as error:
                if isinstance(error, AuthError) and not error.transient:
                    raise
                if attempt >= self._retry.max_attempts:
                    raise
                self._wait(attempt, None, f"{method} {loggable_url} failed ({error!r})")
                attempt += 1
                continue

            if response.status_code == 401 and refresh_on_401 and not refreshed:
                log.info("Access token rejected; refreshing it once")
                self._tokens.invalidate()
                refreshed = True
                continue
            if response.status_code in RETRYABLE_STATUS and attempt < self._retry.max_attempts:
                self._wait(
                    attempt,
                    retry_after_seconds(response),
                    f"{method} {loggable_url} returned {response.status_code}",
                )
                attempt += 1
                continue
            if response.is_error:
                raise AmazonAdsApiError.from_response(response)
            return response

    def _wait(self, attempt: int, retry_after: float | None, reason: str) -> None:
        delay = self._retry.delay(attempt, retry_after)
        log.warning(
            "%s; attempt %d of %d, retrying in %.1fs",
            reason,
            attempt,
            self._retry.max_attempts,
            delay,
        )
        self._retry.sleep(delay)
