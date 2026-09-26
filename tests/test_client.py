import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime

import httpx
import pytest

from amazon_ads_pipeline.backoff import RetryPolicy
from amazon_ads_pipeline.client import (
    CREATE_REPORT_MEDIA_TYPE,
    AmazonAdsApiError,
    retry_after_seconds,
)

from support import CLIENT_ID, Script, StubTokens, api_sample, gzip_json, make_client

BODY = {"name": "test", "startDate": "2026-09-01", "endDate": "2026-09-14", "configuration": {}}
REPORT_ID = "06ba6494-8568-4066-949f-abb6a91c0b9c"


def ok(payload: object) -> httpx.Response:
    return httpx.Response(200, json=payload)


def test_profiles_come_from_the_regional_host(fast_retry: RetryPolicy) -> None:
    script = Script(ok([]))
    make_client(script, fast_retry, region="EU").list_profiles()

    [request] = script.requests
    assert str(request.url) == "https://advertising-api-eu.amazon.com/v2/profiles"
    assert request.headers["Amazon-Advertising-API-ClientId"] == CLIENT_ID
    assert request.headers["Authorization"] == "Bearer token-1"
    assert "Amazon-Advertising-API-Scope" not in request.headers


def test_create_report_sends_the_v3_media_type_and_profile_scope(fast_retry: RetryPolicy) -> None:
    script = Script(ok(api_sample("report_pending")))
    report_id = make_client(script, fast_retry).create_report(3000000000000001, BODY)

    assert report_id == REPORT_ID
    [request] = script.requests
    assert request.method == "POST"
    assert str(request.url) == "https://advertising-api.amazon.com/reporting/reports"
    assert request.headers["Content-Type"] == CREATE_REPORT_MEDIA_TYPE
    assert request.headers["Amazon-Advertising-API-Scope"] == "3000000000000001"
    assert json.loads(request.content) == BODY


def test_report_status_is_read_with_the_profile_scope(fast_retry: RetryPolicy) -> None:
    script = Script(ok(api_sample("report_completed")))
    report = make_client(script, fast_retry).get_report(3000000000000001, REPORT_ID)
    assert report["status"] == "COMPLETED"
    assert report["url"].startswith("https://offline-report-storage-")
    assert script.requests[0].url.path == f"/reporting/reports/{REPORT_ID}"
    assert script.requests[0].headers["Amazon-Advertising-API-Scope"] == "3000000000000001"


def test_429_waits_as_long_as_retry_after_says(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    script = Script(httpx.Response(429, headers={"Retry-After": "7"}), ok([]))
    make_client(script, fast_retry).list_profiles()
    assert sleeps == [7.0]


def test_429_without_retry_after_backs_off_exponentially(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    # Amazon's docs promise Retry-After, but POST /reporting/reports is known to omit it.
    script = Script(httpx.Response(429), httpx.Response(429), ok(api_sample("report_pending")))
    make_client(script, fast_retry).create_report(1, BODY)
    assert sleeps == [5.0, 10.0]


def test_server_and_network_errors_are_retried(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    script = Script(
        httpx.Response(503),
        httpx.ConnectError("connection reset"),
        httpx.Response(502),
        ok([]),
    )
    make_client(script, fast_retry).list_profiles()
    assert sleeps == [5.0, 10.0, 20.0]


def test_retries_stop_after_max_attempts(fast_retry: RetryPolicy, sleeps: list[float]) -> None:
    script = Script(*[httpx.Response(429, json={"code": "429", "detail": "Too Many Requests"})] * 6)
    with pytest.raises(AmazonAdsApiError, match="429") as error:
        make_client(script, fast_retry).list_profiles()
    assert error.value.status_code == 429
    assert len(script.requests) == 6
    assert sleeps == [5.0, 10.0, 20.0, 40.0, 80.0]


def test_client_errors_fail_fast_with_amazons_message(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    script = Script(
        httpx.Response(
            400,
            json={"code": "400", "detail": "Report date is too far in the past"},
            headers={"x-amzn-RequestId": "req-123"},
        )
    )
    with pytest.raises(AmazonAdsApiError, match="too far in the past") as error:
        make_client(script, fast_retry).create_report(1, BODY)
    assert error.value.request_id == "req-123"
    assert sleeps == []


def test_a_rejected_token_is_refreshed_once(fast_retry: RetryPolicy) -> None:
    tokens = StubTokens()
    script = Script(httpx.Response(401), ok([]))
    make_client(script, fast_retry, tokens=tokens).list_profiles()
    assert [r.headers["Authorization"] for r in script.requests] == [
        "Bearer token-1",
        "Bearer token-2",
    ]


def test_a_second_401_is_raised(fast_retry: RetryPolicy) -> None:
    script = Script(httpx.Response(401), httpx.Response(401, json={"detail": "Unauthorized"}))
    with pytest.raises(AmazonAdsApiError, match="401"):
        make_client(script, fast_retry).list_profiles()


def test_a_duplicate_request_reuses_the_report_in_progress(fast_retry: RetryPolicy) -> None:
    script = Script(httpx.Response(425, json=api_sample("duplicate_425")))
    assert make_client(script, fast_retry).create_report(1, BODY) == REPORT_ID


def test_a_425_without_a_report_id_is_raised(fast_retry: RetryPolicy) -> None:
    script = Script(httpx.Response(425, json={"code": "425", "detail": "Too Early"}))
    with pytest.raises(AmazonAdsApiError, match="425"):
        make_client(script, fast_retry).create_report(1, BODY)


def test_download_is_unauthenticated_and_keeps_money_exact(fast_retry: RetryPolicy) -> None:
    records = [{"date": "2026-09-01", "campaignId": 500000000000001, "cost": 8.18, "clicks": 9}]
    script = Script(httpx.Response(200, content=gzip_json(records)))
    url = api_sample("report_completed")["url"]

    downloaded = make_client(script, fast_retry).download_report(url)

    assert downloaded == [
        {"date": "2026-09-01", "campaignId": 500000000000001, "cost": Decimal("8.18"), "clicks": 9}
    ]
    [request] = script.requests
    assert str(request.url) == url
    assert "Authorization" not in request.headers  # S3 rejects a second auth mechanism
    assert "Amazon-Advertising-API-ClientId" not in request.headers


def test_a_download_already_decoded_by_httpx_is_parsed(fast_retry: RetryPolicy) -> None:
    # If S3 serves the file with Content-Encoding: gzip, httpx gunzips it on the way in.
    records = [{"date": "2026-09-01", "cost": 8.18}]
    script = Script(
        httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=gzip_json(records))
    )
    downloaded = make_client(script, fast_retry).download_report(
        api_sample("report_completed")["url"]
    )
    assert downloaded == [{"date": "2026-09-01", "cost": Decimal("8.18")}]


def test_signed_download_urls_stay_out_of_the_logs(
    fast_retry: RetryPolicy, caplog: pytest.LogCaptureFixture
) -> None:
    script = Script(httpx.Response(503), httpx.Response(200, content=gzip_json([])))
    with caplog.at_level(logging.WARNING):
        make_client(script, fast_retry).download_report(api_sample("report_completed")["url"])
    assert "returned 503" in caplog.text
    assert "X-Amz-Signature" not in caplog.text


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("12", 12.0),
        (None, None),
        ("soon", None),
    ],
)
def test_retry_after_seconds(header: str | None, expected: float | None) -> None:
    headers = {"Retry-After": header} if header else {}
    assert retry_after_seconds(httpx.Response(429, headers=headers)) == expected


def test_retry_after_as_an_http_date() -> None:
    moment = datetime.now(timezone.utc) + timedelta(seconds=30)
    response = httpx.Response(429, headers={"Retry-After": format_datetime(moment, usegmt=True)})
    assert 25 <= retry_after_seconds(response) <= 30


def test_an_absurd_retry_after_is_capped(fast_retry: RetryPolicy, sleeps: list[float]) -> None:
    script = Script(httpx.Response(429, headers={"Retry-After": "86400"}), ok([]))
    make_client(script, fast_retry).list_profiles()
    assert sleeps == [900.0]
