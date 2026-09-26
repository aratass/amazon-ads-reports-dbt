from datetime import date, datetime, timedelta, timezone

import httpx
import pytest

from amazon_ads_pipeline.backoff import RetryPolicy
from amazon_ads_pipeline.cli import main
from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.pipeline import (
    PipelineError,
    PollPolicy,
    ReportTimeout,
    profile_window,
    run,
)
from amazon_ads_pipeline.regions import REGIONS
from amazon_ads_pipeline.replay import FakeAmazonAdsApi, Recorder
from amazon_ads_pipeline.reports import SP_CAMPAIGNS
from amazon_ads_pipeline.warehouse import DuckDBWarehouse

from support import AGENCY_PROFILE, CLIENT_ID, FIXTURES, UK_PROFILE, US_PROFILE, load_script

SEPT_1, SEPT_14, SEPT_15 = date(2026, 9, 1), date(2026, 9, 14), date(2026, 9, 15)
NO_WAIT = PollPolicy(sleep=lambda _: None)


def clients(api: FakeAmazonAdsApi, fast_retry: RetryPolicy, regions=("NA", "EU")):
    http = httpx.Client(transport=httpx.MockTransport(api.handle))
    return [
        AmazonAdsClient.create(
            region=REGIONS[code],
            client_id=CLIENT_ID,
            client_secret="secret",
            refresh_token="Atzr|refresh",
            http=http,
            retry=fast_retry,
        )
        for code in regions
    ]


@pytest.fixture
def api() -> FakeAmazonAdsApi:
    return FakeAmazonAdsApi(FIXTURES)


@pytest.fixture
def warehouse(tmp_path):
    with DuckDBWarehouse(tmp_path / "pipeline.duckdb") as duckdb_warehouse:
        yield duckdb_warehouse


def counts(warehouse: DuckDBWarehouse) -> dict[str, int]:
    tables = ["profiles", "sp_campaign_daily", "sp_advertised_product_daily"]
    return {t: warehouse.query(f"select count(*) from amazon_ads_raw.{t}")[0][0] for t in tables}


def load(api, warehouse, fast_retry, start=SEPT_1, end=SEPT_14, **kwargs):
    return run(
        clients(api, fast_retry),
        warehouse,
        start=start,
        end=end,
        snapshot_date=SEPT_15,
        poll=NO_WAIT,
        **kwargs,
    )


def test_full_run_loads_every_profile_and_report(api, warehouse, fast_retry) -> None:
    results = load(api, warehouse, fast_retry)
    assert sorted((r.profile_id, r.table, r.rows) for r in results) == [
        (US_PROFILE, "profiles", 1),
        (US_PROFILE, "sp_advertised_product_daily", 70),
        (US_PROFILE, "sp_campaign_daily", 35),
        (UK_PROFILE, "profiles", 1),
        (UK_PROFILE, "sp_advertised_product_daily", 42),
        (UK_PROFILE, "sp_campaign_daily", 28),
    ]
    assert counts(warehouse) == {
        "profiles": 2,  # the agency (DSP) profile is skipped
        "sp_campaign_daily": 63,
        "sp_advertised_product_daily": 112,
    }


def test_each_profile_is_served_by_its_own_region(api, warehouse, fast_retry) -> None:
    load(api, warehouse, fast_retry)
    hosts = {
        request.headers.get("Amazon-Advertising-API-Scope"): request.url.host
        for request in api.requests
        if request.url.path == "/reporting/reports"
    }
    assert hosts == {
        str(US_PROFILE): "advertising-api.amazon.com",
        str(UK_PROFILE): "advertising-api-eu.amazon.com",
    }
    token_hosts = {r.url.host for r in api.requests if r.url.path == "/auth/o2/token"}
    assert token_hosts == {"api.amazon.com", "api.amazon.co.uk"}
    regions = dict(warehouse.query("select profile_id, region from amazon_ads_raw.profiles"))
    assert regions == {US_PROFILE: "NA", UK_PROFILE: "EU"}


def test_reruns_and_overlapping_windows_do_not_duplicate(api, warehouse, fast_retry) -> None:
    load(api, warehouse, fast_retry)
    before = counts(warehouse)
    spend = warehouse.query("select sum(cost) from amazon_ads_raw.sp_advertised_product_daily")

    load(FakeAmazonAdsApi(FIXTURES), warehouse, fast_retry)
    load(FakeAmazonAdsApi(FIXTURES), warehouse, fast_retry, start=date(2026, 9, 10))

    assert counts(warehouse) == before
    assert (
        warehouse.query("select sum(cost) from amazon_ads_raw.sp_advertised_product_daily") == spend
    )


def test_a_long_range_is_requested_in_31_day_windows(api, warehouse, fast_retry) -> None:
    results = load(api, warehouse, fast_retry, start=date(2026, 7, 20))
    windows = sorted({(r.start, r.end) for r in results if r.table == "sp_campaign_daily"})
    assert windows == [(date(2026, 7, 20), date(2026, 8, 19)), (date(2026, 8, 20), SEPT_14)]
    assert counts(warehouse)["sp_campaign_daily"] == 63


def test_only_the_requested_profiles_are_loaded(api, warehouse, fast_retry) -> None:
    load(api, warehouse, fast_retry, profile_ids=[UK_PROFILE])
    assert warehouse.query("select distinct profile_id from amazon_ads_raw.sp_campaign_daily") == [
        (UK_PROFILE,)
    ]


def test_an_unknown_profile_is_an_error(api, warehouse, fast_retry) -> None:
    with pytest.raises(PipelineError, match="not found in the requested regions: \\[1234\\]"):
        load(api, warehouse, fast_retry, profile_ids=[UK_PROFILE, 1234])


def test_a_duplicate_of_a_report_in_progress_is_reused(api, warehouse, fast_retry) -> None:
    # A previous run crashed after requesting this report; Amazon is still generating it.
    [us_client] = clients(api, fast_retry, regions=("NA",))
    earlier = us_client.create_report(US_PROFILE, SP_CAMPAIGNS.request_body(SEPT_1, SEPT_14))

    load(api, warehouse, fast_retry)

    creates = [r for r in api.requests if r.url.path == "/reporting/reports"]
    assert len(creates) == 5  # 1 earlier + 4 in the run, one of which got 425
    assert counts(warehouse)["sp_campaign_daily"] == 63
    polled = {
        r.url.path.rsplit("/", 1)[1] for r in api.requests if "/reporting/reports/" in r.url.path
    }
    assert earlier in polled


class FailingApi(FakeAmazonAdsApi):
    """Amazon reports FAILED for every spAdvertisedProduct report."""

    def _describe(self, report_id: str) -> dict:
        report = super()._describe(report_id)
        if report["configuration"]["reportTypeId"] == "spAdvertisedProduct":
            report.update(status="FAILED", url=None, failureReason="Internal error")
        return report


def test_failed_reports_are_raised_after_the_others_load(warehouse, fast_retry) -> None:
    with pytest.raises(PipelineError, match=r"spAdvertisedProduct .*: Internal error"):
        load(FailingApi(FIXTURES), warehouse, fast_retry)
    assert counts(warehouse)["sp_campaign_daily"] == 63
    assert counts(warehouse)["sp_advertised_product_daily"] == 0


class ForbiddenUkApi(FakeAmazonAdsApi):
    """The UK profile is listed but the credentials may not request its reports."""

    def _create_report(self, request, region):
        if region == "EU":
            return httpx.Response(
                403, json={"code": "403", "detail": "Not authorized to access scope"}
            )
        return super()._create_report(request, region)


def test_one_failing_profile_does_not_block_the_others(warehouse, fast_retry) -> None:
    with pytest.raises(PipelineError, match=f"profile {UK_PROFILE}: AmazonAdsApiError.*403"):
        load(ForbiddenUkApi(FIXTURES), warehouse, fast_retry)
    loaded = warehouse.query("select distinct profile_id from amazon_ads_raw.sp_campaign_daily")
    assert loaded == [(US_PROFILE,)]


def fake_clock_poll() -> PollPolicy:
    now = [0.0]
    return PollPolicy(
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds), clock=lambda: now[0]
    )


def test_reports_that_never_finish_time_out(warehouse, fast_retry) -> None:
    with pytest.raises(ReportTimeout, match="not ready after 10800s"):
        run(
            clients(FakeAmazonAdsApi(FIXTURES, polls_until_complete=10_000), fast_retry),
            warehouse,
            start=SEPT_1,
            end=SEPT_14,
            snapshot_date=SEPT_15,
            poll=fake_clock_poll(),
        )


class FailedAndStuckApi(FakeAmazonAdsApi):
    """spAdvertisedProduct reports FAIL at once; spCampaigns reports never finish."""

    def _describe(self, report_id: str) -> dict:
        report = super()._describe(report_id)
        if report["configuration"]["reportTypeId"] == "spAdvertisedProduct":
            report.update(status="FAILED", url=None, failureReason="Internal error")
        else:
            report.update(status="PROCESSING", url=None)
        return report


def test_a_timeout_still_reports_the_other_failures(warehouse, fast_retry) -> None:
    with pytest.raises(ReportTimeout) as error:
        run(
            clients(FailedAndStuckApi(FIXTURES), fast_retry),
            warehouse,
            start=SEPT_1,
            end=SEPT_14,
            snapshot_date=SEPT_15,
            poll=fake_clock_poll(),
        )
    message = str(error.value)
    assert message.count("spCampaigns") == 2 and "not ready after 10800s" in message
    assert message.count("spAdvertisedProduct") == 2 and "Internal error" in message
    assert isinstance(error.value, PipelineError)  # the CLI exits 1 for either


# 02:30 UTC on the 15th: 19:30 on the 14th in Los Angeles, 03:30 on the 15th in London.
EARLY_UTC = datetime(2026, 9, 15, 2, 30, tzinfo=timezone.utc)


def test_default_window_ends_yesterday_in_each_profile_time_zone(
    api, warehouse, fast_retry
) -> None:
    results = run(
        clients(api, fast_retry),
        warehouse,
        snapshot_date=SEPT_15,
        lookback_days=5,
        now=lambda: EARLY_UTC,
        poll=NO_WAIT,
    )
    windows = {(r.profile_id, r.table): (r.start, r.end) for r in results}
    # The 14th has not ended in Los Angeles, so the US profile stops at the 13th.
    assert windows[(US_PROFILE, "sp_campaign_daily")] == (date(2026, 9, 9), date(2026, 9, 13))
    assert windows[(UK_PROFILE, "sp_campaign_daily")] == (date(2026, 9, 10), SEPT_14)
    last_days = dict(
        warehouse.query(
            "select profile_id, max(date) from amazon_ads_raw.sp_campaign_daily group by 1"
        )
    )
    assert last_days == {US_PROFILE: date(2026, 9, 13), UK_PROFILE: SEPT_14}


@pytest.mark.parametrize(
    ("time_zone", "now", "yesterday"),
    [
        ("America/Los_Angeles", datetime(2026, 9, 15, 6, 59, tzinfo=timezone.utc), 13),
        ("America/Los_Angeles", datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc), 14),
        ("Europe/London", datetime(2026, 9, 14, 23, 30, tzinfo=timezone.utc), 14),
        ("Asia/Tokyo", datetime(2026, 9, 14, 20, 0, tzinfo=timezone.utc), 14),
    ],
)
def test_profile_window_follows_the_profile_calendar(
    time_zone: str, now: datetime, yesterday: int
) -> None:
    start, end = profile_window(time_zone, now, lookback_days=30)
    assert end == date(2026, 9, yesterday)
    assert start == end - timedelta(days=29)


def test_recording_then_replaying_round_trips(tmp_path, api, warehouse, fast_retry) -> None:
    load(api, warehouse, fast_retry, recorder=Recorder(tmp_path / "recorded"))
    with DuckDBWarehouse(tmp_path / "replayed.duckdb") as replayed:
        load(FakeAmazonAdsApi(tmp_path / "recorded"), replayed, fast_retry)
        assert counts(replayed) == counts(warehouse)
        query = "select sum(cost), sum(sales_7d) from amazon_ads_raw.sp_advertised_product_daily"
        assert replayed.query(query) == warehouse.query(query)
    assert (tmp_path / "recorded" / "api" / "profiles_NA.json").exists()
    assert str(AGENCY_PROFILE) in (tmp_path / "recorded" / "api" / "profiles_NA.json").read_text()


def test_cli_runs_offline_against_duckdb(tmp_path, capsys) -> None:
    exit_code = main(
        [
            "--region", "NA", "--region", "EU",
            "--start", "2026-09-01", "--end", "2026-09-14",
            "--warehouse", "duckdb", "--duckdb-path", str(tmp_path / "cli.duckdb"),
            "--replay", str(FIXTURES),
        ]
    )  # fmt: skip
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "sp_advertised_product_daily" in output
    assert "70 rows" in output and "42 rows" in output


def test_cli_exits_with_1_when_something_fails(tmp_path, caplog) -> None:
    exit_code = main(
        [
            "--region", "NA", "--profile-id", "1234",
            "--warehouse", "duckdb", "--duckdb-path", str(tmp_path / "cli.duckdb"),
            "--replay", str(FIXTURES),
        ]
    )  # fmt: skip
    assert exit_code == 1
    assert "Profiles not found" in caplog.text


def test_cli_asks_for_credentials(monkeypatch, capsys) -> None:
    for name in ("AMAZON_ADS_CLIENT_ID", "AMAZON_ADS_CLIENT_SECRET", "AMAZON_ADS_REFRESH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit):
        main(["--warehouse", "duckdb"])
    assert "AMAZON_ADS_CLIENT_ID" in capsys.readouterr().err


def test_fixtures_are_reproducible(tmp_path) -> None:
    for path in load_script("generate_fixtures").write_fixtures(tmp_path):
        committed = FIXTURES / path.relative_to(tmp_path)
        assert path.read_text() == committed.read_text(), path.name
