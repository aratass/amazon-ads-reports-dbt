"""One run: resolve profiles, request every report, then load each report as it completes."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.replay import Recorder
from amazon_ads_pipeline.reports import ALL_REPORTS, LOOKBACK_DAYS, ReportSpec, date_windows
from amazon_ads_pipeline.schemas import ALL_TABLES, PROFILES
from amazon_ads_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

# Sponsored ads reporting is for seller and vendor profiles; agency profiles are DSP.
SPONSORED_ADS_ACCOUNT_TYPES = frozenset({"seller", "vendor"})
DEFAULT_LOOKBACK_DAYS = 30


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PipelineError(RuntimeError):
    """Some profiles or reports failed; everything else was loaded."""


class ReportTimeout(PipelineError):
    """Reports were still pending when the poll timeout ran out (other failures included)."""


def profile_window(
    time_zone: str,
    now: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    start: date | None = None,
    end: date | None = None,
) -> tuple[date, date]:
    """Fill in the dates left out, in the profile's own time zone.

    Amazon reports dates in the profile's time zone ("The time zone used for all
    date-based campaign management and reporting", Profiles API). At 02:30 UTC it is
    still the previous evening in Los Angeles, so "yesterday in UTC" would be a US
    profile's unfinished today, loaded as if it were complete.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    end = end or now.astimezone(ZoneInfo(time_zone)).date() - timedelta(days=1)
    start = start or end - timedelta(days=lookback_days - 1)
    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    return start, end


@dataclass(frozen=True)
class PollPolicy:
    """Status checks start every 15s and back off to every 2 minutes.

    Amazon says a report can take up to three hours to generate, hence the timeout.
    """

    initial_interval: float = 15.0
    max_interval: float = 120.0
    multiplier: float = 1.5
    timeout: float = 3 * 60 * 60.0
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic


@dataclass(frozen=True)
class LoadResult:
    profile_id: int
    table: str
    start: date
    end: date
    rows: int


@dataclass(frozen=True)
class _Job:
    client: AmazonAdsClient
    profile_id: int
    spec: ReportSpec
    start: date
    end: date
    report_id: str

    def describe(self) -> str:
        return (
            f"{self.spec.report_type_id} {self.start} to {self.end} "
            f"for profile {self.profile_id} (report {self.report_id})"
        )


def profile_row(profile: Mapping[str, Any], region: str, snapshot_date: date) -> dict[str, Any]:
    account = profile.get("accountInfo") or {}
    return {
        "snapshot_date": snapshot_date,
        "profile_id": int(profile["profileId"]),
        "region": region,
        "country_code": profile["countryCode"],
        "currency_code": profile["currencyCode"],
        "timezone": profile["timezone"],
        "marketplace_id": account["marketplaceStringId"],
        "account_id": account.get("id") or None,
        "account_type": account["type"],
        "account_name": account.get("name") or None,  # not populated for sellers
    }


def select_profiles(
    profiles: Iterable[Mapping[str, Any]], wanted: set[int]
) -> list[Mapping[str, Any]]:
    """The requested profiles, or every seller and vendor profile when none are requested."""
    if wanted:
        return [p for p in profiles if int(p["profileId"]) in wanted]
    return [
        p
        for p in profiles
        if (p.get("accountInfo") or {}).get("type") in SPONSORED_ADS_ACCOUNT_TYPES
    ]


def run(
    clients: Sequence[AmazonAdsClient],
    warehouse: Warehouse,
    *,
    snapshot_date: date,
    start: date | None = None,
    end: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now: Callable[[], datetime] | None = None,
    profile_ids: Iterable[int] = (),
    reports: Sequence[ReportSpec] = ALL_REPORTS,
    poll: PollPolicy | None = None,
    recorder: Recorder | None = None,
) -> list[LoadResult]:
    """Load `start`..`end` (inclusive) of every report for every selected profile.

    Dates left out are worked out per profile in the profile's time zone (see
    `profile_window`): `end` defaults to the profile's yesterday and `start` to
    `lookback_days` days before it. Each report window replaces exactly the profile and
    dates it covers, so re-running a range, or a rolling window that overlaps earlier
    runs, never duplicates rows.
    """
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be at least 1, not {lookback_days}")
    if start is not None and end is not None:
        date_windows(start, end)  # refuse start > end before calling the API
    moment = (now or utc_now)()
    warehouse.ensure_tables(ALL_TABLES)

    wanted = {int(profile_id) for profile_id in profile_ids}
    selected: list[tuple[AmazonAdsClient, Mapping[str, Any]]] = []
    for client in clients:
        profiles = client.list_profiles()
        if recorder:
            recorder.profiles(client.region.code, profiles)
        selected.extend((client, profile) for profile in select_profiles(profiles, wanted))
    missing = wanted - {int(profile["profileId"]) for _, profile in selected}
    if missing:
        raise PipelineError(f"Profiles not found in the requested regions: {sorted(missing)}")

    results: list[LoadResult] = []
    failures: list[str] = []
    jobs: list[_Job] = []
    for client, profile in selected:
        profile_id = int(profile["profileId"])
        try:
            first, last = profile_window(profile["timezone"], moment, lookback_days, start, end)
            windows = date_windows(first, last)
            local_today = moment.astimezone(ZoneInfo(profile["timezone"])).date()
            if first < local_today - timedelta(days=LOOKBACK_DAYS):
                log.warning(
                    "Profile %s: %s is more than %d days back; most report types "
                    "return nothing that old",
                    profile_id,
                    first,
                    LOOKBACK_DAYS,
                )
            warehouse.replace_range(
                PROFILES,
                [profile_row(profile, client.region.code, snapshot_date)],
                profile_id=profile_id,
                start=snapshot_date,
                end=snapshot_date,
            )
            results.append(LoadResult(profile_id, PROFILES.name, snapshot_date, snapshot_date, 1))
            # Request everything first: Amazon generates reports in parallel on its side.
            for spec in reports:
                for window_start, window_end in windows:
                    body = spec.request_body(window_start, window_end)
                    report_id = client.create_report(profile_id, body)
                    job = _Job(client, profile_id, spec, window_start, window_end, report_id)
                    log.info("Requested %s", job.describe())
                    jobs.append(job)
        except Exception as error:  # one broken profile must not block the others
            log.exception("Profile %s failed", profile_id)
            failures.append(f"profile {profile_id}: {type(error).__name__}: {error}")

    loaded, report_failures, timed_out = _wait_and_load(
        jobs, warehouse, poll or PollPolicy(), recorder
    )
    results.extend(loaded)
    failures.extend(report_failures)
    if timed_out:
        # Still a failure of the run, but the message keeps every other failure too.
        raise ReportTimeout("Not loaded: " + "; ".join([*timed_out, *failures]))
    if failures:
        raise PipelineError("Not loaded: " + "; ".join(failures))
    return results


def _wait_and_load(
    jobs: list[_Job], warehouse: Warehouse, poll: PollPolicy, recorder: Recorder | None
) -> tuple[list[LoadResult], list[str], list[str]]:
    """Poll every pending report; download and load each one as soon as it completes.

    Loading on completion keeps well inside the download URL's one-hour lifetime. A report
    that fails is recorded and the others carry on; the caller raises for all of them.
    Returns the loads, the failures, and the reports still pending at the timeout.
    """
    results: list[LoadResult] = []
    failures: list[str] = []
    pending = list(jobs)
    deadline = poll.clock() + poll.timeout
    interval = poll.initial_interval
    while pending:
        for job in list(pending):
            try:
                report = job.client.get_report(job.profile_id, job.report_id)
                status = report.get("status")
                if status == "COMPLETED":
                    results.append(_load(job, report["url"], warehouse, recorder))
                elif status == "FAILED":
                    log.error(
                        "%s: Amazon reports FAILED: %s", job.describe(), report.get("failureReason")
                    )
                    failures.append(f"{job.describe()}: {report.get('failureReason')}")
                else:
                    continue  # PENDING or PROCESSING
            except Exception as error:
                log.exception("Loading %s failed", job.describe())
                failures.append(f"{job.describe()}: {type(error).__name__}: {error}")
            pending.remove(job)
        if pending:
            if poll.clock() >= deadline:
                timed_out = [
                    f"{job.describe()}: not ready after {poll.timeout:.0f}s" for job in pending
                ]
                return results, failures, timed_out
            poll.sleep(interval)
            interval = min(interval * poll.multiplier, poll.max_interval)
    return results, failures, []


def _load(job: _Job, url: str, warehouse: Warehouse, recorder: Recorder | None) -> LoadResult:
    records = job.client.download_report(url)
    if recorder:
        recorder.report(job.profile_id, job.spec.report_type_id, job.start, job.end, records)
    rows = [job.spec.to_row(record, job.profile_id) for record in records]
    warehouse.replace_range(
        job.spec.table, rows, profile_id=job.profile_id, start=job.start, end=job.end
    )
    return LoadResult(job.profile_id, job.spec.table.name, job.start, job.end, len(rows))
