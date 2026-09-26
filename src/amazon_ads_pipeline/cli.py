"""Command-line entry point: `amazon-ads-pipeline`."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import date, datetime, timezone

import httpx

from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.pipeline import DEFAULT_LOOKBACK_DAYS, PipelineError, PollPolicy, run
from amazon_ads_pipeline.regions import get_region
from amazon_ads_pipeline.replay import FakeAmazonAdsApi, Recorder
from amazon_ads_pipeline.warehouse import BigQueryWarehouse, DuckDBWarehouse, Warehouse

log = logging.getLogger(__name__)

CREDENTIAL_VARIABLES = (
    "AMAZON_ADS_CLIENT_ID",
    "AMAZON_ADS_CLIENT_SECRET",
    "AMAZON_ADS_REFRESH_TOKEN",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="amazon-ads-pipeline",
        description=(
            "Load Sponsored Products campaign and advertised product reports (Amazon Ads "
            "reporting v3) into BigQuery, or a local DuckDB file. Credentials come from "
            + ", ".join(CREDENTIAL_VARIABLES)
            + "."
        ),
    )
    parser.add_argument(
        "--region",
        dest="regions",
        action="append",
        choices=("NA", "EU", "FE"),
        help="API region to read. Repeatable. Default: comma-separated AMAZON_ADS_REGIONS, or NA.",
    )
    parser.add_argument(
        "--profile-id",
        dest="profile_ids",
        action="append",
        type=int,
        help="Profile to load. Repeatable. Default: comma-separated AMAZON_ADS_PROFILE_IDS, "
        "or every seller and vendor profile in the regions.",
    )
    parser.add_argument("--start", type=date.fromisoformat, help="First day to load (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        help="Last day to load (default: yesterday in each profile's time zone, the zone "
        "Amazon reports dates in).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Without --start, reload this many days up to --end (default 30), so late "
        "attributed sales are refreshed.",
    )
    parser.add_argument(
        "--warehouse",
        choices=("bigquery", "duckdb"),
        default=os.environ.get("WAREHOUSE", "bigquery"),
    )
    parser.add_argument("--bq-project", default=os.environ.get("BQ_PROJECT"))
    parser.add_argument("--bq-dataset", default=os.environ.get("BQ_RAW_DATASET", "amazon_ads_raw"))
    parser.add_argument("--bq-location", default=os.environ.get("BQ_LOCATION"))
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "local.duckdb"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--replay", metavar="DIR", help="Serve recorded reports from DIR through a fake API."
    )
    source.add_argument(
        "--record", metavar="DIR", help="Also save every profile list and report to DIR."
    )
    return parser


def _clients(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[AmazonAdsClient]:
    regions = args.regions or os.environ.get("AMAZON_ADS_REGIONS", "NA").split(",")
    if args.replay:
        api = FakeAmazonAdsApi(args.replay)
        http = httpx.Client(transport=httpx.MockTransport(api.handle))
        credentials = {
            "client_id": api.client_id,
            "client_secret": "offline",
            "refresh_token": "offline",
        }
    else:
        missing = [name for name in CREDENTIAL_VARIABLES if not os.environ.get(name)]
        if missing:
            parser.error(f"set {', '.join(missing)} (the client's Amazon Ads API credentials)")
        http = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0))
        credentials = {
            "client_id": os.environ["AMAZON_ADS_CLIENT_ID"],
            "client_secret": os.environ["AMAZON_ADS_CLIENT_SECRET"],
            "refresh_token": os.environ["AMAZON_ADS_REFRESH_TOKEN"],
        }
    return [
        AmazonAdsClient.create(region=get_region(code.strip()), http=http, **credentials)
        for code in regions
        if code.strip()
    ]


def _warehouse(args: argparse.Namespace, parser: argparse.ArgumentParser) -> Warehouse:
    if args.warehouse == "duckdb":
        return DuckDBWarehouse(args.duckdb_path)
    if not args.bq_project:
        parser.error("--bq-project (or BQ_PROJECT) is required when --warehouse is bigquery")
    from google.cloud import bigquery

    client = bigquery.Client(project=args.bq_project, location=args.bq_location)
    return BigQueryWarehouse(client, args.bq_dataset, args.bq_location)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs every URL at INFO, including signed download links; keep those out.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    profile_ids = args.profile_ids or [
        int(value) for value in os.environ.get("AMAZON_ADS_PROFILE_IDS", "").split(",") if value
    ]
    if args.lookback_days < 1:
        parser.error("--lookback-days must be at least 1")
    if args.start and args.end and args.start > args.end:
        parser.error(f"--start {args.start} is after --end {args.end}")

    clients = _clients(args, parser)
    warehouse = _warehouse(args, parser)
    # Offline replays complete instantly, so there is nothing to wait for between polls.
    poll = PollPolicy(sleep=lambda _: None) if args.replay else PollPolicy()
    try:
        # Dates left out are worked out per profile, in the profile's time zone.
        results = run(
            clients,
            warehouse,
            start=args.start,
            end=args.end,
            lookback_days=args.lookback_days,
            snapshot_date=datetime.now(timezone.utc).date(),
            profile_ids=profile_ids,
            poll=poll,
            recorder=Recorder(args.record) if args.record else None,
        )
    except PipelineError as error:  # ReportTimeout is a PipelineError too
        log.error("%s", error)
        return 1
    finally:
        if isinstance(warehouse, DuckDBWarehouse):
            warehouse.close()

    for result in results:
        print(
            f"{result.profile_id}  {result.table:<28} {result.start} to {result.end}"
            f"  {result.rows:>6} rows"
        )
    return 0
