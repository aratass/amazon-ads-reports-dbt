"""Sponsored Products reporting v3 definitions and row mappers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from amazon_ads_pipeline import schemas
from amazon_ads_pipeline.schemas import TableSpec

Row = dict[str, Any]
Record = Mapping[str, Any]

MAX_DAYS_PER_REPORT = 31  # Amazon allows at most 31 days in one report request
LOOKBACK_DAYS = 95  # most report types can go back 95 days

_METRIC_COLUMNS = (
    "impressions",
    "clicks",
    "cost",
    "purchases7d",
    "sales7d",
    "unitsSoldClicks7d",
    "purchases14d",
    "sales14d",
)


def date_windows(
    start: date, end: date, max_days: int = MAX_DAYS_PER_REPORT
) -> list[tuple[date, date]]:
    """Split [start, end] into consecutive windows of at most `max_days` days."""
    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    windows = []
    while start <= end:
        window_end = min(end, start + timedelta(days=max_days - 1))
        windows.append((start, window_end))
        start = window_end + timedelta(days=1)
    return windows


@dataclass(frozen=True)
class ReportSpec:
    """One v3 report type and the mapping of its records onto a raw table."""

    name: str
    report_type_id: str
    group_by: tuple[str, ...]
    columns: tuple[str, ...]
    table: TableSpec
    to_row: Callable[[Record, int], Row]

    def request_body(self, start: date, end: date) -> dict[str, Any]:
        days = (end - start).days + 1
        if not 1 <= days <= MAX_DAYS_PER_REPORT:
            raise ValueError(f"A report covers 1 to {MAX_DAYS_PER_REPORT} days, not {days}")
        return {
            "name": f"{self.name} {start.isoformat()} to {end.isoformat()}",
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "reportTypeId": self.report_type_id,
                "groupBy": list(self.group_by),
                "columns": list(self.columns),
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }


def _decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _int(value: Any) -> int:
    return 0 if value is None else int(value)


def _metrics(record: Record) -> Row:
    # A missing key raises: it means the report columns and the mapping drifted apart.
    return {
        "impressions": _int(record["impressions"]),
        "clicks": _int(record["clicks"]),
        "cost": _decimal(record["cost"]),
        "purchases_7d": _int(record["purchases7d"]),
        "sales_7d": _decimal(record["sales7d"]),
        "units_sold_7d": _int(record["unitsSoldClicks7d"]),
        "purchases_14d": _int(record["purchases14d"]),
        "sales_14d": _decimal(record["sales14d"]),
    }


def _campaign_row(record: Record, profile_id: int) -> Row:
    budget = record.get("campaignBudgetAmount")
    return {
        "date": date.fromisoformat(record["date"]),
        "profile_id": profile_id,
        "campaign_id": int(record["campaignId"]),
        "campaign_name": record["campaignName"],
        "campaign_status": record["campaignStatus"],
        "campaign_budget_amount": None if budget is None else _decimal(budget),
        **_metrics(record),
    }


def _advertised_product_row(record: Record, profile_id: int) -> Row:
    return {
        "date": date.fromisoformat(record["date"]),
        "profile_id": profile_id,
        "campaign_id": int(record["campaignId"]),
        "ad_group_id": int(record["adGroupId"]),
        "ad_group_name": record["adGroupName"],
        "ad_id": int(record["adId"]),
        "advertised_asin": record["advertisedAsin"],
        "advertised_sku": record.get("advertisedSku") or None,
        **_metrics(record),
    }


SP_CAMPAIGNS = ReportSpec(
    name="sp_campaign_daily",
    report_type_id="spCampaigns",
    group_by=("campaign",),
    columns=(
        "date",
        "campaignId",
        "campaignName",
        "campaignStatus",
        "campaignBudgetAmount",
        *_METRIC_COLUMNS,
    ),
    table=schemas.SP_CAMPAIGN_DAILY,
    to_row=_campaign_row,
)

SP_ADVERTISED_PRODUCT = ReportSpec(
    name="sp_advertised_product_daily",
    report_type_id="spAdvertisedProduct",
    group_by=("advertiser",),
    columns=(
        "date",
        "campaignId",
        "adGroupId",
        "adGroupName",
        "adId",
        "advertisedAsin",
        "advertisedSku",
        *_METRIC_COLUMNS,
    ),
    table=schemas.SP_ADVERTISED_PRODUCT_DAILY,
    to_row=_advertised_product_row,
)

ALL_REPORTS = (SP_CAMPAIGNS, SP_ADVERTISED_PRODUCT)
