import json
from datetime import date
from decimal import Decimal

import pytest

from amazon_ads_pipeline.reports import (
    ALL_REPORTS,
    MAX_DAYS_PER_REPORT,
    SP_ADVERTISED_PRODUCT,
    SP_CAMPAIGNS,
    date_windows,
)

from support import FIXTURES, US_PROFILE


def recorded(report_type: str) -> list[dict]:
    path = FIXTURES / "reports" / str(US_PROFILE) / f"{report_type}.json"
    return json.loads(path.read_text(), parse_float=Decimal)


def test_long_ranges_split_into_31_day_windows() -> None:
    assert date_windows(date(2026, 7, 1), date(2026, 9, 14)) == [
        (date(2026, 7, 1), date(2026, 7, 31)),
        (date(2026, 8, 1), date(2026, 8, 31)),
        (date(2026, 9, 1), date(2026, 9, 14)),
    ]
    assert date_windows(date(2026, 9, 1), date(2026, 9, 1)) == [
        (date(2026, 9, 1), date(2026, 9, 1))
    ]
    assert len(date_windows(date(2026, 8, 1), date(2026, 8, 31))) == 1
    with pytest.raises(ValueError):
        date_windows(date(2026, 9, 2), date(2026, 9, 1))


def test_campaign_report_request_body() -> None:
    assert SP_CAMPAIGNS.request_body(date(2026, 9, 1), date(2026, 9, 14)) == {
        "name": "sp_campaign_daily 2026-09-01 to 2026-09-14",
        "startDate": "2026-09-01",
        "endDate": "2026-09-14",
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spCampaigns",
            "groupBy": ["campaign"],
            "columns": [
                "date",
                "campaignId",
                "campaignName",
                "campaignStatus",
                "campaignBudgetAmount",
                "impressions",
                "clicks",
                "cost",
                "purchases7d",
                "sales7d",
                "unitsSoldClicks7d",
                "purchases14d",
                "sales14d",
            ],
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }


def test_advertised_product_report_groups_by_advertiser() -> None:
    config = SP_ADVERTISED_PRODUCT.request_body(date(2026, 9, 1), date(2026, 9, 1))["configuration"]
    assert config["reportTypeId"] == "spAdvertisedProduct"
    assert config["groupBy"] == ["advertiser"]
    assert {"advertisedAsin", "advertisedSku", "adId", "cost"} <= set(config["columns"])


def test_a_request_longer_than_31_days_is_refused() -> None:
    with pytest.raises(ValueError, match=f"1 to {MAX_DAYS_PER_REPORT} days"):
        SP_CAMPAIGNS.request_body(date(2026, 8, 1), date(2026, 9, 1))


def test_campaign_record_maps_to_a_typed_row() -> None:
    row = SP_CAMPAIGNS.to_row(recorded("spCampaigns")[0], US_PROFILE)
    assert row == {
        "date": date(2026, 9, 1),
        "profile_id": US_PROFILE,
        "campaign_id": 500000000000001,
        "campaign_name": "SP - Auto - Tents",
        "campaign_status": "ENABLED",
        "campaign_budget_amount": Decimal("50.0"),
        "impressions": 1797,
        "clicks": 9,
        "cost": Decimal("11.62"),
        "purchases_7d": 0,
        "sales_7d": Decimal("0.0"),
        "units_sold_7d": 0,
        "purchases_14d": 0,
        "sales_14d": Decimal("0.0"),
    }


def test_vendor_records_without_a_sku_map_to_null() -> None:
    record = {**recorded("spAdvertisedProduct")[0], "advertisedSku": ""}
    assert SP_ADVERTISED_PRODUCT.to_row(record, US_PROFILE)["advertised_sku"] is None


@pytest.mark.parametrize("spec", ALL_REPORTS, ids=lambda spec: spec.report_type_id)
def test_mapping_reads_only_requested_columns_and_fills_the_schema(spec) -> None:
    # Amazon returns exactly the requested columns, so map a record trimmed to them.
    record = {column: recorded(spec.report_type_id)[0][column] for column in spec.columns}
    row = spec.to_row(record, US_PROFILE)
    assert set(row) == {column.name for column in spec.table.data_columns}


def test_a_missing_metric_column_fails_loudly() -> None:
    record = recorded("spCampaigns")[0]
    del record["cost"]
    with pytest.raises(KeyError, match="cost"):
        SP_CAMPAIGNS.to_row(record, US_PROFILE)
