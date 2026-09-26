from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import sqlglot
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from amazon_ads_pipeline.schemas import ALL_TABLES, PROFILES, SP_CAMPAIGN_DAILY
from amazon_ads_pipeline.warehouse import BigQueryWarehouse, DuckDBWarehouse

SEPT_1, SEPT_2, SEPT_3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)
US, UK = 3000000000000001, 3000000000000002


def campaign_row(day: date, cost: str, profile_id: int = US) -> dict:
    return {
        "date": day,
        "profile_id": profile_id,
        "campaign_id": 500000000000001,
        "campaign_name": "SP - Auto - Tents",
        "campaign_status": "ENABLED",
        "campaign_budget_amount": Decimal("50.00"),
        "impressions": 1000,
        "clicks": 10,
        "cost": Decimal(cost),
        "purchases_7d": 1,
        "sales_7d": Decimal("89.99"),
        "units_sold_7d": 1,
        "purchases_14d": 1,
        "sales_14d": Decimal("89.99"),
    }


@pytest.fixture
def duckdb_warehouse(tmp_path):
    with DuckDBWarehouse(tmp_path / "test.duckdb") as warehouse:
        warehouse.ensure_tables(ALL_TABLES)
        yield warehouse


def _costs(warehouse: DuckDBWarehouse) -> list[tuple]:
    return warehouse.query(
        "select profile_id, date, cost from amazon_ads_raw.sp_campaign_daily order by 1, 2"
    )


def _replace(warehouse, rows, profile_id=US, start=SEPT_1, end=SEPT_2) -> None:
    warehouse.replace_range(SP_CAMPAIGN_DAILY, rows, profile_id=profile_id, start=start, end=end)


def test_reloading_a_window_replaces_rows(duckdb_warehouse: DuckDBWarehouse) -> None:
    rows = [campaign_row(SEPT_1, "10.00"), campaign_row(SEPT_2, "20.00")]
    _replace(duckdb_warehouse, rows)
    _replace(duckdb_warehouse, rows)
    assert len(_costs(duckdb_warehouse)) == 2

    # Amazon restates recent days (invalid clicks, late sales): the newest load wins.
    _replace(duckdb_warehouse, [campaign_row(SEPT_1, "9.50"), campaign_row(SEPT_2, "20.00")])
    assert [row[2] for row in _costs(duckdb_warehouse)] == [Decimal("9.50"), Decimal("20.00")]


def test_an_empty_report_clears_its_window(duckdb_warehouse: DuckDBWarehouse) -> None:
    _replace(duckdb_warehouse, [campaign_row(SEPT_1, "10.00"), campaign_row(SEPT_2, "20.00")])
    _replace(duckdb_warehouse, [], start=SEPT_2, end=SEPT_2)
    assert [row[1] for row in _costs(duckdb_warehouse)] == [SEPT_1]


def test_other_days_and_other_profiles_are_untouched(duckdb_warehouse: DuckDBWarehouse) -> None:
    _replace(
        duckdb_warehouse, [campaign_row(SEPT_1, "1.00"), campaign_row(SEPT_3, "3.00")], end=SEPT_3
    )
    _replace(duckdb_warehouse, [campaign_row(SEPT_2, "5.00", UK)], profile_id=UK, end=SEPT_3)
    _replace(duckdb_warehouse, [campaign_row(SEPT_3, "3.30")], start=SEPT_3, end=SEPT_3)
    assert _costs(duckdb_warehouse) == [
        (US, SEPT_1, Decimal("1.00")),
        (US, SEPT_3, Decimal("3.30")),
        (UK, SEPT_2, Decimal("5.00")),
    ]


def test_a_failed_load_keeps_the_previous_data(duckdb_warehouse: DuckDBWarehouse) -> None:
    _replace(duckdb_warehouse, [campaign_row(SEPT_1, "10.00")], end=SEPT_1)
    broken = {**campaign_row(SEPT_1, "11.00"), "campaign_name": None}  # NOT NULL column
    with pytest.raises(Exception, match="NOT NULL"):
        _replace(duckdb_warehouse, [broken], end=SEPT_1)
    assert _costs(duckdb_warehouse) == [(US, SEPT_1, Decimal("10.00"))]


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (campaign_row(date(2026, 9, 9), "1.00"), "outside the range"),
        (campaign_row(SEPT_1, "1.00", profile_id=UK), "outside the range"),
        ({**campaign_row(SEPT_1, "1.00"), "extra": 1}, "do not match the schema"),
    ],
)
def test_rows_outside_the_replaced_window_are_refused(
    duckdb_warehouse: DuckDBWarehouse, row: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _replace(duckdb_warehouse, [row])


@pytest.fixture
def bq_client() -> MagicMock:
    client = MagicMock(spec=bigquery.Client)
    client.project = "demo-project"
    return client


def test_tables_are_created_partitioned_and_clustered(bq_client: MagicMock) -> None:
    bq_client.get_dataset.side_effect = NotFound("no dataset yet")
    BigQueryWarehouse(bq_client, "amazon_ads_raw", "US").ensure_tables(ALL_TABLES)

    assert bq_client.create_dataset.call_args.args[0].location == "US"
    tables = {call.args[0].table_id: call.args[0] for call in bq_client.create_table.call_args_list}
    assert set(tables) == {"profiles", "sp_campaign_daily", "sp_advertised_product_daily"}
    assert tables["sp_advertised_product_daily"].time_partitioning.field == "date"
    assert tables["sp_advertised_product_daily"].clustering_fields == [
        "profile_id",
        "campaign_id",
        "advertised_asin",
    ]
    assert tables["profiles"].time_partitioning.field == "snapshot_date"


def test_load_stages_rows_then_swaps_the_window_in_one_transaction(bq_client: MagicMock) -> None:
    BigQueryWarehouse(bq_client, "amazon_ads_raw").replace_range(
        SP_CAMPAIGN_DAILY,
        [campaign_row(SEPT_1, "8.18"), campaign_row(SEPT_2, "0.10")],
        profile_id=US,
        start=SEPT_1,
        end=SEPT_2,
    )
    staging = bq_client.create_table.call_args.args[0]
    staging_id = f"{staging.project}.{staging.dataset_id}.{staging.table_id}"
    rows_json, destination = bq_client.load_table_from_json.call_args.args
    assert destination == staging_id
    assert [row["cost"] for row in rows_json] == ["8.18", "0.10"]

    sql = bq_client.query.call_args.args[0]
    statements = [type(s).__name__ for s in sqlglot.parse(sql, read="bigquery")]
    assert statements == ["Transaction", "Delete", "Insert", "Commit"]
    assert "WHERE profile_id = @profile_id\n  AND date BETWEEN @start_date AND @end_date" in sql
    params = {
        p.name: p.value for p in bq_client.query.call_args.kwargs["job_config"].query_parameters
    }
    assert params == {"profile_id": US, "start_date": SEPT_1, "end_date": SEPT_2}
    bq_client.delete_table.assert_called_once_with(staging_id, not_found_ok=True)


def test_an_empty_window_is_cleared_without_a_load_job(bq_client: MagicMock) -> None:
    BigQueryWarehouse(bq_client, "amazon_ads_raw").replace_range(
        PROFILES, [], profile_id=US, start=SEPT_1, end=SEPT_1
    )
    bq_client.load_table_from_json.assert_not_called()
    sql = bq_client.query.call_args.args[0]
    assert [type(s).__name__ for s in sqlglot.parse(sql, read="bigquery")] == ["Delete"]
