"""Build the dbt project on DuckDB, then break the data on purpose and check the tests notice.

A data test that has never been seen failing proves nothing, so each sabotage below
introduces one realistic defect and asserts that the matching tests, and only those,
report it.
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import httpx
import pytest

from amazon_ads_pipeline.client import AmazonAdsClient
from amazon_ads_pipeline.pipeline import PollPolicy, run
from amazon_ads_pipeline.regions import REGIONS
from amazon_ads_pipeline.replay import FakeAmazonAdsApi
from amazon_ads_pipeline.warehouse import DuckDBWarehouse

from support import CLIENT_ID, FIXTURES, ROOT

pytestmark = pytest.mark.dbt

TRANSFORM = ROOT / "transform"
DBT = Path(sys.executable).with_name("dbt")
RECONCILIATION = "assert_campaign_spend_matches_advertised_products"


@pytest.fixture(scope="session")
def loaded_database(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("raw") / "amazon_ads.duckdb"
    http = httpx.Client(transport=httpx.MockTransport(FakeAmazonAdsApi(FIXTURES).handle))
    clients = [
        AmazonAdsClient.create(
            region=REGIONS[code],
            client_id=CLIENT_ID,
            client_secret="secret",
            refresh_token="Atzr|refresh",
            http=http,
        )
        for code in ("NA", "EU")
    ]
    with DuckDBWarehouse(path) as warehouse:
        run(
            clients,
            warehouse,
            start=date(2026, 9, 1),
            end=date(2026, 9, 14),
            snapshot_date=date(2026, 9, 15),
            poll=PollPolicy(sleep=lambda _: None),
        )
    return path


@pytest.fixture
def database(loaded_database: Path, tmp_path: Path) -> Path:
    """A private copy of the loaded database that a test may damage."""
    return Path(shutil.copy(loaded_database, tmp_path / "amazon_ads.duckdb"))


def dbt(command: str, database: Path) -> tuple[int, str, dict[str, str]]:
    """Run a dbt command; return exit code, output and the status of every node."""
    target = database.parent / "target"
    result = subprocess.run(
        [
            str(DBT), *command.split(),
            "--project-dir", str(TRANSFORM),
            "--profiles-dir", str(TRANSFORM),
            "--target-path", str(target),
            "--log-path", str(database.parent / "logs"),
        ],
        env={**os.environ, "DUCKDB_PATH": str(database), "DBT_TARGET": "local"},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )  # fmt: skip
    results_file = "sources.json" if command == "source freshness" else "run_results.json"
    results = json.loads((target / results_file).read_text())["results"]
    statuses = {node_name(result["unique_id"]): result["status"] for result in results}
    return result.returncode, result.stdout, statuses


def node_name(unique_id: str) -> str:
    """test.project.name.hash -> name; source.project.source.table -> table."""
    kind, _, *parts = unique_id.split(".")
    if kind == "source":
        return parts[-1]
    if kind == "unit_test":
        return parts[1]
    return parts[0]


def failed(statuses: dict[str, str]) -> set[str]:
    return {node for node, status in statuses.items() if status in {"fail", "error"}}


def sabotage(database: Path, sql: str) -> None:
    with duckdb.connect(str(database)) as con:
        con.execute(sql)


def test_dbt_build_passes_on_the_recorded_data(database: Path) -> None:
    code, output, statuses = dbt("build", database)
    assert code == 0, output
    assert failed(statuses) == set()
    assert "ERROR=0" in output and "WARN=0" in output


def test_sources_are_fresh_after_a_load(database: Path) -> None:
    code, output, statuses = dbt("source freshness", database)
    assert code == 0, output
    assert set(statuses.values()) == {"pass"}


def test_marts_hold_the_expected_numbers(database: Path) -> None:
    code, output, _ = dbt("build", database)
    assert code == 0, output
    with duckdb.connect(str(database), read_only=True) as con:
        by_currency = dict(
            con.execute(
                """
                select currency_code, sum(cost)
                from main_marts.fct_sp_campaign_performance_daily
                group by currency_code
                """
            ).fetchall()
        )
        [(asins,)] = con.execute(
            """
            select count(distinct advertised_asin)
            from main_marts.fct_sp_product_performance_daily
            """
        ).fetchall()
    assert {currency: str(total) for currency, total in by_currency.items()} == {
        "USD": "550.010000000",
        "GBP": "236.890000000",
    }
    assert asins == 6


@pytest.mark.parametrize(
    ("defect", "expected_failures"),
    [
        pytest.param(
            # One product ad missing from its report window (for example a partial load).
            """
            delete from amazon_ads_raw.sp_advertised_product_daily
            where ad_id = 700000000000001 and date = date '2026-09-03'
            """,
            {RECONCILIATION},
            id="product-row-missing",
        ),
        pytest.param(
            # 2% more spend on the product side of one campaign-day: over the 0.5% limit.
            """
            update amazon_ads_raw.sp_advertised_product_daily
            set cost = cost * 1.02 + 0.05
            where ad_id = 700000000000002 and date = date '2026-09-05'
            """,
            {RECONCILIATION},
            id="spend-drift-over-tolerance",
        ),
        pytest.param(
            """
            insert into amazon_ads_raw.sp_campaign_daily
            select * from amazon_ads_raw.sp_campaign_daily limit 1
            """,
            {
                "unique_combination_stg_amazon_ads__sp_campaign_daily_profile_id__campaign_id__date_day"
            },
            id="duplicate-row",
        ),
        pytest.param(
            """
            update amazon_ads_raw.sp_campaign_daily
            set campaign_status = 'DELETED' where campaign_id = 500000000000003
            """,
            {
                "accepted_values_stg_amazon_ads__sp_campaign_daily_campaign_status__ENABLED__PAUSED__ARCHIVED"
            },
            id="unknown-status",
        ),
        pytest.param(
            # Reports loaded for a profile the profile list no longer has.
            f"delete from amazon_ads_raw.profiles where profile_id = {3000000000000002}",
            {
                "relationships_fct_sp_campaign_performance_daily_profile_id__profile_id__ref_stg_amazon_ads__profiles_",
                "not_null_fct_sp_campaign_performance_daily_currency_code",
                "not_null_fct_sp_product_performance_daily_currency_code",
            },
            id="profile-missing",
        ),
    ],
)
def test_data_tests_catch_the_defect(
    database: Path, defect: str, expected_failures: set[str]
) -> None:
    sabotage(database, defect)
    code, output, statuses = dbt("build", database)
    assert code != 0
    assert failed(statuses) == expected_failures, output


def test_rounding_differences_within_a_cent_pass(database: Path) -> None:
    sabotage(
        database,
        """
        update amazon_ads_raw.sp_advertised_product_daily
        set cost = cost + 0.004
        where ad_id = 700000000000002 and date = date '2026-09-05'
        """,
    )
    code, output, statuses = dbt("build", database)
    assert code == 0, output
    assert statuses[RECONCILIATION] == "pass"


def test_stale_data_fails_source_freshness(database: Path) -> None:
    sabotage(
        database,
        "update amazon_ads_raw.sp_advertised_product_daily set _loaded_at = now() - interval 3 day",
    )
    code, _, statuses = dbt("source freshness", database)
    assert code != 0
    assert statuses["sp_advertised_product_daily"] == "error"
    assert statuses["sp_campaign_daily"] == "pass"
