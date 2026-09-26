"""Raw table definitions shared by the BigQuery and DuckDB loaders (BigQuery types)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = True
    description: str = ""


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    partition_column: str
    cluster_columns: tuple[str, ...]
    description: str

    @property
    def data_columns(self) -> tuple[Column, ...]:
        """Columns supplied by the extractor. The loader stamps `_loaded_at` itself."""
        return tuple(c for c in self.columns if c.name != LOADED_AT.name)


LOADED_AT = Column("_loaded_at", "TIMESTAMP", description="When the pipeline loaded the row (UTC).")

_METRICS = (
    Column("impressions", "INT64"),
    Column("clicks", "INT64"),
    Column("cost", "NUMERIC", description="Spend in the profile's currency (see profiles)."),
    Column("purchases_7d", "INT64", description="Orders attributed within 7 days of a click."),
    Column("sales_7d", "NUMERIC", description="Sales attributed within 7 days of a click."),
    Column("units_sold_7d", "INT64", description="Report column unitsSoldClicks7d."),
    Column("purchases_14d", "INT64"),
    Column("sales_14d", "NUMERIC"),
)

SP_CAMPAIGN_DAILY = TableSpec(
    name="sp_campaign_daily",
    columns=(
        Column("date", "DATE", description="Report date, in the profile's time zone."),
        Column("profile_id", "INT64"),
        Column("campaign_id", "INT64"),
        Column("campaign_name", "STRING"),
        Column("campaign_status", "STRING"),
        Column("campaign_budget_amount", "NUMERIC", required=False),
        *_METRICS,
        LOADED_AT,
    ),
    partition_column="date",
    cluster_columns=("profile_id", "campaign_id"),
    description="Sponsored Products campaign report (spCampaigns, groupBy campaign, DAILY).",
)

SP_ADVERTISED_PRODUCT_DAILY = TableSpec(
    name="sp_advertised_product_daily",
    columns=(
        Column("date", "DATE"),
        Column("profile_id", "INT64"),
        Column("campaign_id", "INT64"),
        Column("ad_group_id", "INT64"),
        Column("ad_group_name", "STRING"),
        Column("ad_id", "INT64"),
        Column("advertised_asin", "STRING"),
        Column("advertised_sku", "STRING", required=False, description="Sellers only."),
        *_METRICS,
        LOADED_AT,
    ),
    partition_column="date",
    cluster_columns=("profile_id", "campaign_id", "advertised_asin"),
    description="Sponsored Products advertised product report (spAdvertisedProduct, DAILY).",
)

PROFILES = TableSpec(
    name="profiles",
    columns=(
        Column("snapshot_date", "DATE", description="Date (UTC) the profile list was read."),
        Column("profile_id", "INT64"),
        Column("region", "STRING", description="NA, EU or FE: the API host that serves it."),
        Column("country_code", "STRING"),
        Column("currency_code", "STRING"),
        Column("timezone", "STRING"),
        Column("marketplace_id", "STRING"),
        Column("account_id", "STRING", required=False),
        Column("account_type", "STRING", description="seller, vendor or agency."),
        Column("account_name", "STRING", required=False),
        LOADED_AT,
    ),
    partition_column="snapshot_date",
    cluster_columns=("profile_id",),
    description="Advertiser profiles (GET /v2/profiles), captured on each run.",
)

ALL_TABLES = (PROFILES, SP_CAMPAIGN_DAILY, SP_ADVERTISED_PRODUCT_DAILY)
