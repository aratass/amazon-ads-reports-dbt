"""Idempotent loaders: each load replaces one profile's date range, so re-runs never duplicate."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from amazon_ads_pipeline.reports import Row
from amazon_ads_pipeline.schemas import LOADED_AT, Column, TableSpec

log = logging.getLogger(__name__)


class Warehouse(Protocol):
    def ensure_tables(self, tables: Sequence[TableSpec]) -> None: ...

    def replace_range(
        self,
        table: TableSpec,
        rows: Sequence[Row],
        *,
        profile_id: int,
        start: date,
        end: date,
    ) -> None: ...


def check_rows(
    table: TableSpec, rows: Sequence[Row], *, profile_id: int, start: date, end: date
) -> None:
    """Refuse rows that do not belong to the profile and range being replaced.

    A row outside [start, end] would survive the next re-run's delete and be loaded a
    second time, so it is treated as a bug rather than loaded.
    """
    expected = {column.name for column in table.data_columns}
    for row in rows:
        if row.keys() != expected:
            raise ValueError(
                f"{table.name}: row columns {sorted(row)} "
                f"do not match the schema {sorted(expected)}"
            )
        day = row[table.partition_column]
        if row["profile_id"] != profile_id or not start <= day <= end:
            raise ValueError(
                f"{table.name}: row for profile {row['profile_id']} on {day} is outside "
                f"the range being replaced (profile {profile_id}, {start} to {end})"
            )


def bigquery_schema(columns: Sequence[Column]) -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(
            column.name,
            column.type,
            mode="REQUIRED" if column.required else "NULLABLE",
            description=column.description or None,
        )
        for column in columns
    ]


def to_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)  # NUMERIC as a string keeps every digit
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


class BigQueryWarehouse:
    """Loads into date-partitioned, clustered BigQuery tables.

    Rows go to a short-lived staging table first. One transaction then deletes the
    profile's rows for the range and inserts the staged rows, so a re-run replaces data
    instead of duplicating it and a failed run leaves the previous data in place.
    """

    def __init__(self, client: bigquery.Client, dataset: str, location: str | None = None) -> None:
        self._client = client
        self._dataset = f"{client.project}.{dataset}"
        self._location = location

    def table_id(self, name: str) -> str:
        return f"{self._dataset}.{name}"

    def ensure_tables(self, tables: Sequence[TableSpec]) -> None:
        try:
            self._client.get_dataset(self._dataset)
        except NotFound:  # creating needs project-level rights, so only try when missing
            dataset = bigquery.Dataset(self._dataset)
            if self._location:
                dataset.location = self._location
            self._client.create_dataset(dataset)
        for table in tables:
            bq_table = bigquery.Table(
                self.table_id(table.name), schema=bigquery_schema(table.columns)
            )
            bq_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=table.partition_column
            )
            bq_table.clustering_fields = list(table.cluster_columns)
            bq_table.description = table.description
            self._client.create_table(bq_table, exists_ok=True)

    def replace_range(
        self,
        table: TableSpec,
        rows: Sequence[Row],
        *,
        profile_id: int,
        start: date,
        end: date,
    ) -> None:
        check_rows(table, rows, profile_id=profile_id, start=start, end=end)
        target = self.table_id(table.name)
        params = [
            bigquery.ScalarQueryParameter("profile_id", "INT64", profile_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ]
        delete = (
            f"DELETE FROM `{target}`\n"
            f"WHERE profile_id = @profile_id\n"
            f"  AND {table.partition_column} BETWEEN @start_date AND @end_date"
        )
        if not rows:
            self._run(delete, params)
            log.info("Cleared %s for profile %s, %s to %s", target, profile_id, start, end)
            return

        staging = self.table_id(f"_staging_{table.name}_{uuid.uuid4().hex[:12]}")
        columns = ", ".join(column.name for column in table.data_columns)
        try:
            self._stage(table, rows, staging)
            self._run(
                "BEGIN TRANSACTION;\n"
                f"{delete};\n"
                f"INSERT INTO `{target}` ({columns}, {LOADED_AT.name})\n"
                f"SELECT {columns}, CURRENT_TIMESTAMP() FROM `{staging}`;\n"
                "COMMIT TRANSACTION;",
                params,
            )
        finally:
            self._client.delete_table(staging, not_found_ok=True)
        log.info("Loaded %d rows into %s for %s to %s", len(rows), target, start, end)

    def _stage(self, table: TableSpec, rows: Sequence[Row], staging: str) -> None:
        schema = bigquery_schema(table.data_columns)
        staging_table = bigquery.Table(staging, schema=schema)
        # Safety net: the table is dropped after the swap, but expires anyway if we crash.
        staging_table.expires = datetime.now(timezone.utc) + timedelta(hours=6)
        self._client.create_table(staging_table)
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        rows_json = [{key: to_json_value(value) for key, value in row.items()} for row in rows]
        self._client.load_table_from_json(rows_json, staging, job_config=job_config).result()

    def _run(self, sql: str, params: list[bigquery.ScalarQueryParameter]) -> None:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self._client.query(sql, job_config=job_config, location=self._location).result()


_DUCKDB_TYPES = {
    "INT64": "BIGINT",
    "STRING": "VARCHAR",
    "DATE": "DATE",
    "NUMERIC": "DECIMAL(38, 9)",  # BigQuery NUMERIC precision and scale
    "FLOAT64": "DOUBLE",
    "TIMESTAMP": "TIMESTAMPTZ",
    "BOOL": "BOOLEAN",
}


class DuckDBWarehouse:
    """The same contract as BigQueryWarehouse on a local DuckDB file, for demos and tests."""

    def __init__(self, path: str | Path, schema: str = "amazon_ads_raw") -> None:
        import duckdb  # local runs only; production never imports it

        self._con = duckdb.connect(str(path))
        self._schema = schema

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> DuckDBWarehouse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def ensure_tables(self, tables: Sequence[TableSpec]) -> None:
        self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        for table in tables:
            columns = ", ".join(
                f"{c.name} {_DUCKDB_TYPES[c.type]}{' NOT NULL' if c.required else ''}"
                for c in table.columns
            )
            self._con.execute(f"CREATE TABLE IF NOT EXISTS {self._schema}.{table.name} ({columns})")

    def replace_range(
        self,
        table: TableSpec,
        rows: Sequence[Row],
        *,
        profile_id: int,
        start: date,
        end: date,
    ) -> None:
        check_rows(table, rows, profile_id=profile_id, start=start, end=end)
        target = f"{self._schema}.{table.name}"
        names = [column.name for column in table.data_columns]
        placeholders = ", ".join("?" for _ in names)
        self._con.begin()
        try:
            self._con.execute(
                f"DELETE FROM {target} "
                f"WHERE profile_id = ? AND {table.partition_column} BETWEEN ? AND ?",
                [profile_id, start, end],
            )
            if rows:
                self._con.executemany(
                    f"INSERT INTO {target} ({', '.join(names)}, {LOADED_AT.name}) "
                    f"VALUES ({placeholders}, now())",
                    [[row[name] for name in names] for row in rows],
                )
            self._con.commit()
        except Exception:
            self._con.rollback()
            raise
        log.info("Loaded %d rows into %s for %s to %s", len(rows), target, start, end)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        return self._con.execute(sql, list(params)).fetchall()
