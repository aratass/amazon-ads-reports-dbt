"""Print the marts of a local DuckDB build as Markdown tables (used for the README sample).

Usage: python scripts/show_marts.py local.duckdb
"""

from __future__ import annotations

import sys

import duckdb

QUERIES = {
    "fct_sp_campaign_performance_daily (2026-09-07)": """
        select date_day, country_code as country, currency_code as currency, campaign_name,
               campaign_status, impressions, clicks, cast(cost as decimal(18, 2)) as cost,
               purchases_7d, cast(sales_7d as decimal(18, 2)) as sales_7d,
               round(acos_7d, 4) as acos_7d, round(roas_7d, 2) as roas_7d,
               round(budget_utilization, 2) as budget_utilization
        from main_marts.fct_sp_campaign_performance_daily
        where date_day = date '2026-09-07'
        order by profile_id, cost desc
    """,
    "fct_sp_product_performance_daily (2026-09-07, US)": """
        select date_day, advertised_asin, campaigns, ads, impressions, clicks,
               cast(cost as decimal(18, 2)) as cost, purchases_7d,
               cast(sales_7d as decimal(18, 2)) as sales_7d, round(acos_7d, 4) as acos_7d
        from main_marts.fct_sp_product_performance_daily
        where date_day = date '2026-09-07' and currency_code = 'USD'
        order by cost desc
    """,
}


def _format(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}"
    return str(value).replace("|", "\\|")  # keep Markdown table cells intact


def main(path: str) -> None:
    with duckdb.connect(path, read_only=True) as con:
        for title, sql in QUERIES.items():
            cursor = con.execute(sql)
            headers = [column[0] for column in cursor.description]
            print(f"\n{title}\n")
            print("| " + " | ".join(headers) + " |")
            print("|" + "|".join("---" for _ in headers) + "|")
            for row in cursor.fetchall():
                print("| " + " | ".join(_format(value) for value in row) + " |")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "local.duckdb")
