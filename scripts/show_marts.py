"""Print the marts of a local DuckDB build as Markdown tables (used for the README sample).

Usage: python scripts/show_marts.py local.duckdb
"""

from __future__ import annotations

import sys

import duckdb

QUERIES = {
    "fct_campaign_performance_daily (2026-09-14)": """
        select date_day, campaign_name, campaign_status, impressions, clicks,
               cast(cost as decimal(18, 2)) as cost, conversions, round(ctr, 4) as ctr,
               round(cpc, 2) as cpc, round(roas, 2) as roas
        from main_marts.fct_campaign_performance_daily
        where date_day = date '2026-09-14'
        order by cost desc
    """,
    "fct_budget_pacing_daily (2026-09-14)": """
        select date_day, budget_name, budget_is_shared as shared,
               cast(daily_budget as decimal(18, 2)) as daily_budget,
               cast(spend as decimal(18, 2)) as spend,
               cast(mtd_spend as decimal(18, 2)) as mtd_spend,
               cast(mtd_budget as decimal(18, 2)) as mtd_budget,
               pacing_ratio,
               cast(projected_month_spend as decimal(18, 2)) as projected_month_spend,
               cast(month_budget as decimal(18, 2)) as month_budget,
               pacing_status
        from main_marts.fct_budget_pacing_daily
        where date_day = date '2026-09-14'
        order by budget_id
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
