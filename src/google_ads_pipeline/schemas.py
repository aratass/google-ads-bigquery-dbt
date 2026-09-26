"""Raw table definitions shared by the BigQuery and DuckDB loaders.

Types are BigQuery standard SQL types. The DuckDB loader maps them to their
DuckDB equivalents, so local runs and production have the same raw schema.
"""

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
    Column(
        "cost_micros", "INT64", description="Cost in micros of the account currency, as returned."
    ),
    Column("cost", "NUMERIC", description="cost_micros / 1,000,000 as an exact decimal."),
    Column(
        "conversions", "FLOAT64", description="Can be fractional under data-driven attribution."
    ),
    Column("conversions_value", "FLOAT64"),
)

ACCOUNT_DAILY = TableSpec(
    name="account_daily",
    columns=(
        Column("date", "DATE", description="segments.date, in the account time zone."),
        Column("customer_id", "INT64"),
        Column("account_name", "STRING", required=False),
        Column("currency_code", "STRING"),
        Column("time_zone", "STRING"),
        *_METRICS,
        LOADED_AT,
    ),
    partition_column="date",
    cluster_columns=("customer_id",),
    description="Account-level daily performance (GAQL: FROM customer).",
)

CAMPAIGN_DAILY = TableSpec(
    name="campaign_daily",
    columns=(
        Column("date", "DATE"),
        Column("customer_id", "INT64"),
        Column("campaign_id", "INT64"),
        Column("campaign_name", "STRING"),
        Column("campaign_status", "STRING"),
        Column("advertising_channel_type", "STRING"),
        *_METRICS,
        LOADED_AT,
    ),
    partition_column="date",
    cluster_columns=("customer_id", "campaign_id"),
    description="Campaign-level daily performance, removed campaigns included.",
)

AD_GROUP_DAILY = TableSpec(
    name="ad_group_daily",
    columns=(
        Column("date", "DATE"),
        Column("customer_id", "INT64"),
        Column("campaign_id", "INT64"),
        Column("ad_group_id", "INT64"),
        Column("ad_group_name", "STRING"),
        Column("ad_group_status", "STRING"),
        Column("ad_group_type", "STRING"),
        *_METRICS,
        LOADED_AT,
    ),
    partition_column="date",
    cluster_columns=("customer_id", "campaign_id", "ad_group_id"),
    description="Ad group daily performance. Performance Max campaigns have no ad groups.",
)

CAMPAIGN_BUDGET_SNAPSHOT = TableSpec(
    name="campaign_budget_snapshot",
    columns=(
        Column(
            "snapshot_date",
            "DATE",
            description="Date the budget state was captured, in the account time zone.",
        ),
        Column("customer_id", "INT64"),
        Column("campaign_id", "INT64"),
        Column("campaign_status", "STRING"),
        Column("budget_id", "INT64"),
        Column("budget_name", "STRING", required=False),
        Column("budget_period", "STRING"),
        Column("budget_amount_micros", "INT64", required=False),
        Column("budget_amount", "NUMERIC", required=False),
        Column("budget_is_shared", "BOOL"),
        LOADED_AT,
    ),
    partition_column="snapshot_date",
    cluster_columns=("customer_id", "campaign_id"),
    description=(
        "Daily snapshot of each campaign's budget. GAQL returns current attribute values only, "
        "so budget history has to be captured by snapshotting it on every run."
    ),
)

ALL_TABLES = (ACCOUNT_DAILY, CAMPAIGN_DAILY, AD_GROUP_DAILY, CAMPAIGN_BUDGET_SNAPSHOT)
