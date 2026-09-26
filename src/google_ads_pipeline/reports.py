"""GAQL report definitions and row mappers (Google Ads API v25)."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from google_ads_pipeline import schemas
from google_ads_pipeline.schemas import TableSpec

Row = dict[str, Any]

MICROS_PER_UNIT = Decimal(1_000_000)


def micros_to_units(micros: int) -> Decimal:
    """Convert an API micros amount to currency units without floating-point error.

    The API reports money as int64 micros: 1_500_000 micros is 1.50 in the account
    currency. Decimal division keeps every digit, so 1_234_567 becomes 1.234567.
    """
    return Decimal(micros) / MICROS_PER_UNIT


def normalize_customer_id(customer_id: str | int) -> str:
    """Return a customer ID as the 10 digits the API expects ("123-456-7890" -> "1234567890")."""
    digits = str(customer_id).replace("-", "").strip()
    if not re.fullmatch(r"\d{10}", digits):
        raise ValueError(f"Invalid Google Ads customer ID {customer_id!r}: expected 10 digits")
    return digits


@dataclass(frozen=True)
class Report:
    """A GAQL query and the mapping of its rows onto a raw table (None: read, not loaded)."""

    name: str
    resource: str
    fields: tuple[str, ...]
    table: TableSpec | None
    to_row: Callable[[Any], Row]
    segmented_by_date: bool = True

    def query(self, start: date | None = None, end: date | None = None) -> str:
        select = ",\n  ".join(self.fields)
        gaql = f"SELECT\n  {select}\nFROM {self.resource}"
        if self.segmented_by_date:
            if start is None or end is None:
                raise ValueError(f"Report {self.name!r} needs a start and an end date")
            if start > end:
                raise ValueError(f"Start date {start} is after end date {end}")
            gaql += f"\nWHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'"
        return gaql


def _enum_name(value: Any) -> str:
    # proto-plus returns enum members; a value newer than this library returns a bare int.
    return getattr(value, "name", str(value))


_METRIC_FIELDS = (
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
    "metrics.conversions",
    "metrics.conversions_value",
)


def _metrics(row: Any) -> Row:
    metrics = row.metrics
    return {
        "impressions": metrics.impressions,
        "clicks": metrics.clicks,
        "cost_micros": metrics.cost_micros,
        "cost": micros_to_units(metrics.cost_micros),
        "conversions": metrics.conversions,
        "conversions_value": metrics.conversions_value,
    }


def _account_daily_row(row: Any) -> Row:
    return {
        "date": date.fromisoformat(row.segments.date),
        "customer_id": row.customer.id,
        "account_name": row.customer.descriptive_name or None,
        "currency_code": row.customer.currency_code,
        "time_zone": row.customer.time_zone,
        **_metrics(row),
    }


def _campaign_daily_row(row: Any) -> Row:
    return {
        "date": date.fromisoformat(row.segments.date),
        "customer_id": row.customer.id,
        "campaign_id": row.campaign.id,
        "campaign_name": row.campaign.name,
        "campaign_status": _enum_name(row.campaign.status),
        "advertising_channel_type": _enum_name(row.campaign.advertising_channel_type),
        **_metrics(row),
    }


def _ad_group_daily_row(row: Any) -> Row:
    return {
        "date": date.fromisoformat(row.segments.date),
        "customer_id": row.customer.id,
        "campaign_id": row.campaign.id,
        "ad_group_id": row.ad_group.id,
        "ad_group_name": row.ad_group.name,
        "ad_group_status": _enum_name(row.ad_group.status),
        "ad_group_type": _enum_name(row.ad_group.type_),
        **_metrics(row),
    }


def _account_time_zone_row(row: Any) -> Row:
    return {"customer_id": row.customer.id, "time_zone": row.customer.time_zone}


def _campaign_budget_row(row: Any) -> Row:
    budget = row.campaign_budget
    # amount_micros is unset for CUSTOM_PERIOD (total) budgets.
    amount_micros = budget.amount_micros if "amount_micros" in budget else None
    return {
        "customer_id": row.customer.id,
        "campaign_id": row.campaign.id,
        "campaign_status": _enum_name(row.campaign.status),
        "budget_id": budget.id,
        "budget_name": budget.name or None,
        "budget_period": _enum_name(budget.period),
        "budget_amount_micros": amount_micros,
        "budget_amount": None if amount_micros is None else micros_to_units(amount_micros),
        "budget_is_shared": budget.explicitly_shared,
    }


ACCOUNT_DAILY_REPORT = Report(
    name="account_daily",
    resource="customer",
    fields=(
        "segments.date",
        "customer.id",
        "customer.descriptive_name",
        "customer.currency_code",
        "customer.time_zone",
        *_METRIC_FIELDS,
    ),
    table=schemas.ACCOUNT_DAILY,
    to_row=_account_daily_row,
)

# No status filter on purpose: a removed campaign still owns the spend from the days it
# ran, and filtering it out would break reconciliation against account-level totals.
CAMPAIGN_DAILY_REPORT = Report(
    name="campaign_daily",
    resource="campaign",
    fields=(
        "segments.date",
        "customer.id",
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "campaign.advertising_channel_type",
        *_METRIC_FIELDS,
    ),
    table=schemas.CAMPAIGN_DAILY,
    to_row=_campaign_daily_row,
)

AD_GROUP_DAILY_REPORT = Report(
    name="ad_group_daily",
    resource="ad_group",
    fields=(
        "segments.date",
        "customer.id",
        "campaign.id",
        "ad_group.id",
        "ad_group.name",
        "ad_group.status",
        "ad_group.type",
        *_METRIC_FIELDS,
    ),
    table=schemas.AD_GROUP_DAILY,
    to_row=_ad_group_daily_row,
)

# Attribute fields return their current value only, never the value on a past date,
# so budgets are snapshotted on every run and history is rebuilt from the snapshots.
CAMPAIGN_BUDGET_REPORT = Report(
    name="campaign_budget_snapshot",
    resource="campaign",
    fields=(
        "customer.id",
        "campaign.id",
        "campaign.status",
        "campaign_budget.id",
        "campaign_budget.name",
        "campaign_budget.period",
        "campaign_budget.amount_micros",
        "campaign_budget.explicitly_shared",
    ),
    table=schemas.CAMPAIGN_BUDGET_SNAPSHOT,
    to_row=_campaign_budget_row,
    segmented_by_date=False,
)

# Read, not loaded. segments.date is a day in the account's time zone, so the default
# window ("up to yesterday") and the budget snapshot date are worked out in that zone.
ACCOUNT_TIME_ZONE_REPORT = Report(
    name="account_time_zone",
    resource="customer",
    fields=("customer.id", "customer.time_zone"),
    table=None,
    to_row=_account_time_zone_row,
    segmented_by_date=False,
)

DAILY_REPORTS = (ACCOUNT_DAILY_REPORT, CAMPAIGN_DAILY_REPORT, AD_GROUP_DAILY_REPORT)
ALL_REPORTS = (*DAILY_REPORTS, CAMPAIGN_BUDGET_REPORT)  # the reports loaded into tables
KNOWN_REPORTS = (*ALL_REPORTS, ACCOUNT_TIME_ZONE_REPORT)

_SELECT_FROM = re.compile(r"SELECT\s+(?P<fields>.+?)\s+FROM\s+(?P<resource>\w+)", re.S | re.I)
_DATE_RANGE = re.compile(
    r"segments\.date\s+BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'"
)


def report_for_query(query: str) -> Report:
    """Identify the report a GAQL string was built from (used by replay and recording)."""
    match = _SELECT_FROM.search(query)
    if match:
        fields = tuple(field.strip() for field in match["fields"].split(","))
        for report in KNOWN_REPORTS:
            if report.fields == fields and report.resource == match["resource"]:
                return report
    raise LookupError(f"Query does not match a known report:\n{query}")


def date_range_of(query: str) -> tuple[date, date] | None:
    """Return the segments.date range a query filters on, if any."""
    match = _DATE_RANGE.search(query)
    if match is None:
        return None
    return date.fromisoformat(match[1]), date.fromisoformat(match[2])
