"""Extract every report for each customer and load it idempotently."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from google_ads_pipeline.extract import GoogleAdsExtractor
from google_ads_pipeline.reports import (
    ACCOUNT_TIME_ZONE_REPORT,
    CAMPAIGN_BUDGET_REPORT,
    DAILY_REPORTS,
    normalize_customer_id,
)
from google_ads_pipeline.schemas import ALL_TABLES
from google_ads_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 30  # Google's default conversion window is 30 days


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LoadResult:
    customer_id: str
    table: str
    start: date
    end: date
    rows: int


@dataclass(frozen=True)
class Window:
    """The days one account's run covers and the date its budget snapshot is filed under."""

    start: date
    end: date
    snapshot_date: date


class PipelineError(RuntimeError):
    """Some accounts failed to load; the others were loaded."""


def local_window(
    time_zone: str,
    now: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    start: date | None = None,
    end: date | None = None,
    snapshot_date: date | None = None,
) -> Window:
    """Fill in the dates left out, in the account's own time zone.

    Google Ads reports segments.date in the account time zone. At 02:30 UTC it is still
    the previous evening in New York, so "yesterday in UTC" would be the account's
    unfinished today, loaded as if it were complete. The default end is therefore the
    account's yesterday, and the budget snapshot is filed under the account's today.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local_today = now.astimezone(ZoneInfo(time_zone)).date()
    end = end or local_today - timedelta(days=1)
    start = start or end - timedelta(days=lookback_days - 1)
    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    return Window(start, end, snapshot_date or local_today)


def run(
    extractor: GoogleAdsExtractor,
    warehouse: Warehouse,
    customer_ids: Iterable[str],
    start: date | None = None,
    end: date | None = None,
    snapshot_date: date | None = None,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now: Callable[[], datetime] | None = None,
) -> list[LoadResult]:
    """Load `start`..`end` (inclusive) for every customer, plus a snapshot of its budgets.

    Dates left out are worked out per account in the account's time zone (see
    `local_window`): `end` defaults to the account's yesterday, `start` to `lookback_days`
    days before it, and `snapshot_date` to the account's today. That costs one extra
    GAQL request per account, to read its time zone.

    Every load replaces exactly the customer and date range it covers, so a run can be
    repeated or overlapped with earlier runs (for example a rolling 30-day window that
    picks up late conversions) without creating duplicates. One failing account does not
    stop the others; failures are raised together at the end.
    """
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be at least 1, not {lookback_days}")
    if start is not None and end is not None and start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    customers = [normalize_customer_id(raw_id) for raw_id in customer_ids]
    moment = (now or utc_now)()
    warehouse.ensure_tables(ALL_TABLES)
    results: list[LoadResult] = []
    failures: list[str] = []
    for customer_id in customers:
        try:
            window = _window(
                extractor, customer_id, start, end, snapshot_date, lookback_days, moment
            )
            results.extend(_load_customer(extractor, warehouse, customer_id, window))
        except Exception as error:
            log.exception("Customer %s failed", customer_id)
            failures.append(f"{customer_id}: {type(error).__name__}: {error}")
    if failures:
        raise PipelineError("Accounts that failed to load: " + "; ".join(failures))
    return results


def _window(
    extractor: GoogleAdsExtractor,
    customer_id: str,
    start: date | None,
    end: date | None,
    snapshot_date: date | None,
    lookback_days: int,
    now: datetime,
) -> Window:
    if end is not None and snapshot_date is not None:
        # Every date is given: no need to ask the API for the account's time zone.
        return Window(start or end - timedelta(days=lookback_days - 1), end, snapshot_date)
    rows = extractor.fetch(ACCOUNT_TIME_ZONE_REPORT, customer_id)
    if len(rows) != 1:
        raise LookupError(f"Expected one row with the account time zone, got {len(rows)}")
    window = local_window(rows[0]["time_zone"], now, lookback_days, start, end, snapshot_date)
    log.info(
        "Customer %s (%s): %s to %s, budget snapshot %s",
        customer_id,
        rows[0]["time_zone"],
        window.start,
        window.end,
        window.snapshot_date,
    )
    return window


def _load_customer(
    extractor: GoogleAdsExtractor,
    warehouse: Warehouse,
    customer_id: str,
    window: Window,
) -> list[LoadResult]:
    results = []
    for report in DAILY_REPORTS:
        rows = extractor.fetch(report, customer_id, window.start, window.end)
        warehouse.replace_range(
            report.table, rows, customer_id=int(customer_id), start=window.start, end=window.end
        )
        results.append(
            LoadResult(customer_id, report.table.name, window.start, window.end, len(rows))
        )

    snapshot = window.snapshot_date
    budgets = [
        {**row, "snapshot_date": snapshot}
        for row in extractor.fetch(CAMPAIGN_BUDGET_REPORT, customer_id)
    ]
    table = CAMPAIGN_BUDGET_REPORT.table
    warehouse.replace_range(
        table, budgets, customer_id=int(customer_id), start=snapshot, end=snapshot
    )
    results.append(LoadResult(customer_id, table.name, snapshot, snapshot, len(budgets)))
    return results
