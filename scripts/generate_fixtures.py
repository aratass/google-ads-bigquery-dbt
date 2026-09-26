"""Generate the synthetic Google Ads recordings used by the tests and the offline demo.

The data is invented, but every file is built from the google-ads v25 message classes and
written in the API's JSON wire format, exactly as `gads-pipeline --record` saves a live
response. Account totals equal the sum of the campaigns, as they do in the real API.

Usage: python scripts/generate_fixtures.py [output_dir]
"""

from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from google.ads.googleads.v25.enums.types.ad_group_status import AdGroupStatusEnum
from google.ads.googleads.v25.enums.types.ad_group_type import AdGroupTypeEnum
from google.ads.googleads.v25.enums.types.advertising_channel_type import (
    AdvertisingChannelTypeEnum,
)
from google.ads.googleads.v25.enums.types.budget_period import BudgetPeriodEnum
from google.ads.googleads.v25.enums.types.campaign_status import CampaignStatusEnum
from google.ads.googleads.v25.services.types.google_ads_service import (
    GoogleAdsRow,
    SearchGoogleAdsStreamResponse,
)
from google.protobuf.field_mask_pb2 import FieldMask

from google_ads_pipeline.replay import dump_batches, recording_path
from google_ads_pipeline.reports import (
    ACCOUNT_DAILY_REPORT,
    ACCOUNT_TIME_ZONE_REPORT,
    AD_GROUP_DAILY_REPORT,
    CAMPAIGN_BUDGET_REPORT,
    CAMPAIGN_DAILY_REPORT,
    Report,
)

CUSTOMER_ID = 1234567890
TIME_ZONE = "America/New_York"
FIRST_DAY = date(2026, 9, 1)
LAST_DAY = date(2026, 9, 14)
BATCH_SIZE = 40  # the live API streams 10,000 rows per batch; small batches exercise paging


@dataclass(frozen=True)
class Campaign:
    id: int
    name: str
    channel: str
    status: str
    budget_id: int
    budget_name: str
    daily_budget: float
    shared_budget: bool
    first_day: date
    last_day: date
    daily_spend: float
    cpc: float
    conversion_rate: float
    order_value: float
    ad_groups: tuple[tuple[int, str, str], ...]  # (id, name, status); none for Performance Max


CAMPAIGNS = (
    Campaign(
        id=20111111111,
        name="Brand | Search",
        channel="SEARCH",
        status="ENABLED",
        budget_id=30001,
        budget_name="Brand search (shared)",
        daily_budget=60.0,
        shared_budget=True,
        first_day=FIRST_DAY,
        last_day=LAST_DAY,
        daily_spend=34.0,
        cpc=0.55,
        conversion_rate=0.12,
        order_value=85.0,
        ad_groups=(
            (40111111101, "Brand - exact", "ENABLED"),
            (40111111102, "Brand - phrase", "ENABLED"),
        ),
    ),
    # Removed on the 3rd, but its spend from the 1st to the 3rd still counts.
    Campaign(
        id=20111111112,
        name="Brand | Search | Legacy",
        channel="SEARCH",
        status="REMOVED",
        budget_id=30001,
        budget_name="Brand search (shared)",
        daily_budget=60.0,
        shared_budget=True,
        first_day=FIRST_DAY,
        last_day=date(2026, 9, 3),
        daily_spend=12.0,
        cpc=0.60,
        conversion_rate=0.10,
        order_value=85.0,
        ad_groups=((40111111103, "Brand legacy", "REMOVED"),),
    ),
    Campaign(
        id=20111111113,
        name="Generic | Search | Tents",
        channel="SEARCH",
        status="ENABLED",
        budget_id=30002,
        budget_name="Generic tents",
        daily_budget=150.0,
        shared_budget=False,
        first_day=FIRST_DAY,
        last_day=LAST_DAY,
        daily_spend=142.0,
        cpc=1.85,
        conversion_rate=0.032,
        order_value=210.0,
        ad_groups=(
            (40111111104, "Tents - 2 person", "ENABLED"),
            (40111111105, "Tents - family", "ENABLED"),
        ),
    ),
    # Spends above its daily budget, which Google allows on individual days.
    Campaign(
        id=20111111114,
        name="PMax | All products",
        channel="PERFORMANCE_MAX",
        status="ENABLED",
        budget_id=30003,
        budget_name="PMax all products",
        daily_budget=220.0,
        shared_budget=False,
        first_day=FIRST_DAY,
        last_day=LAST_DAY,
        daily_spend=246.0,
        cpc=0.95,
        conversion_rate=0.041,
        order_value=160.0,
        ad_groups=(),
    ),
    # Paused after the 7th.
    Campaign(
        id=20111111115,
        name="Display | Remarketing",
        channel="DISPLAY",
        status="PAUSED",
        budget_id=30004,
        budget_name="Display remarketing",
        daily_budget=40.0,
        shared_budget=False,
        first_day=FIRST_DAY,
        last_day=date(2026, 9, 7),
        daily_spend=38.0,
        cpc=0.42,
        conversion_rate=0.018,
        order_value=120.0,
        ad_groups=((40111111106, "Remarketing - 30 day visitors", "ENABLED"),),
    ),
)


@dataclass
class Metrics:
    impressions: int = 0
    clicks: int = 0
    cost_micros: int = 0
    conversions: float = 0.0
    conversions_value: float = 0.0

    def add(self, other: Metrics) -> None:
        self.impressions += other.impressions
        self.clicks += other.clicks
        self.cost_micros += other.cost_micros
        self.conversions = round(self.conversions + other.conversions, 2)
        self.conversions_value = round(self.conversions_value + other.conversions_value, 2)


def _day_metrics(rng: random.Random, campaign: Campaign) -> Metrics:
    cost_cents = round(campaign.daily_spend * rng.uniform(0.85, 1.15) * 100)
    clicks = max(1, round(cost_cents / 100 / (campaign.cpc * rng.uniform(0.9, 1.1))))
    conversions = round(clicks * campaign.conversion_rate * rng.uniform(0.7, 1.3), 2)
    return Metrics(
        impressions=round(clicks / rng.uniform(0.02, 0.09)),
        clicks=clicks,
        cost_micros=cost_cents * 10_000,  # billed amounts are whole cents
        conversions=conversions,
        conversions_value=round(conversions * campaign.order_value * rng.uniform(0.9, 1.1), 2),
    )


def _split(total: int, weights: list[float]) -> list[int]:
    """Split an integer so the parts add up exactly to the total."""
    parts = [int(total * w / sum(weights)) for w in weights]
    parts[0] += total - sum(parts)
    return parts


def _set_metrics(row: GoogleAdsRow, metrics: Metrics) -> None:
    row.metrics.impressions = metrics.impressions
    row.metrics.clicks = metrics.clicks
    row.metrics.cost_micros = metrics.cost_micros
    row.metrics.conversions = metrics.conversions
    row.metrics.conversions_value = metrics.conversions_value


def build_rows() -> dict[str, list[GoogleAdsRow]]:
    rng = random.Random(20260901)
    rows: dict[str, list[GoogleAdsRow]] = {report.name: [] for report in _REPORTS}
    day = FIRST_DAY
    while day <= LAST_DAY:
        account = Metrics()
        for campaign in CAMPAIGNS:
            if not campaign.first_day <= day <= campaign.last_day:
                continue
            metrics = _day_metrics(rng, campaign)
            account.add(metrics)
            row = GoogleAdsRow()
            row.segments.date = day.isoformat()
            row.customer.id = CUSTOMER_ID
            row.campaign.id = campaign.id
            row.campaign.name = campaign.name
            row.campaign.status = CampaignStatusEnum.CampaignStatus[campaign.status]
            row.campaign.advertising_channel_type = (
                AdvertisingChannelTypeEnum.AdvertisingChannelType[campaign.channel]
            )
            _set_metrics(row, metrics)
            rows[CAMPAIGN_DAILY_REPORT.name].append(row)
            _add_ad_group_rows(rng, rows[AD_GROUP_DAILY_REPORT.name], campaign, day, metrics)

        row = GoogleAdsRow()
        row.segments.date = day.isoformat()
        row.customer.id = CUSTOMER_ID
        row.customer.descriptive_name = "Acme Outdoor (demo)"
        row.customer.currency_code = "USD"
        row.customer.time_zone = TIME_ZONE
        _set_metrics(row, account)
        rows[ACCOUNT_DAILY_REPORT.name].append(row)
        day += timedelta(days=1)

    for campaign in CAMPAIGNS:
        row = GoogleAdsRow()
        row.customer.id = CUSTOMER_ID
        row.campaign.id = campaign.id
        row.campaign.status = CampaignStatusEnum.CampaignStatus[campaign.status]
        row.campaign_budget.id = campaign.budget_id
        row.campaign_budget.name = campaign.budget_name
        row.campaign_budget.period = BudgetPeriodEnum.BudgetPeriod.DAILY
        row.campaign_budget.amount_micros = round(campaign.daily_budget * 1_000_000)
        row.campaign_budget.explicitly_shared = campaign.shared_budget
        rows[CAMPAIGN_BUDGET_REPORT.name].append(row)

    row = GoogleAdsRow()
    row.customer.id = CUSTOMER_ID
    row.customer.time_zone = TIME_ZONE
    rows[ACCOUNT_TIME_ZONE_REPORT.name].append(row)
    return rows


def _add_ad_group_rows(
    rng: random.Random, out: list[GoogleAdsRow], campaign: Campaign, day: date, metrics: Metrics
) -> None:
    if not campaign.ad_groups:
        return
    weights = [rng.uniform(0.3, 1.0) for _ in campaign.ad_groups]
    shares = zip(
        campaign.ad_groups,
        _split(metrics.impressions, weights),
        _split(metrics.clicks, weights),
        _split(metrics.cost_micros // 10_000, weights),
        strict=True,
    )
    for (ad_group_id, name, status), impressions, clicks, cost_cents in shares:
        share = cost_cents / max(1, metrics.cost_micros // 10_000)
        row = GoogleAdsRow()
        row.segments.date = day.isoformat()
        row.customer.id = CUSTOMER_ID
        row.campaign.id = campaign.id
        row.ad_group.id = ad_group_id
        row.ad_group.name = name
        row.ad_group.status = AdGroupStatusEnum.AdGroupStatus[status]
        row.ad_group.type_ = AdGroupTypeEnum.AdGroupType[
            "DISPLAY_STANDARD" if campaign.channel == "DISPLAY" else "SEARCH_STANDARD"
        ]
        _set_metrics(
            row,
            Metrics(
                impressions=impressions,
                clicks=clicks,
                cost_micros=cost_cents * 10_000,
                conversions=round(metrics.conversions * share, 2),
                conversions_value=round(metrics.conversions_value * share, 2),
            ),
        )
        out.append(row)


_REPORTS: tuple[Report, ...] = (
    ACCOUNT_DAILY_REPORT,
    CAMPAIGN_DAILY_REPORT,
    AD_GROUP_DAILY_REPORT,
    CAMPAIGN_BUDGET_REPORT,
    ACCOUNT_TIME_ZONE_REPORT,  # last, so the request ids of the files above stay the same
)


def write_fixtures(directory: Path) -> list[Path]:
    rows = build_rows()
    written = []
    for index, report in enumerate(_REPORTS):
        report_rows = rows[report.name]
        batches = [
            SearchGoogleAdsStreamResponse(
                results=report_rows[start : start + BATCH_SIZE],
                field_mask=FieldMask(paths=list(report.fields)),
                request_id=f"demo-request-{index}-{start // BATCH_SIZE}",
            )
            for start in range(0, len(report_rows), BATCH_SIZE)
        ]
        path = recording_path(directory, report.name, str(CUSTOMER_ID))
        dump_batches(batches, path)
        written.append(path)
    return written


if __name__ == "__main__":
    default = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "google_ads"
    for path in write_fixtures(Path(sys.argv[1]) if len(sys.argv) > 1 else default):
        print(f"wrote {path}")
