import json
import shutil
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from google_ads_pipeline import pipeline
from google_ads_pipeline.backoff import RetryPolicy
from google_ads_pipeline.cli import main
from google_ads_pipeline.extract import GoogleAdsExtractor
from google_ads_pipeline.pipeline import PipelineError, local_window, run
from google_ads_pipeline.replay import RecordingGoogleAdsService, ReplayGoogleAdsService
from google_ads_pipeline.reports import report_for_query
from google_ads_pipeline.warehouse import DuckDBWarehouse

from support import CUSTOMER_ID, FIXTURES, load_script

SEPT_1, SEPT_14, SEPT_15 = date(2026, 9, 1), date(2026, 9, 14), date(2026, 9, 15)
# 02:30 UTC on the 15th is 22:30 on the 14th in New York, the fixture account's time zone.
LATE_EVENING_IN_NEW_YORK = datetime(2026, 9, 15, 2, 30, tzinfo=timezone.utc)


class CountingService:
    """Replays the fixtures and records the name of every report requested."""

    def __init__(self) -> None:
        self._replay = ReplayGoogleAdsService(FIXTURES)
        self.reports: list[str] = []

    def search_stream(self, *, customer_id: str, query: str):
        self.reports.append(report_for_query(query).name)
        return self._replay.search_stream(customer_id=customer_id, query=query)


@pytest.fixture
def warehouse(tmp_path):
    with DuckDBWarehouse(tmp_path / "pipeline.duckdb") as duckdb_warehouse:
        yield duckdb_warehouse


def extractor(fast_retry: RetryPolicy) -> GoogleAdsExtractor:
    return GoogleAdsExtractor(ReplayGoogleAdsService(FIXTURES), fast_retry)


def table_counts(warehouse: DuckDBWarehouse) -> dict[str, int]:
    tables = ["account_daily", "campaign_daily", "ad_group_daily", "campaign_budget_snapshot"]
    return {
        table: warehouse.query(f"select count(*) from google_ads_raw.{table}")[0][0]
        for table in tables
    }


def test_full_run_loads_every_table(warehouse: DuckDBWarehouse, fast_retry: RetryPolicy) -> None:
    results = run(extractor(fast_retry), warehouse, ["123-456-7890"], SEPT_1, SEPT_14, SEPT_15)
    assert [(r.table, r.rows) for r in results] == [
        ("account_daily", 14),
        ("campaign_daily", 52),
        ("ad_group_daily", 66),
        ("campaign_budget_snapshot", 5),
    ]
    assert table_counts(warehouse) == {
        "account_daily": 14,
        "campaign_daily": 52,
        "ad_group_daily": 66,
        "campaign_budget_snapshot": 5,
    }


def test_rerunning_and_overlapping_runs_do_not_duplicate(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    run(extractor(fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
    before = table_counts(warehouse)
    total_before = warehouse.query("select sum(cost) from google_ads_raw.campaign_daily")

    run(extractor(fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
    run(extractor(fast_retry), warehouse, [CUSTOMER_ID], date(2026, 9, 8), SEPT_14, SEPT_15)

    assert table_counts(warehouse) == before
    assert warehouse.query("select sum(cost) from google_ads_raw.campaign_daily") == total_before


def test_default_window_ends_yesterday_in_the_account_time_zone(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    results = run(
        extractor(fast_retry),
        warehouse,
        [CUSTOMER_ID],
        lookback_days=5,
        now=lambda: LATE_EVENING_IN_NEW_YORK,
    )
    windows = {result.table: (result.start, result.end, result.rows) for result in results}
    # In UTC it is already the 15th, but the 14th has not ended in New York. Loading the
    # 14th now would store a partial day as if it were complete.
    assert windows["account_daily"] == (date(2026, 9, 9), date(2026, 9, 13), 5)
    assert windows["campaign_budget_snapshot"][:2] == (SEPT_14, SEPT_14)
    assert warehouse.query("select max(date) from google_ads_raw.account_daily") == [
        (date(2026, 9, 13),)
    ]


@pytest.mark.parametrize(
    ("time_zone", "now", "yesterday"),
    [
        # 06:59 UTC on the 15th is 23:59 on the 14th in Los Angeles: the 14th is not over.
        ("America/Los_Angeles", datetime(2026, 9, 15, 6, 59, tzinfo=timezone.utc), 13),
        ("America/Los_Angeles", datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc), 14),
        # 20:00 UTC on the 14th is 05:00 on the 15th in Tokyo: the 14th is complete.
        ("Asia/Tokyo", datetime(2026, 9, 14, 20, 0, tzinfo=timezone.utc), 14),
        ("UTC", datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc), 14),
    ],
)
def test_local_window_follows_the_account_calendar(
    time_zone: str, now: datetime, yesterday: int
) -> None:
    window = local_window(time_zone, now, lookback_days=30)
    assert window.end == date(2026, 9, yesterday)
    assert window.start == window.end - timedelta(days=29)
    assert window.snapshot_date == window.end + timedelta(days=1)


def test_explicit_dates_need_no_time_zone_request(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    service = CountingService()
    run(GoogleAdsExtractor(service, fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
    assert "account_time_zone" not in service.reports
    assert len(service.reports) == 4  # one GAQL request per report


def test_a_re_pull_overwrites_restated_days(
    tmp_path, warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    # Google restates recent days (late conversions, invalid-click credits). Build a copy
    # of the recordings in which one campaign costs 10.00 more on the 12th to the 14th.
    restated = tmp_path / "restated"
    shutil.copytree(FIXTURES, restated)
    days = {"2026-09-12", "2026-09-13", "2026-09-14"}
    for report in ("campaign_daily", "account_daily"):
        path = restated / f"{report}__{CUSTOMER_ID}.json"
        batches = json.loads(path.read_text())
        for row in (row for batch in batches for row in batch["results"]):
            campaign = row.get("campaign", {}).get("id")
            if row["segments"]["date"] in days and campaign in (None, "20111111113"):
                row["metrics"]["costMicros"] = str(int(row["metrics"]["costMicros"]) + 10_000_000)
        path.write_text(json.dumps(batches))

    run(extractor(fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
    counts_before = table_counts(warehouse)
    [(cost_before,)] = warehouse.query("select sum(cost) from google_ads_raw.campaign_daily")

    restated_extractor = GoogleAdsExtractor(ReplayGoogleAdsService(restated), fast_retry)
    run(restated_extractor, warehouse, [CUSTOMER_ID], date(2026, 9, 8), SEPT_14, SEPT_15)

    assert table_counts(warehouse) == counts_before
    [(cost_after,)] = warehouse.query("select sum(cost) from google_ads_raw.campaign_daily")
    assert cost_after - cost_before == Decimal("30")
    assert warehouse.query(
        """
        select cost from google_ads_raw.campaign_daily
        where campaign_id = 20111111113 and date = date '2026-09-14'
        """
    ) == [(Decimal("166.22"),)]


def test_account_spend_equals_campaign_spend_in_the_fixtures(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    run(extractor(fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
    mismatches = warehouse.query(
        """
        select a.date
        from google_ads_raw.account_daily a
        join (
            select date, sum(cost_micros) as micros
            from google_ads_raw.campaign_daily
            group by date
        ) c using (date)
        where a.cost_micros != c.micros
        """
    )
    assert mismatches == []


def test_one_failing_account_does_not_block_the_others(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    # There is no recording for the second account, so every request for it fails.
    with pytest.raises(PipelineError, match="9876543210: FileNotFoundError"):
        run(
            extractor(fast_retry),
            warehouse,
            [CUSTOMER_ID, "987-654-3210"],
            SEPT_1,
            SEPT_14,
            SEPT_15,
        )
    assert table_counts(warehouse)["campaign_daily"] == 52


def test_an_invalid_account_id_stops_the_run_before_loading(
    warehouse: DuckDBWarehouse, fast_retry: RetryPolicy
) -> None:
    with pytest.raises(ValueError, match="customer ID"):
        run(extractor(fast_retry), warehouse, [CUSTOMER_ID, "12-34"], SEPT_1, SEPT_14, SEPT_15)
    assert warehouse.query("select count(*) from information_schema.tables") == [(0,)]


def test_recording_then_replaying_round_trips(tmp_path, warehouse, fast_retry) -> None:
    recorder = RecordingGoogleAdsService(ReplayGoogleAdsService(FIXTURES), tmp_path / "recorded")
    run(
        GoogleAdsExtractor(recorder, fast_retry), warehouse, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15
    )
    original = table_counts(warehouse)

    with DuckDBWarehouse(tmp_path / "replayed.duckdb") as replayed:
        replay = GoogleAdsExtractor(ReplayGoogleAdsService(tmp_path / "recorded"), fast_retry)
        run(replay, replayed, [CUSTOMER_ID], SEPT_1, SEPT_14, SEPT_15)
        assert table_counts(replayed) == original


def test_cli_runs_offline_against_duckdb(tmp_path, capsys) -> None:
    exit_code = main(
        [
            "--customer-id", "123-456-7890",
            "--start", "2026-09-01",
            "--end", "2026-09-14",
            "--snapshot-date", "2026-09-15",
            "--warehouse", "duckdb",
            "--duckdb-path", str(tmp_path / "cli.duckdb"),
            "--replay", str(FIXTURES),
        ]
    )  # fmt: skip
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "campaign_daily" in output
    assert "52 rows" in output


def test_cli_defaults_to_yesterday_in_the_account_time_zone(tmp_path, capsys, monkeypatch) -> None:
    monkeypatch.setattr(pipeline, "utc_now", lambda: LATE_EVENING_IN_NEW_YORK)
    exit_code = main(
        [
            "--customer-id", "123-456-7890", "--lookback-days", "3",
            "--warehouse", "duckdb", "--duckdb-path", str(tmp_path / "cli.duckdb"),
            "--replay", str(FIXTURES),
        ]
    )  # fmt: skip
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "account_daily              2026-09-11 to 2026-09-13" in output
    assert "campaign_budget_snapshot   2026-09-14 to 2026-09-14" in output


def test_cli_exits_with_1_when_an_account_fails(tmp_path, caplog) -> None:
    exit_code = main(
        [
            "--customer-id", "123-456-7890", "--customer-id", "987-654-3210",
            "--start", "2026-09-01", "--end", "2026-09-14",
            "--warehouse", "duckdb", "--duckdb-path", str(tmp_path / "cli.duckdb"),
            "--replay", str(FIXTURES),
        ]
    )  # fmt: skip
    assert exit_code == 1
    assert "9876543210" in caplog.text


def test_cli_requires_a_bigquery_project(monkeypatch) -> None:
    monkeypatch.delenv("BQ_PROJECT", raising=False)
    with pytest.raises(SystemExit):
        main(["--customer-id", CUSTOMER_ID, "--warehouse", "bigquery", "--replay", str(FIXTURES)])


def test_fixtures_are_reproducible(tmp_path) -> None:
    for path in load_script("generate_fixtures").write_fixtures(tmp_path):
        assert path.read_text() == (FIXTURES / path.name).read_text(), path.name
