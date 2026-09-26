from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import sqlglot
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from google_ads_pipeline.schemas import ACCOUNT_DAILY, ALL_TABLES, CAMPAIGN_BUDGET_SNAPSHOT
from google_ads_pipeline.warehouse import BigQueryWarehouse, DuckDBWarehouse

SEPT_1, SEPT_2, SEPT_3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)


def account_row(day: date, cost: str, customer_id: int = 1234567890) -> dict:
    return {
        "date": day,
        "customer_id": customer_id,
        "account_name": "Demo",
        "currency_code": "USD",
        "time_zone": "America/New_York",
        "impressions": 100,
        "clicks": 10,
        "cost_micros": int(Decimal(cost) * 1_000_000),
        "cost": Decimal(cost),
        "conversions": 1.5,
        "conversions_value": 99.0,
    }


# DuckDB: the same load contract, executed for real.


@pytest.fixture
def duckdb_warehouse(tmp_path):
    with DuckDBWarehouse(tmp_path / "test.duckdb") as warehouse:
        warehouse.ensure_tables(ALL_TABLES)
        yield warehouse


def _costs(warehouse: DuckDBWarehouse) -> list[tuple]:
    return warehouse.query(
        "select customer_id, date, cost from google_ads_raw.account_daily order by 1, 2"
    )


def test_reloading_a_range_replaces_rows(duckdb_warehouse: DuckDBWarehouse) -> None:
    rows = [account_row(SEPT_1, "10.00"), account_row(SEPT_2, "20.00")]
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY, rows, customer_id=1234567890, start=SEPT_1, end=SEPT_2
    )
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY, rows, customer_id=1234567890, start=SEPT_1, end=SEPT_2
    )
    assert len(_costs(duckdb_warehouse)) == 2

    # Late conversions/cost adjustments: the second load wins, no duplicates.
    updated = [account_row(SEPT_1, "10.50"), account_row(SEPT_2, "20.25")]
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY, updated, customer_id=1234567890, start=SEPT_1, end=SEPT_2
    )
    assert [row[2] for row in _costs(duckdb_warehouse)] == [Decimal("10.50"), Decimal("20.25")]


def test_rows_that_vanished_from_the_source_are_removed(duckdb_warehouse: DuckDBWarehouse) -> None:
    rows = [account_row(SEPT_1, "10.00"), account_row(SEPT_2, "20.00")]
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY, rows, customer_id=1234567890, start=SEPT_1, end=SEPT_2
    )
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY, [], customer_id=1234567890, start=SEPT_2, end=SEPT_2
    )
    assert [row[1] for row in _costs(duckdb_warehouse)] == [SEPT_1]


def test_other_days_and_other_accounts_are_untouched(duckdb_warehouse: DuckDBWarehouse) -> None:
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_1, "1.00"), account_row(SEPT_3, "3.00")],
        customer_id=1234567890,
        start=SEPT_1,
        end=SEPT_3,
    )
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_2, "5.00", customer_id=9876543210)],
        customer_id=9876543210,
        start=SEPT_1,
        end=SEPT_3,
    )
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_3, "3.30")],
        customer_id=1234567890,
        start=SEPT_3,
        end=SEPT_3,
    )
    assert _costs(duckdb_warehouse) == [
        (1234567890, SEPT_1, Decimal("1.00")),
        (1234567890, SEPT_3, Decimal("3.30")),
        (9876543210, SEPT_2, Decimal("5.00")),
    ]


def test_a_failed_load_keeps_the_previous_data(duckdb_warehouse: DuckDBWarehouse) -> None:
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_1, "10.00")],
        customer_id=1234567890,
        start=SEPT_1,
        end=SEPT_1,
    )
    broken = account_row(SEPT_1, "11.00")
    broken["currency_code"] = None  # NOT NULL column
    with pytest.raises(Exception, match="NOT NULL"):
        duckdb_warehouse.replace_range(
            ACCOUNT_DAILY, [broken], customer_id=1234567890, start=SEPT_1, end=SEPT_1
        )
    assert _costs(duckdb_warehouse) == [(1234567890, SEPT_1, Decimal("10.00"))]


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (account_row(date(2026, 9, 9), "1.00"), "outside the range"),
        (account_row(SEPT_1, "1.00", customer_id=1111111111), "outside the range"),
        ({**account_row(SEPT_1, "1.00"), "extra": 1}, "do not match the schema"),
    ],
)
def test_rows_outside_the_replaced_range_are_refused(
    duckdb_warehouse: DuckDBWarehouse, row: dict, message: str
) -> None:
    # Such rows would escape the next re-run's delete and end up duplicated.
    with pytest.raises(ValueError, match=message):
        duckdb_warehouse.replace_range(
            ACCOUNT_DAILY, [row], customer_id=1234567890, start=SEPT_1, end=SEPT_2
        )


def test_loaded_at_is_stamped_by_the_loader(duckdb_warehouse: DuckDBWarehouse) -> None:
    duckdb_warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_1, "1.00")],
        customer_id=1234567890,
        start=SEPT_1,
        end=SEPT_1,
    )
    [(age_seconds,)] = duckdb_warehouse.query(
        "select epoch(now() - _loaded_at) from google_ads_raw.account_daily"
    )
    assert 0 <= age_seconds < 60


# BigQuery: no network in tests, so assert on the API calls and the SQL we send.


@pytest.fixture
def bq_client() -> MagicMock:
    client = MagicMock(spec=bigquery.Client)
    client.project = "demo-project"
    return client


def test_tables_are_created_partitioned_and_clustered(bq_client: MagicMock) -> None:
    bq_client.get_dataset.side_effect = NotFound("no dataset yet")
    BigQueryWarehouse(bq_client, "google_ads_raw", "EU").ensure_tables(ALL_TABLES)

    dataset = bq_client.create_dataset.call_args.args[0]
    assert dataset.dataset_id == "google_ads_raw"
    assert dataset.location == "EU"
    tables = {call.args[0].table_id: call.args[0] for call in bq_client.create_table.call_args_list}
    assert set(tables) == {table.name for table in ALL_TABLES}
    campaign_daily = tables["campaign_daily"]
    assert campaign_daily.time_partitioning.field == "date"
    assert campaign_daily.clustering_fields == ["customer_id", "campaign_id"]
    assert tables["campaign_budget_snapshot"].time_partitioning.field == "snapshot_date"
    cost = next(field for field in campaign_daily.schema if field.name == "cost")
    assert (cost.field_type, cost.mode) == ("NUMERIC", "REQUIRED")
    assert all(call.kwargs == {"exists_ok": True} for call in bq_client.create_table.call_args_list)


def test_an_existing_dataset_is_not_recreated(bq_client: MagicMock) -> None:
    BigQueryWarehouse(bq_client, "google_ads_raw").ensure_tables(ALL_TABLES)
    bq_client.get_dataset.assert_called_once_with("demo-project.google_ads_raw")
    bq_client.create_dataset.assert_not_called()


def test_load_stages_rows_then_swaps_the_range_in_one_transaction(bq_client: MagicMock) -> None:
    warehouse = BigQueryWarehouse(bq_client, "google_ads_raw")
    warehouse.replace_range(
        ACCOUNT_DAILY,
        [account_row(SEPT_1, "37.07"), account_row(SEPT_2, "1.234567")],
        customer_id=1234567890,
        start=SEPT_1,
        end=SEPT_2,
    )

    staging_table = bq_client.create_table.call_args.args[0]
    staging_id = f"{staging_table.project}.{staging_table.dataset_id}.{staging_table.table_id}"
    assert staging_table.table_id.startswith("_staging_account_daily_")
    assert staging_table.expires is not None
    assert "_loaded_at" not in [field.name for field in staging_table.schema]

    rows_json, destination = bq_client.load_table_from_json.call_args.args
    assert destination == staging_id
    assert rows_json[0]["cost"] == "37.07"  # NUMERIC sent as a string: no float rounding
    assert rows_json[1]["cost"] == "1.234567"
    assert rows_json[0]["date"] == "2026-09-01"

    sql = bq_client.query.call_args.args[0]
    statements = [type(statement).__name__ for statement in sqlglot.parse(sql, read="bigquery")]
    assert statements == ["Transaction", "Delete", "Insert", "Commit"]
    assert "DELETE FROM `demo-project.google_ads_raw.account_daily`" in sql
    assert "WHERE customer_id = @customer_id\n  AND date BETWEEN @start_date AND @end_date" in sql
    assert f"CURRENT_TIMESTAMP() FROM `{staging_id}`" in sql
    params = {
        p.name: p.value for p in bq_client.query.call_args.kwargs["job_config"].query_parameters
    }
    assert params == {"customer_id": 1234567890, "start_date": SEPT_1, "end_date": SEPT_2}

    bq_client.delete_table.assert_called_once_with(staging_id, not_found_ok=True)


def test_staging_table_is_dropped_even_when_the_swap_fails(bq_client: MagicMock) -> None:
    bq_client.query.side_effect = RuntimeError("transaction aborted")
    with pytest.raises(RuntimeError):
        BigQueryWarehouse(bq_client, "google_ads_raw").replace_range(
            ACCOUNT_DAILY,
            [account_row(SEPT_1, "1.00")],
            customer_id=1234567890,
            start=SEPT_1,
            end=SEPT_1,
        )
    bq_client.delete_table.assert_called_once()


def test_an_empty_range_is_cleared_without_a_load_job(bq_client: MagicMock) -> None:
    BigQueryWarehouse(bq_client, "google_ads_raw").replace_range(
        CAMPAIGN_BUDGET_SNAPSHOT, [], customer_id=1234567890, start=SEPT_1, end=SEPT_1
    )
    bq_client.load_table_from_json.assert_not_called()
    sql = bq_client.query.call_args.args[0]
    assert [type(s).__name__ for s in sqlglot.parse(sql, read="bigquery")] == ["Delete"]
    assert "snapshot_date BETWEEN @start_date AND @end_date" in sql
