"""Build the dbt project on DuckDB, then break the data on purpose and check the tests notice.

A data test that has never been seen failing proves nothing, so each sabotage below
introduces one realistic defect and asserts that the matching test, and only a test
that should care, reports it.
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

from google_ads_pipeline.extract import GoogleAdsExtractor
from google_ads_pipeline.pipeline import run
from google_ads_pipeline.replay import ReplayGoogleAdsService
from google_ads_pipeline.warehouse import DuckDBWarehouse

from support import CUSTOMER_ID, FIXTURES, ROOT

pytestmark = pytest.mark.dbt

TRANSFORM = ROOT / "transform"
DBT = Path(sys.executable).with_name("dbt")


@pytest.fixture(scope="session")
def loaded_database(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("raw") / "google_ads.duckdb"
    with DuckDBWarehouse(path) as warehouse:
        run(
            GoogleAdsExtractor(ReplayGoogleAdsService(FIXTURES)),
            warehouse,
            [CUSTOMER_ID],
            date(2026, 9, 1),
            date(2026, 9, 14),
            snapshot_date=date(2026, 9, 15),
        )
    return path


@pytest.fixture
def database(loaded_database: Path, tmp_path: Path) -> Path:
    """A private copy of the loaded database that a test may damage."""
    return Path(shutil.copy(loaded_database, tmp_path / "google_ads.duckdb"))


def dbt(command: str, database: Path, *args: str) -> tuple[int, str, dict[str, str]]:
    """Run a dbt command; return exit code, output and the status of every node."""
    target = database.parent / "target"
    result = subprocess.run(
        [
            str(DBT), *command.split(),
            "--project-dir", str(TRANSFORM),
            "--profiles-dir", str(TRANSFORM),
            "--target-path", str(target),
            "--log-path", str(database.parent / "logs"),
            *args,
        ],
        env={**os.environ, "DUCKDB_PATH": str(database), "DBT_TARGET": "local"},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )  # fmt: skip
    results_file = "sources.json" if command == "source freshness" else "run_results.json"
    results = json.loads((target / results_file).read_text())["results"]
    statuses = {node_name(result["unique_id"]): result["status"] for result in results}
    return result.returncode, result.stdout, statuses


def node_name(unique_id: str) -> str:
    """test.project.name.hash -> name; source.project.source.table -> table."""
    kind, _, *parts = unique_id.split(".")
    if kind == "source":
        return parts[-1]
    if kind == "unit_test":
        return parts[1]
    return parts[0]


def failed(statuses: dict[str, str]) -> set[str]:
    return {node for node, status in statuses.items() if status in {"fail", "error"}}


def sabotage(database: Path, sql: str) -> None:
    with duckdb.connect(str(database)) as con:
        con.execute(sql)


def test_dbt_build_passes_on_the_recorded_data(database: Path) -> None:
    code, output, statuses = dbt("build", database)
    assert code == 0, output
    assert failed(statuses) == set()
    assert "ERROR=0" in output and "WARN=0" in output


def test_sources_are_fresh_after_a_load(database: Path) -> None:
    code, output, statuses = dbt("source freshness", database)
    assert code == 0, output
    assert set(statuses.values()) == {"pass"}


def test_marts_hold_the_expected_numbers(database: Path) -> None:
    code, output, _ = dbt("build", database)
    assert code == 0, output
    with duckdb.connect(str(database), read_only=True) as con:
        campaign_cost, account_cost = con.execute(
            """
            select
                (select sum(cost) from main_marts.fct_campaign_performance_daily),
                (select sum(cost) from google_ads_raw.account_daily)
            """
        ).fetchone()
        pacing = dict(
            con.execute(
                """
                select budget_name, pacing_status
                from main_marts.fct_budget_pacing_daily
                where date_day = date '2026-09-14'
                """
            ).fetchall()
        )
    assert campaign_cost == account_cost
    assert pacing == {
        "Brand search (shared)": "UNDER",
        "Generic tents": "ON_TRACK",
        "PMax all products": "OVER",
        "Display remarketing": "UNDER",
    }


@pytest.mark.parametrize(
    ("defect", "expected_failure"),
    [
        pytest.param(
            # The classic bug: filtering campaign.status != 'REMOVED' in the extractor.
            "delete from google_ads_raw.campaign_daily where campaign_status = 'REMOVED'",
            "assert_campaign_spend_reconciles_with_account",
            id="removed-campaigns-dropped",
        ),
        pytest.param(
            # 0.6% more campaign spend than account spend on one day: over the 0.5% limit.
            """
            update google_ads_raw.campaign_daily
            set cost = cost + (select cost * 0.006 from google_ads_raw.account_daily
                               where date = date '2026-09-05')
            where date = date '2026-09-05' and campaign_id = 20111111113
            """,
            "assert_campaign_spend_reconciles_with_account",
            id="spend-drift-over-tolerance",
        ),
        pytest.param(
            """
            insert into google_ads_raw.campaign_daily
            select * from google_ads_raw.campaign_daily limit 1
            """,
            "unique_combination_stg_google_ads__campaign_daily_customer_id__campaign_id__date_day",
            id="duplicate-row",
        ),
        pytest.param(
            """
            update google_ads_raw.campaign_daily
            set campaign_status = 'ARCHIVED' where campaign_id = 20111111115
            """,
            "accepted_values_stg_google_ads__campaign_daily_campaign_status__ENABLED__PAUSED__REMOVED",
            id="unknown-status",
        ),
        pytest.param(
            "delete from google_ads_raw.campaign_budget_snapshot where campaign_id = 20111111112",
            "assert_budget_pacing_covers_all_campaign_spend",
            id="campaign-missing-from-budget-snapshot",
        ),
    ],
)
def test_data_tests_catch_the_defect(database: Path, defect: str, expected_failure: str) -> None:
    sabotage(database, defect)
    code, output, statuses = dbt("build", database)
    assert code != 0
    assert failed(statuses) == {expected_failure}, output


def test_spend_drift_within_tolerance_passes(database: Path) -> None:
    sabotage(
        database,
        """
        update google_ads_raw.campaign_daily
        set cost = cost + (select cost * 0.004 from google_ads_raw.account_daily
                           where date = date '2026-09-05')
        where date = date '2026-09-05' and campaign_id = 20111111113
        """,
    )
    code, output, statuses = dbt("build", database)
    assert code == 0, output
    assert statuses["assert_campaign_spend_reconciles_with_account"] == "pass"


def test_a_new_channel_type_warns_without_failing_the_build(database: Path) -> None:
    # Google adds channel types over time; that deserves a look, not a broken pipeline.
    sabotage(
        database,
        """
        update google_ads_raw.campaign_daily
        set advertising_channel_type = 'NEW_CHANNEL_TYPE' where campaign_id = 20111111114
        """,
    )
    code, output, statuses = dbt("build", database)
    assert code == 0, output
    warned = [node for node, status in statuses.items() if status == "warn"]
    assert len(warned) == 1
    assert warned[0].startswith("accepted_values_stg_google_ads__campaign_daily_advertising")


def test_stale_data_fails_source_freshness(database: Path) -> None:
    sabotage(
        database, "update google_ads_raw.campaign_daily set _loaded_at = now() - interval 3 day"
    )
    code, _, statuses = dbt("source freshness", database)
    assert code != 0
    assert statuses["campaign_daily"] == "error"
    assert statuses["account_daily"] == "pass"
