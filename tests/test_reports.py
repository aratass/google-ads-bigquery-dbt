from datetime import date
from decimal import Decimal

import pytest
from google.ads.googleads.v25.services.types.google_ads_service import GoogleAdsRow

from google_ads_pipeline.replay import load_batches, recording_path
from google_ads_pipeline.reports import (
    ACCOUNT_TIME_ZONE_REPORT,
    ALL_REPORTS,
    CAMPAIGN_BUDGET_REPORT,
    CAMPAIGN_DAILY_REPORT,
    KNOWN_REPORTS,
    date_range_of,
    micros_to_units,
    normalize_customer_id,
    report_for_query,
)

from support import CUSTOMER_ID, FIXTURES


@pytest.mark.parametrize(
    ("micros", "expected"),
    [
        (0, Decimal("0")),
        (1, Decimal("0.000001")),
        (10_000, Decimal("0.01")),
        (1_500_000, Decimal("1.5")),
        (1_234_567, Decimal("1.234567")),
        (37_070_000, Decimal("37.07")),
        (9_223_372_036_854_775_807, Decimal("9223372036854.775807")),  # int64 max
    ],
)
def test_micros_convert_exactly(micros: int, expected: Decimal) -> None:
    assert micros_to_units(micros) == expected


def test_micros_conversion_has_no_float_drift() -> None:
    # 0.1 + 0.2 != 0.3 in floating point; in micros it must hold exactly.
    assert micros_to_units(100_000) + micros_to_units(200_000) == Decimal("0.3")
    daily = [1_110_000, 2_220_000, 3_330_000] * 1000
    assert sum(map(micros_to_units, daily)) == micros_to_units(sum(daily))


@pytest.mark.parametrize("raw", ["123-456-7890", "1234567890", 1234567890, " 123-456-7890 "])
def test_customer_id_is_normalised(raw: str | int) -> None:
    assert normalize_customer_id(raw) == "1234567890"


@pytest.mark.parametrize("raw", ["123-456-789", "12345678901", "abc-def-ghij", ""])
def test_invalid_customer_id_is_rejected(raw: str) -> None:
    with pytest.raises(ValueError, match="customer ID"):
        normalize_customer_id(raw)


def test_daily_query_filters_on_the_requested_range() -> None:
    query = CAMPAIGN_DAILY_REPORT.query(date(2026, 9, 1), date(2026, 9, 14))
    assert query.startswith("SELECT\n  segments.date,")
    assert "FROM campaign\n" in query
    assert query.endswith("WHERE segments.date BETWEEN '2026-09-01' AND '2026-09-14'")
    assert date_range_of(query) == (date(2026, 9, 1), date(2026, 9, 14))


def test_campaign_query_keeps_removed_campaigns() -> None:
    # Filtering on status would drop spend that still counts in the account totals.
    assert (
        "campaign.status"
        not in CAMPAIGN_DAILY_REPORT.query(date(2026, 9, 1), date(2026, 9, 1)).split("WHERE")[1]
    )


def test_budget_snapshot_query_has_no_date_filter() -> None:
    query = CAMPAIGN_BUDGET_REPORT.query()
    assert "WHERE" not in query
    assert date_range_of(query) is None


def test_daily_query_requires_a_valid_range() -> None:
    with pytest.raises(ValueError, match="needs a start and an end"):
        CAMPAIGN_DAILY_REPORT.query()
    with pytest.raises(ValueError, match="after end date"):
        CAMPAIGN_DAILY_REPORT.query(date(2026, 9, 2), date(2026, 9, 1))


def _field_exists(path: str) -> bool:
    descriptor = GoogleAdsRow.pb().DESCRIPTOR
    parts = path.split(".")
    for index, part in enumerate(parts):
        # proto-plus renames fields that clash with Python names: ad_group.type -> type_
        field = descriptor.fields_by_name.get(part) or descriptor.fields_by_name.get(part + "_")
        if field is None:
            return False
        descriptor = field.message_type
        if descriptor is None and index < len(parts) - 1:
            return False
    return True


@pytest.mark.parametrize("report", KNOWN_REPORTS, ids=lambda report: report.name)
def test_every_selected_field_exists_in_api_v25(report) -> None:
    missing = [field for field in report.fields if not _field_exists(field)]
    assert missing == []


@pytest.mark.parametrize("report", KNOWN_REPORTS, ids=lambda report: report.name)
def test_query_is_recognised_as_its_report(report) -> None:
    query = (
        report.query(date(2026, 9, 1), date(2026, 9, 2))
        if report.segmented_by_date
        else report.query()
    )
    assert report_for_query(query) is report


def test_time_zone_query_reads_one_account_row() -> None:
    query = ACCOUNT_TIME_ZONE_REPORT.query()
    assert query == "SELECT\n  customer.id,\n  customer.time_zone\nFROM customer"
    [batch] = load_batches(recording_path(FIXTURES, ACCOUNT_TIME_ZONE_REPORT.name, CUSTOMER_ID))
    [row] = batch.results
    assert ACCOUNT_TIME_ZONE_REPORT.to_row(row) == {
        "customer_id": 1234567890,
        "time_zone": "America/New_York",
    }


def test_unknown_query_is_rejected() -> None:
    with pytest.raises(LookupError):
        report_for_query("SELECT campaign.id FROM campaign")


@pytest.mark.parametrize("report", ALL_REPORTS, ids=lambda report: report.name)
def test_mapped_rows_match_the_table_schema(report) -> None:
    batches = load_batches(recording_path(FIXTURES, report.name, CUSTOMER_ID))
    row = report.to_row(batches[0].results[0])
    expected = {column.name for column in report.table.data_columns}
    if report is CAMPAIGN_BUDGET_REPORT:
        expected.discard("snapshot_date")  # added by the pipeline, not the API
    assert set(row) == expected
