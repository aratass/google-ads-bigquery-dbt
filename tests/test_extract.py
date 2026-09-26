from datetime import date
from decimal import Decimal

import grpc
import pytest
from google.ads.googleads.v25.errors.types.authorization_error import AuthorizationErrorEnum
from google.ads.googleads.v25.errors.types.internal_error import InternalErrorEnum
from google.ads.googleads.v25.errors.types.query_error import QueryErrorEnum
from google.ads.googleads.v25.errors.types.quota_error import QuotaErrorEnum
from google.api_core import exceptions as core_exceptions

from google_ads_pipeline.backoff import RetryPolicy
from google_ads_pipeline.extract import GoogleAdsExtractor, is_transient
from google_ads_pipeline.replay import ReplayGoogleAdsService
from google_ads_pipeline.reports import (
    ACCOUNT_DAILY_REPORT,
    CAMPAIGN_BUDGET_REPORT,
    CAMPAIGN_DAILY_REPORT,
)

from support import CUSTOMER_ID, FIXTURES, FakeRpcError, ads_exception

SEPT_1, SEPT_14 = date(2026, 9, 1), date(2026, 9, 14)


class ScriptedService:
    """Fails the first calls as scripted, then serves the recorded fixtures."""

    def __init__(self, *failures: Exception, mid_stream: bool = False) -> None:
        self.calls = 0
        self._failures = list(failures)
        self._mid_stream = mid_stream
        self._replay = ReplayGoogleAdsService(FIXTURES)

    def search_stream(self, *, customer_id: str, query: str):
        self.calls += 1
        stream = self._replay.search_stream(customer_id=customer_id, query=query)
        if not self._failures:
            return stream
        error = self._failures.pop(0)
        if self._mid_stream:
            return self._break_after_first_batch(stream, error)
        raise error

    @staticmethod
    def _break_after_first_batch(stream, error):
        yield next(stream)
        raise error


def test_rows_are_mapped_from_the_recorded_stream(fast_retry: RetryPolicy) -> None:
    extractor = GoogleAdsExtractor(ScriptedService(), fast_retry)
    rows = extractor.fetch(CAMPAIGN_DAILY_REPORT, "123-456-7890", SEPT_1, SEPT_14)

    assert len(rows) == 52  # several streamed batches, all consumed
    first = rows[0]
    assert first["date"] == SEPT_1
    assert first["customer_id"] == 1234567890
    assert first["campaign_name"] == "Brand | Search"
    assert first["campaign_status"] == "ENABLED"
    assert first["advertising_channel_type"] == "SEARCH"
    assert first["cost_micros"] == 37_070_000
    assert first["cost"] == Decimal("37.07")
    assert isinstance(first["conversions"], float)
    assert {row["campaign_status"] for row in rows} == {"ENABLED", "PAUSED", "REMOVED"}


def test_a_sub_range_returns_only_those_days(fast_retry: RetryPolicy) -> None:
    extractor = GoogleAdsExtractor(ScriptedService(), fast_retry)
    rows = extractor.fetch(ACCOUNT_DAILY_REPORT, CUSTOMER_ID, date(2026, 9, 10), SEPT_14)
    assert [row["date"].day for row in rows] == [10, 11, 12, 13, 14]


def test_budget_rows_carry_amounts_in_currency_units(fast_retry: RetryPolicy) -> None:
    rows = GoogleAdsExtractor(ScriptedService(), fast_retry).fetch(
        CAMPAIGN_BUDGET_REPORT, CUSTOMER_ID
    )
    by_campaign = {row["campaign_id"]: row for row in rows}
    assert by_campaign[20111111113]["budget_amount"] == Decimal("150")
    assert by_campaign[20111111113]["budget_period"] == "DAILY"
    shared = [row for row in rows if row["budget_is_shared"]]
    assert {row["budget_id"] for row in shared} == {30001}
    assert len(shared) == 2


def test_transient_errors_are_retried_with_backoff(
    fast_retry: RetryPolicy, sleeps: list[float]
) -> None:
    service = ScriptedService(
        FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        ads_exception(
            grpc.StatusCode.INTERNAL, internal_error=InternalErrorEnum.InternalError.TRANSIENT_ERROR
        ),
    )
    rows = GoogleAdsExtractor(service, fast_retry).fetch(
        ACCOUNT_DAILY_REPORT, CUSTOMER_ID, SEPT_1, SEPT_14
    )
    assert len(rows) == 14
    assert service.calls == 3
    assert sleeps == [5.0, 10.0]


def test_a_stream_that_breaks_halfway_is_restarted_not_duplicated(fast_retry: RetryPolicy) -> None:
    service = ScriptedService(FakeRpcError(grpc.StatusCode.UNAVAILABLE), mid_stream=True)
    rows = GoogleAdsExtractor(service, fast_retry).fetch(
        CAMPAIGN_DAILY_REPORT, CUSTOMER_ID, SEPT_1, SEPT_14
    )
    assert service.calls == 2
    assert len(rows) == 52
    assert len({(row["campaign_id"], row["date"]) for row in rows}) == 52


def test_permanent_errors_fail_fast(fast_retry: RetryPolicy, sleeps: list[float]) -> None:
    # v25 returns this when a Cloud project with Test access calls a production account.
    error = ads_exception(
        grpc.StatusCode.PERMISSION_DENIED,
        authorization_error=AuthorizationErrorEnum.AuthorizationError.CLOUD_PROJECT_NOT_APPROVED_FOR_PRODUCTION,
    )
    service = ScriptedService(error)
    with pytest.raises(type(error)):
        GoogleAdsExtractor(service, fast_retry).fetch(
            ACCOUNT_DAILY_REPORT, CUSTOMER_ID, SEPT_1, SEPT_14
        )
    assert service.calls == 1
    assert sleeps == []


def test_retries_stop_after_max_attempts(fast_retry: RetryPolicy, sleeps: list[float]) -> None:
    service = ScriptedService(*[FakeRpcError(grpc.StatusCode.UNAVAILABLE)] * 5)
    with pytest.raises(FakeRpcError):
        GoogleAdsExtractor(service, fast_retry).fetch(
            ACCOUNT_DAILY_REPORT, CUSTOMER_ID, SEPT_1, SEPT_14
        )
    assert service.calls == 5
    assert sleeps == [5.0, 10.0, 20.0, 40.0]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(FakeRpcError(grpc.StatusCode.UNAVAILABLE), True, id="unavailable"),
        pytest.param(FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED), True, id="deadline"),
        pytest.param(FakeRpcError(grpc.StatusCode.INTERNAL), True, id="internal"),
        pytest.param(FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED), True, id="rate-limited"),
        pytest.param(FakeRpcError(grpc.StatusCode.UNAUTHENTICATED), False, id="bad-credentials"),
        pytest.param(FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT), False, id="bad-request"),
        pytest.param(
            ads_exception(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                quota_error=QuotaErrorEnum.QuotaError.RESOURCE_TEMPORARILY_EXHAUSTED,
            ),
            True,
            id="quota-temporarily-exhausted",
        ),
        pytest.param(
            ads_exception(
                grpc.StatusCode.INVALID_ARGUMENT,
                query_error=QueryErrorEnum.QueryError.UNRECOGNIZED_FIELD,
            ),
            False,
            id="gaql-error",
        ),
        pytest.param(core_exceptions.ServiceUnavailable("down"), True, id="core-unavailable"),
        pytest.param(core_exceptions.PermissionDenied("no"), False, id="core-permission-denied"),
        pytest.param(ValueError("bug"), False, id="programming-error"),
    ],
)
def test_transient_error_classification(error: Exception, expected: bool) -> None:
    assert is_transient(error) is expected
