"""Run GAQL reports through GoogleAdsService.SearchStream."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from typing import Any, Protocol

import grpc
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import exceptions as core_exceptions

from google_ads_pipeline.backoff import RetryPolicy
from google_ads_pipeline.reports import Report, Row, normalize_customer_id

log = logging.getLogger(__name__)

# gRPC status codes the Google Ads API documentation lists as retryable.
TRANSIENT_STATUS_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)

# Google Ads error codes, as (error_code oneof field, enum name), that mean "try again later".
TRANSIENT_ERROR_CODES = frozenset(
    {
        ("internal_error", "INTERNAL_ERROR"),
        ("internal_error", "TRANSIENT_ERROR"),
        ("internal_error", "DEADLINE_EXCEEDED"),
        ("quota_error", "RESOURCE_TEMPORARILY_EXHAUSTED"),
    }
)

_TRANSIENT_CORE_ERRORS = (
    core_exceptions.ServiceUnavailable,
    core_exceptions.DeadlineExceeded,
    core_exceptions.InternalServerError,
    core_exceptions.Unknown,
    core_exceptions.Aborted,
    core_exceptions.TooManyRequests,
)


def _status_code(error: Any) -> grpc.StatusCode | None:
    code = getattr(error, "code", None)
    return code() if callable(code) else None


def error_codes(failure: Any) -> set[tuple[str, str]]:
    """Return the (category, name) pairs of every error in a GoogleAdsFailure."""
    codes = set()
    for error in failure.errors:
        code = getattr(error.error_code, "_pb", error.error_code)  # unwrap proto-plus
        kind = code.WhichOneof("error_code")
        if kind is not None:
            enum = code.DESCRIPTOR.fields_by_name[kind].enum_type
            codes.add((kind, enum.values_by_number[getattr(code, kind)].name))
    return codes


def is_transient(exc: BaseException) -> bool:
    """True when a failed request is worth retrying.

    The client library raises GoogleAdsException when the API returns a GoogleAdsFailure,
    and passes INTERNAL, RESOURCE_EXHAUSTED and transport errors through as grpc.RpcError.
    """
    if isinstance(exc, GoogleAdsException):
        if error_codes(exc.failure) & TRANSIENT_ERROR_CODES:
            return True
        return _status_code(exc.error) in TRANSIENT_STATUS_CODES
    if isinstance(exc, core_exceptions.GoogleAPICallError):
        return isinstance(exc, _TRANSIENT_CORE_ERRORS)
    if isinstance(exc, grpc.RpcError):
        return _status_code(exc) in TRANSIENT_STATUS_CODES
    return False


class SearchStreamService(Protocol):
    def search_stream(self, *, customer_id: str, query: str) -> Iterable[Any]: ...


class GoogleAdsExtractor:
    """Runs GAQL reports and returns plain Python rows ready for loading."""

    def __init__(self, service: SearchStreamService, retry: RetryPolicy | None = None) -> None:
        self._service = service
        self._retry = retry or RetryPolicy()

    def fetch(
        self,
        report: Report,
        customer_id: str | int,
        start: date | None = None,
        end: date | None = None,
    ) -> list[Row]:
        customer_id = normalize_customer_id(customer_id)
        query = report.query(start, end)
        rows = self._retry.call(
            lambda: self._stream(report, customer_id, query),
            is_transient,
            description=f"{report.name} for customer {customer_id}",
        )
        log.info("Fetched %d %s rows for customer %s", len(rows), report.name, customer_id)
        return rows

    def _stream(self, report: Report, customer_id: str, query: str) -> list[Row]:
        # The stream is consumed inside the retry, so a stream that breaks halfway is
        # restarted from the beginning instead of being loaded partially.
        stream = self._service.search_stream(customer_id=customer_id, query=query)
        return [report.to_row(row) for batch in stream for row in batch.results]
