"""Offline stand-ins for GoogleAdsService: record live responses, or replay recorded ones.

Recordings are the SearchGoogleAdsStreamResponse batches of one report for one customer,
stored in the API's own JSON form (int64 as strings, enums as names). Parsing them back
through the v25 message classes means a fixture that drifts from the API schema fails
loudly instead of passing silently.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

from google.ads.googleads.v25.services.types.google_ads_service import (
    SearchGoogleAdsStreamResponse,
)

from google_ads_pipeline.extract import SearchStreamService
from google_ads_pipeline.reports import date_range_of, report_for_query


def recording_path(directory: Path, report_name: str, customer_id: str) -> Path:
    return directory / f"{report_name}__{customer_id}.json"


def load_batches(path: Path) -> list[SearchGoogleAdsStreamResponse]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [SearchGoogleAdsStreamResponse.from_json(json.dumps(batch)) for batch in payload]


def dump_batches(batches: Iterable[SearchGoogleAdsStreamResponse], path: Path) -> None:
    payload = [
        json.loads(
            SearchGoogleAdsStreamResponse.to_json(
                batch, use_integers_for_enums=False, always_print_fields_with_no_presence=False
            )
        )
        for batch in batches
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")


class ReplayGoogleAdsService:
    """Serves recorded SearchStream responses instead of calling the API.

    Rows are filtered to the date range in the query, so replaying any sub-range of a
    recording behaves like asking the API for that range.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    def search_stream(self, *, customer_id: str, query: str) -> Iterator[Any]:
        report = report_for_query(query)
        path = recording_path(self._dir, report.name, customer_id)
        if not path.exists():
            raise FileNotFoundError(
                f"No recording for {report.name}, customer {customer_id}: {path}"
            )
        date_range = date_range_of(query)
        for batch in load_batches(path):
            rows = [row for row in batch.results if _in_range(row.segments.date, date_range)]
            yield SearchGoogleAdsStreamResponse(
                results=rows, field_mask=batch.field_mask, request_id=batch.request_id
            )


def _in_range(day: str, date_range: tuple[date, date] | None) -> bool:
    if date_range is None:
        return True
    start, end = date_range
    return start.isoformat() <= day <= end.isoformat()


class RecordingGoogleAdsService:
    """Wraps the real GoogleAdsService and saves each report's responses for replay.

    Recordings contain real account data: anonymise them before committing.
    """

    def __init__(self, service: SearchStreamService, directory: str | Path) -> None:
        self._service = service
        self._dir = Path(directory)

    def search_stream(self, *, customer_id: str, query: str) -> list[Any]:
        report = report_for_query(query)
        batches = list(self._service.search_stream(customer_id=customer_id, query=query))
        dump_batches(batches, recording_path(self._dir, report.name, customer_id))
        return batches
