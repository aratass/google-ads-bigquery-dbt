"""Command-line entry point: `gads-pipeline`."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import date

from google_ads_pipeline import API_VERSION
from google_ads_pipeline.extract import GoogleAdsExtractor
from google_ads_pipeline.pipeline import DEFAULT_LOOKBACK_DAYS, PipelineError, run
from google_ads_pipeline.replay import RecordingGoogleAdsService, ReplayGoogleAdsService
from google_ads_pipeline.warehouse import BigQueryWarehouse, DuckDBWarehouse, Warehouse

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gads-pipeline",
        description=(
            "Load Google Ads account, campaign and ad group performance into BigQuery "
            "(or a local DuckDB file). Google Ads credentials are read from GOOGLE_ADS_* "
            "environment variables or the YAML file in GOOGLE_ADS_CONFIGURATION_FILE_PATH."
        ),
    )
    parser.add_argument(
        "--customer-id",
        dest="customer_ids",
        action="append",
        help="Account to load, e.g. 123-456-7890. Repeat for several accounts. "
        "Default: comma-separated GADS_CUSTOMER_IDS.",
    )
    parser.add_argument("--start", type=date.fromisoformat, help="First day to load (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        help="Last day to load (default: yesterday in each account's time zone, the zone "
        "Google Ads reports dates in).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Without --start, reload this many days up to --end (default 30, Google's "
        "default conversion window), which refreshes late conversions and cost corrections. "
        "Accounts with 60- or 90-day conversion windows need 60 or 90.",
    )
    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        help="Date to file the budget snapshot under (default: today in each account's time zone).",
    )
    parser.add_argument(
        "--warehouse",
        choices=("bigquery", "duckdb"),
        default=os.environ.get("WAREHOUSE", "bigquery"),
    )
    parser.add_argument("--bq-project", default=os.environ.get("BQ_PROJECT"))
    parser.add_argument("--bq-dataset", default=os.environ.get("BQ_RAW_DATASET", "google_ads_raw"))
    parser.add_argument("--bq-location", default=os.environ.get("BQ_LOCATION"))
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "local.duckdb"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--replay", metavar="DIR", help="Serve recorded API responses from DIR (no API calls)."
    )
    source.add_argument(
        "--record", metavar="DIR", help="Call the API and save every response to DIR for replay."
    )
    return parser


def _extractor(args: argparse.Namespace) -> GoogleAdsExtractor:
    if args.replay:
        return GoogleAdsExtractor(ReplayGoogleAdsService(args.replay))
    from google.ads.googleads.client import GoogleAdsClient

    client = GoogleAdsClient.load_from_env(version=API_VERSION)
    if not client.use_proto_plus:
        raise SystemExit(
            "Set GOOGLE_ADS_USE_PROTO_PLUS=true: the row mappers read proto-plus messages"
        )
    service = client.get_service("GoogleAdsService", version=API_VERSION)
    if args.record:
        service = RecordingGoogleAdsService(service, args.record)
    return GoogleAdsExtractor(service)


def _warehouse(args: argparse.Namespace, parser: argparse.ArgumentParser) -> Warehouse:
    if args.warehouse == "duckdb":
        return DuckDBWarehouse(args.duckdb_path)
    if not args.bq_project:
        parser.error("--bq-project (or BQ_PROJECT) is required when --warehouse is bigquery")
    from google.cloud import bigquery

    client = bigquery.Client(project=args.bq_project, location=args.bq_location)
    return BigQueryWarehouse(client, args.bq_dataset, args.bq_location)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    customer_ids = args.customer_ids or [
        cid for cid in os.environ.get("GADS_CUSTOMER_IDS", "").split(",") if cid.strip()
    ]
    if not customer_ids:
        parser.error("no accounts given: pass --customer-id or set GADS_CUSTOMER_IDS")
    if args.lookback_days < 1:
        parser.error("--lookback-days must be at least 1")
    if args.start and args.end and args.start > args.end:
        parser.error(f"--start {args.start} is after --end {args.end}")

    warehouse = _warehouse(args, parser)
    try:
        # Dates left out are worked out per account, in the account's time zone.
        results = run(
            _extractor(args),
            warehouse,
            customer_ids,
            args.start,
            args.end,
            args.snapshot_date,
            lookback_days=args.lookback_days,
        )
    except PipelineError as error:
        log.error("%s", error)
        return 1
    finally:
        if isinstance(warehouse, DuckDBWarehouse):
            warehouse.close()

    for result in results:
        print(
            f"{result.customer_id}  {result.table:<26} {result.start} to {result.end}"
            f"  {result.rows:>6} rows"
        )
    return 0
