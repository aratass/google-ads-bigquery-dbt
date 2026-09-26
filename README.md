# Google Ads API to BigQuery, modelled with dbt

A small, production-shaped pipeline:

1. **Extract**: pulls daily account, campaign and ad group performance from the **Google Ads API v25** with GAQL (`SearchStream`), plus a daily snapshot of every campaign budget.
2. **Load**: writes it into **date-partitioned, clustered BigQuery tables**. Loads are idempotent: each run replaces exactly the account and date range it covers, so re-runs and overlapping windows never create duplicates.
3. **Transform**: a **dbt** project builds daily campaign performance and budget pacing marts, with tests for keys, accepted values, source freshness, and a reconciliation of campaign spend against account spend.

Everything can be checked without a Google Ads account: `make test` replays synthetic API responses (stored in the same format as real recordings), loads them into DuckDB and runs `dbt build` on it. Production runs the same extractor and dbt models against the Google Ads API and BigQuery; only the loader class differs.

## Architecture

```
 Google Ads API v25 (GoogleAdsService.SearchStream, GAQL)
        |
        |   gads-pipeline  (src/google_ads_pipeline)
        |     4 queries per account: account, campaign and ad group per day,
        |     plus the current budget of every campaign (and 1 more to read the
        |     account's time zone when the dates are left to default)
        |     retries transient errors: 5s, 10s, 20s, 40s backoff with jitter
        |     cost_micros kept as INT64 and converted to an exact NUMERIC
        v
 BigQuery dataset google_ads_raw
        account_daily | campaign_daily | ad_group_daily   partitioned by date
        campaign_budget_snapshot                          partitioned by snapshot_date
        |
        |   load = staging table, then one transaction:
        |          DELETE the account's date range, INSERT the staged rows
        v
 dbt  (transform/)
        staging:       stg_google_ads__*          views, one per raw table
        intermediate:  int_google_ads__budget_daily
        marts:         fct_campaign_performance_daily
                       fct_budget_pacing_daily
        tests:         keys, accepted values, freshness, reconciliation, unit tests
```

## Decisions a reviewer should know about

| Topic | What the code does, and why |
|---|---|
| Idempotent loads | Rows go to a staging table; one BigQuery transaction deletes the account's rows for the date range and inserts the new ones. A load that fails leaves that table's previous rows in place. A run that fails part-way can leave some tables refreshed and others not; it exits with code 1, and re-running the same range repairs it. The loader refuses rows outside the range it is replacing, because those would survive the next re-run and be duplicated. |
| Dates and time zones | Google Ads reports `segments.date` in the account's time zone. Without `--end`, the window ends on the account's yesterday, worked out in that zone (one extra GAQL request per account reads it). A run at 02:30 UTC therefore does not load New York's unfinished day as if it were complete. The budget snapshot is filed under the account's today. |
| Late conversions | The default run reloads the last 30 days (`--lookback-days`), which matches Google's default 30-day conversion window, so late conversions and cost corrections overwrite the earlier numbers. Accounts with 60- or 90-day conversion windows need `--lookback-days 60` or `90`; that costs no extra requests. |
| Money | The API returns cost as int64 micros. `cost_micros` is stored as delivered, and `cost` is `Decimal(cost_micros) / 1,000,000`, loaded as NUMERIC, so spend is exact. `conversions` and `conversions_value` arrive from the API as doubles and are stored as FLOAT64. |
| Removed campaigns | The campaign query has no status filter. A removed campaign still owns the spend from the days it ran; dropping it breaks the match with account totals, and the reconciliation test catches exactly that. |
| Budget history | GAQL returns an attribute's current value only, never its value on a past date. The pipeline snapshots every campaign's budget on each run, and dbt picks the latest snapshot on or before each day. |
| Shared budgets | Pacing is computed per budget, not per campaign, because a shared budget caps the combined spend of all its campaigns. |
| One account failing | Each account loads on its own. If one fails (for example, access was removed), the others still load, and the run then exits with code 1 and lists the failures. |
| Retries | Retries the gRPC statuses UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, UNKNOWN, ABORTED and RESOURCE_EXHAUSTED, and the Google Ads errors `TRANSIENT_ERROR`, `INTERNAL_ERROR` and `RESOURCE_TEMPORARILY_EXHAUSTED`. Google's docs name `TRANSIENT_ERROR` and `INTERNAL_ERROR` as worth retrying and `RESOURCE_TEMPORARILY_EXHAUSTED` as the rate-limit error. The client library raises INTERNAL and RESOURCE_EXHAUSTED without their error details, so an exhausted daily quota (also RESOURCE_EXHAUSTED) is retried as well and fails after the fifth attempt, within about 75 seconds. Everything else fails fast, e.g. `CLOUD_PROJECT_NOT_APPROVED_FOR_PRODUCTION`. A stream that breaks halfway is restarted, never loaded partially. |

## Google Ads API access, as of September 2026

Checked against Google's documentation on 24 September 2026:

- **Developer tokens were sunset on 9 September 2026.** In Google's words, "Your API access levels are now determined by the Google Cloud project you used to generate your OAuth credentials": the project that owns the OAuth client ID and secret (user authentication), or the project that owns the service account (service-account workflow). You can still send a developer token, "but this is optional and ignored by the API servers", and Google "will start rejecting developer tokens in API calls in a future major version". This pipeline sends none, so leave `GOOGLE_ADS_DEVELOPER_TOKEN` unset. `google-ads` 32.0.0 removed the library's check that one is configured. ([Developer token](https://developers.google.com/google-ads/api/docs/api-policy/developer-token), [client library ChangeLog](https://github.com/googleads/google-ads-python/blob/main/ChangeLog)) Google's [OAuth overview](https://developers.google.com/google-ads/api/docs/oauth/overview) still carries an older note that a developer token is needed; the developer-token page is the specific, newer statement.
- Access levels (Test, Explorer, Basic, Standard) are shown on the **Google Ads API Overview** page of the Cloud project. Test access reaches test accounts only; a call to a production account then fails with `AuthorizationError.CLOUD_PROJECT_NOT_APPROVED_FOR_PRODUCTION` ("The Google Cloud project is only approved for use with test accounts" in the v25 error definitions). Explorer allows 2,880 operations a day on production accounts, and a `SearchStream` request counts as one operation however many rows it returns. This pipeline makes 4 requests per account per run, plus 1 to read the account's time zone when `--end` or `--snapshot-date` is left to default. ([Access levels](https://developers.google.com/google-ads/api/docs/api-policy/access-levels), [Quotas](https://developers.google.com/google-ads/api/docs/best-practices/quotas))
- A **service account can be added directly as a user** in Google Ads (Admin > Access and security), with no domain-wide delegation. For accounts you manage yourself, Google's advice is "Use service account workflow". ([Service accounts](https://developers.google.com/google-ads/api/docs/oauth/service-accounts), [OAuth overview](https://developers.google.com/google-ads/api/docs/oauth/overview))
- **v25** is the latest major version (22 July 2026); its latest minor release is v25.2 (23 September 2026). The pinned `google-ads` 32.0.0 serves v25; `google-ads` 33.0.0 (23 September 2026) adds v25.2 and drops v21 and v22. ([Release notes](https://developers.google.com/google-ads/api/docs/release-notes))

## Run it against a real account

**1. Google Cloud and Google Ads**

1. In a Google Cloud project, enable the Google Ads API and check that its access level on the [Google Ads API Overview page](https://console.cloud.google.com/google/ads-apis/overview) is Explorer or higher.
2. Pick an identity. The client library reads it from `GOOGLE_ADS_*` variables (or a `google-ads.yaml` named in `GOOGLE_ADS_CONFIGURATION_FILE_PATH`):
   - **Service account**, Google's recommended workflow for accounts you manage. Create it in the Cloud project from step 1 (that project's access level is the one that applies), add its email as a user in each Google Ads account (Admin > Access and security; read-only is enough) and set `GOOGLE_ADS_JSON_KEY_FILE_PATH` to its JSON key, kept in a secret manager.
   - **OAuth user.** Set `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET` and `GOOGLE_ADS_REFRESH_TOKEN` for a Google user who can see the accounts. The OAuth client must belong to the Cloud project from step 1.
   - **Application Default Credentials** (`GOOGLE_ADS_USE_APPLICATION_DEFAULT_CREDENTIALS=true`) are supported by the client library, with the `https://www.googleapis.com/auth/adwords` scope. Google's OAuth guide documents only the service-account and user workflows, and `gcloud` user credentials use gcloud's own OAuth client rather than one in your project, so test ADC against a test account before relying on it.
3. If the accounts sit under a manager account, set `GOOGLE_ADS_LOGIN_CUSTOMER_ID` to the manager's ID.
4. Give the same identity **BigQuery Job User** on the project and **BigQuery Data Editor** on the datasets it writes: `google_ads_raw` for the pipeline, `<BQ_DATASET>_staging` and `<BQ_DATASET>_marts` for dbt. Grant Data Editor on the project instead if they should be created automatically.

`.env.example` lists every variable.

**2. Install and run**

```bash
python -m venv .venv
.venv/bin/pip install -r requirements/prod.txt && .venv/bin/pip install --no-deps .

export GOOGLE_ADS_USE_PROTO_PLUS=true
export GOOGLE_ADS_JSON_KEY_FILE_PATH=/secrets/google-ads-sa.json   # or OAuth / ADC, see above
export BQ_PROJECT=my-gcp-project BQ_LOCATION=EU                   # BigQuery uses ADC

# Daily run: reload the last 30 days, ending yesterday in each account's time zone
.venv/bin/gads-pipeline --customer-id 123-456-7890 --customer-id 987-654-3210

# Backfill a fixed range
.venv/bin/gads-pipeline --customer-id 123-456-7890 --start 2026-06-01 --end 2026-08-31

# Transform
cd transform && DBT_TARGET=prod ../.venv/bin/dbt build && DBT_TARGET=prod ../.venv/bin/dbt source freshness
```

Schedule both steps once a day (Cloud Run job, Cloud Composer or cron). dbt writes staging views to `<BQ_DATASET>_staging` and marts to `<BQ_DATASET>_marts`.

To keep a copy of real responses for offline tests, add `--record recordings/`; replay them with `--replay recordings/`. Recordings contain client data, so anonymise them before committing.

## Run the tests (no account needed)

```bash
make test
```

This creates `.venv` from the pinned `requirements/dev.txt`, then runs:

| Step | What it checks | Result on 24 Sep 2026 |
|---|---|---|
| `ruff check`, `ruff format --check` | lint and formatting | clean |
| `pytest` | GAQL fields exist in the v25 API, micros conversion, retry and fail-fast rules, the default date window in the account's time zone, a re-pull of restated days, BigQuery loader calls and SQL (mocked client, SQL parsed as BigQuery), DuckDB idempotency, CLI, and dbt: build passes, 6 deliberately broken datasets are each caught by the right test, and 2 tolerated changes (a 0.4% spend drift, a new channel type) do not break the build | 96 passed |
| `gads-pipeline --replay ... --warehouse duckdb`, then `dbt build` and `dbt source freshness` | the whole chain on the synthetic responses | PASS=55 (7 models, 46 data tests, 2 unit tests), freshness 4 of 4 pass |

`make bigquery-check` installs `requirements/prod.txt` and compiles the dbt project for the BigQuery target, then parses every compiled model and test with sqlglot's BigQuery dialect (53 of 53 pass). It needs no Google Cloud project and executes nothing. sqlglot is a lenient parser: it catches broken SQL such as unbalanced parentheses or a bad macro expansion, but it accepts some SQL that BigQuery would reject and checks no types or permissions.

**About the test fixtures.** The files in `tests/fixtures/google_ads/` are synthetic: one invented account (time zone America/New_York), five campaigns, 1 to 14 September 2026. They are built with the `google-ads` v25 message classes and stored in the API's JSON wire format, exactly as `--record` saves live responses, and the tests parse them back through those classes, so a fixture that drifts from the API schema fails. `scripts/generate_fixtures.py` rebuilds them and a test checks it reproduces them byte for byte.

## Sample output

`make demo` prints the marts for the last loaded day. Numbers come from the synthetic account.

`fct_campaign_performance_daily` (2026-09-14)

| date_day | campaign_name | campaign_status | impressions | clicks | cost | conversions | ctr | cpc | roas |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-14 | PMax \| All products | ENABLED | 3723 | 298 | 265.95 | 13.41 | 0.08 | 0.89 | 7.75 |
| 2026-09-14 | Generic \| Search \| Tents | ENABLED | 1934 | 88 | 156.22 | 2.53 | 0.0455 | 1.78 | 3.29 |
| 2026-09-14 | Brand \| Search | ENABLED | 1160 | 71 | 38.59 | 11.06 | 0.0612 | 0.54 | 21.94 |

`fct_budget_pacing_daily` (2026-09-14)

| date_day | budget_name | shared | daily_budget | spend | mtd_spend | mtd_budget | pacing_ratio | projected_month_spend | month_budget | pacing_status |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-14 | Brand search (shared) | True | 60.00 | 38.59 | 518.08 | 840.00 | 0.6168 | 1110.17 | 1800.00 | UNDER |
| 2026-09-14 | Generic tents | False | 150.00 | 156.22 | 1931.25 | 2100.00 | 0.9196 | 4138.39 | 4500.00 | ON_TRACK |
| 2026-09-14 | PMax all products | False | 220.00 | 265.95 | 3501.57 | 3080.00 | 1.1369 | 7503.36 | 6600.00 | OVER |
| 2026-09-14 | Display remarketing | False | 40.00 | 0.00 | 256.99 | 560.00 | 0.4589 | 550.69 | 1200.00 | UNDER |

The paused display campaign keeps a pacing row with zero spend: days without delivery still count against the budget.

## dbt tests

| Test | Type | Fails when |
|---|---|---|
| `unique_combination` on every model's grain | generic (in `transform/tests/generic`) | a row is duplicated, e.g. a load appended instead of replacing |
| `not_null` on keys, dates, currency and spend | built-in | a key or amount is missing |
| `accepted_values` on campaign, ad group and budget enums, and `pacing_status` | built-in | an unexpected status appears (a new channel type only warns) |
| source freshness on `_loaded_at` | built-in | no load for 26 hours (warn) or 50 hours (error) |
| `assert_campaign_spend_reconciles_with_account` | singular | campaign spend differs from account spend by more than 0.5% on any day, or a day with spend exists on one side only |
| `assert_budget_pacing_covers_all_campaign_spend` | singular | the budget lookup drops or double counts a campaign's spend |
| `budget_in_force_follows_the_latest_snapshot`, `pacing_status_uses_month_to_date_totals` | dbt unit tests | the as-of budget logic changes (a budget raised mid-month, a campaign moving to another budget, days before the first snapshot) or the pacing arithmetic changes |

## Project layout

```
src/google_ads_pipeline/
  reports.py      GAQL queries and row mappers (micros conversion lives here)
  extract.py      SearchStream runner and the transient-error rules
  backoff.py      retry policy
  warehouse.py    BigQuery and DuckDB loaders (same contract)
  pipeline.py     one run: every report, every account
  replay.py       record live responses / replay recorded ones
  cli.py          the gads-pipeline command
transform/        dbt project (profiles.yml: local = DuckDB, prod = BigQuery)
tests/            pytest suite and the synthetic API responses
scripts/          fixture generator, mart printer, BigQuery SQL check
requirements/     pinned lock files (dev: tests and DuckDB, prod: runtime and dbt-bigquery)
```

## Limitations

- Nothing here has run against a live Google Ads account or BigQuery project yet. The GAQL fields are checked against the v25 client library's message definitions and follow the shape of Google's published example queries; the BigQuery loader is tested against a mocked client and a SQL parser. Watch the first real run, and keep its responses with `--record`.
- Budget history starts with the first snapshot. Days before it use the earliest snapshot, the only information GAQL gives.
- `month_budget` is the daily budget times the calendar days in the month. Google caps a month's charges at 30.4 times the average daily budget, so `month_budget` runs from about 8% below Google's cap (February) to about 2% above it (31-day months).
- Performance Max campaigns have no ad groups, so ad group spend does not add up to campaign spend for them. The reconciliation is campaign against account.
- Amounts are in each account's currency; there is no currency conversion.
- Raw table schemas are created once. A new column needs an `ALTER TABLE ... ADD COLUMN` before the deploy that starts filling it.
- Run one load per table at a time: BigQuery cancels one of two transactions that change the same table concurrently.

## License

MIT, see [LICENSE](LICENSE).
