# Amazon Ads reporting v3 to BigQuery, modelled with dbt

A small, production-shaped pipeline for **Sponsored Products** reporting:

1. **Extract**: requests the Amazon Ads API **v3 reports** `spCampaigns` and `spAdvertisedProduct` (create report, poll status, download the gzipped JSON) for every seller and vendor profile, or the profiles you name, across the NA, EU and FE regions.
2. **Load**: writes them into **date-partitioned, clustered BigQuery tables**. Loads are idempotent: each report window replaces exactly the profile and dates it covers.
3. **Transform**: a **dbt** project builds daily campaign and product (ASIN) marts, with tests for keys, accepted values, relationships, source freshness, and a reconciliation of campaign spend against the sum of its advertised products.

It is written for the usual situation: **the client already has approved Amazon Ads API access** and provides the credentials. Everything can be checked without them: `make test` runs the real client code against an offline fake of the API that serves synthetic reports, loads DuckDB and runs `dbt build` on it.

## Architecture

```
 Amazon Ads API  (NA advertising-api.amazon.com, EU advertising-api-eu.amazon.com,
                  FE advertising-api-fe.amazon.com)
        |
        |   amazon-ads-pipeline  (src/amazon_ads_pipeline)
        |     Login with Amazon: refresh token -> access token, renewed before expiry
        |     GET  /v2/profiles                       per region; sellers and vendors
        |     POST /reporting/reports                 spCampaigns + spAdvertisedProduct,
        |                                             DAILY, GZIP_JSON, <= 31 days each
        |     GET  /reporting/reports/{id}            PENDING / PROCESSING / COMPLETED / FAILED
        |     GET  pre-signed S3 url                  gunzip, JSON money -> Decimal
        |     retries: 429 honours Retry-After, else 5s, 10s, 20s... ; 5xx; network
        v
 BigQuery dataset amazon_ads_raw
        sp_campaign_daily | sp_advertised_product_daily   partitioned by date
        profiles                                          partitioned by snapshot_date
        |
        |   load = staging table, then one transaction:
        |          DELETE the profile's report window, INSERT the staged rows
        v
 dbt  (transform/)
        staging:  stg_amazon_ads__profiles, __sp_campaign_daily, __sp_advertised_product_daily
        marts:    fct_sp_campaign_performance_daily   (ACOS, ROAS, CPC, CTR, budget use)
                  fct_sp_product_performance_daily    (per advertised ASIN)
        tests:    keys, accepted values, relationships, freshness, reconciliation, unit test
```

## Decisions a reviewer should know about

| Topic | What the code does, and why |
|---|---|
| Idempotent loads | Each report window goes to a staging table; one BigQuery transaction deletes that profile's rows for the window and inserts the new ones. Re-running a range, or a rolling window that overlaps earlier runs, never duplicates. The loader refuses rows outside the window it replaces. |
| Dates and time zones | Amazon reports dates in each profile's time zone. Without `--end`, each profile's window ends on its own yesterday, so a run at 02:30 UTC does not load a US profile's unfinished day (still the evening before in Los Angeles) as if it were complete. |
| Late attribution | The default run reloads the last 30 days (`--lookback-days`), so 7 and 14 day attributed sales, which keep changing after the click, are refreshed. |
| Request everything, then poll | All reports are requested first, because Amazon generates them in parallel, then polled with backoff (15s growing to 2 min, 3 hour limit). Each report is downloaded and loaded as soon as it completes, well inside the download link's one-hour life. |
| One profile failing | Each profile and report is handled on its own. If one fails (a profile the credentials cannot access, a report Amazon marks `FAILED`, a report still pending at the 3-hour limit), the rest still load, and the run then exits with code 1 and lists every failure. |
| Duplicate requests | Amazon answers `425` when an identical report is still being generated ("Request is a duplicate of a processing request"), for example after a crashed run. Amazon's spec does not describe the error body. When the message carries the existing report's ID, as developers report (`The Request is a duplicate of : <reportId>`), the client waits for that report; otherwise that report fails and is listed. |
| Rate limits | `429` and `5xx` are retried. The client waits for `Retry-After` when the response has one and backs off exponentially otherwise: Amazon's reporting spec says only "Retry later" for `429`, and developers report the header missing on `POST /reporting/reports`. |
| Tokens | Access tokens are cached and renewed a minute before expiry. A `401` triggers one refresh and one retry. A revoked refresh token (`invalid_grant`) fails immediately with Amazon's message. |
| Money | Report JSON is parsed with `Decimal`, loaded as NUMERIC. No floats touch currency. |
| Download | The report URL is a pre-signed S3 link; it is fetched without the API's `Authorization` header, which S3 would reject. The body is gunzipped only while it is still gzip, so it also parses if S3 serves it with `Content-Encoding: gzip` and the HTTP client has already decompressed it. Signed URLs are kept out of the logs. |
| Regions | A profile belongs to one region and is only visible on that region's host. The pipeline lists profiles per region and records the region in `profiles`. |

## Amazon Ads API facts this is built on (checked 24 September 2026)

- Reporting v3 flow, headers and statuses: `POST /reporting/reports` with `Content-Type: application/vnd.createasyncreportrequest.v3+json`, `Amazon-Advertising-API-ClientId` and `Amazon-Advertising-API-Scope` (the profile ID); `GET /reporting/reports/{reportId}` returns `PENDING`, `PROCESSING`, `COMPLETED` or `FAILED`; `format` is `GZIP_JSON`; `425` is "Too Early - Request is a duplicate of a processing request"; the download URL "defaults to 3600 seconds"; generation "can take as long as 3 hours"; "Most report types support 95 days as lookback window". Source: Amazon's OpenAPI specification for reporting v3 ([OfflineReport_prod_3p.json](https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/OfflineReport_prod_3p.json)) and Amazon's official [Postman collection](https://github.com/amzn/ads-advanced-tools-docs/tree/main/postman), whose examples also show `groupBy: ["advertiser"]` for `spAdvertisedProduct`.
- Profiles: `timezone` is "The time zone used for all date-based campaign management and reporting"; `accountInfo.type` is `seller` or `vendor` for sponsored ads and `agency` for DSP; `accountInfo.name` is "Not currently populated for sellers". Source: Amazon's Profiles OpenAPI specification ([Profiles_prod_3p.json](https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/Profiles_prod_3p.json)).
- Report columns are not in the OpenAPI specification. `date`, `campaignId`, `adGroupId`, `adId`, `advertisedAsin`, `advertisedSku`, `impressions`, `clicks`, `cost`, `purchases7d`, `purchases14d` and `sales7d` appear in Amazon's Postman examples; `campaignName`, `campaignStatus`, `campaignBudgetAmount`, `adGroupName`, `unitsSoldClicks7d` and `sales14d` match the columns Airbyte's open-source Amazon Ads connector requests for the same report types. Check them against Amazon's report-type pages before the first run.
- At most 31 days per report request: [ads-advanced-tools-docs discussion #157](https://github.com/amzn/ads-advanced-tools-docs/discussions/157).
- Regional hosts: NA `advertising-api.amazon.com`, EU `advertising-api-eu.amazon.com`, FE `advertising-api-fe.amazon.com`. Token endpoints: `api.amazon.com`, `api.amazon.co.uk`, `api.amazon.co.jp` (`/auth/o2/token`); "You can create, verify, and refresh tokens using any regional LWA endpoint" ([LWA docs](https://developer.amazon.com/docs/login-with-amazon/authorization-code-grant.html)).
- `Retry-After` missing on `429` from `POST /reporting/reports`: [issue #344](https://github.com/amzn/ads-advanced-tools-docs/issues/344).
- In Amazon's Postman collection for the unified Ads API (`/adsApi/v1`), Reports are listed under "Unified API — Beta", and of beta resources Amazon says: "Treat them as unstable and avoid depending on them in production integrations" ([Postman collections README](https://github.com/amzn/ads-advanced-tools-docs/blob/main/postman/README.md)). This project therefore uses reporting v3.

Further reading: [Reporting v3 get started](https://advertising.amazon.com/API/docs/en-us/guides/reporting/v3/get-started), [rate limiting](https://advertising.amazon.com/API/docs/en-us/reference/concepts/rate-limiting).

## Run it against a real account

**What the client provides**

1. The **client ID and client secret** of their Login with Amazon application that is approved for the Amazon Ads API. This project does not cover applying for access.
2. A **refresh token** from the authorization grant (scope `advertising::campaign_management`) of a user who can see the advertising accounts.
3. Which **regions** (NA, EU, FE) and, optionally, which **profile IDs** to load. Without profile IDs, every seller and vendor profile in the regions is loaded.

**Google Cloud**: the identity running the job needs **BigQuery Job User** on the project and **BigQuery Data Editor** on the datasets it writes: `amazon_ads_raw` for the pipeline, `<BQ_DATASET>_staging` and `<BQ_DATASET>_marts` for dbt (or Data Editor on the project to have them created). Store the client secret and refresh token in a secret manager, not in the repository. `.env.example` lists every variable.

```bash
python -m venv .venv
.venv/bin/pip install -r requirements/prod.txt && .venv/bin/pip install --no-deps .

export AMAZON_ADS_CLIENT_ID=... AMAZON_ADS_CLIENT_SECRET=... AMAZON_ADS_REFRESH_TOKEN=...
export BQ_PROJECT=my-gcp-project BQ_LOCATION=US

# Daily run: reload the last 30 days in NA and EU, ending yesterday in each profile's time zone
.venv/bin/amazon-ads-pipeline --region NA --region EU

# Backfill a range (split into 31-day report windows automatically)
.venv/bin/amazon-ads-pipeline --region NA --profile-id 1234567890123456 --start 2026-07-01 --end 2026-09-14

# Transform
cd transform && DBT_TARGET=prod ../.venv/bin/dbt build && DBT_TARGET=prod ../.venv/bin/dbt source freshness
```

Schedule both steps once a day. To keep real reports for offline tests, add `--record recordings/` and replay them with `--replay recordings/`. Recordings contain client data, so anonymise them before committing.

## Run the tests (no credentials needed)

```bash
make test
```

This creates `.venv` from the pinned `requirements/dev.txt`, then runs:

| Step | What it checks | Result on 24 Sep 2026 |
|---|---|---|
| `ruff check`, `ruff format --check` | lint and formatting | clean |
| `pytest` | token refresh and caching, headers and media type, 429 with and without Retry-After, 5xx and network retries, fail-fast on 4xx, 425 duplicate reuse, unauthenticated gzip download with exact decimals (also when already decompressed), 31-day windows, the default date window in each profile's time zone, row mapping, region routing, FAILED reports and timeouts (a timeout still lists the other failures), DuckDB idempotency, BigQuery loader calls and SQL (mocked client, SQL parsed as BigQuery), CLI, and dbt: build passes, 6 deliberately broken datasets are each caught by the right tests, and a sub-cent rounding difference does not break the build | 75 passed |
| `amazon-ads-pipeline --replay ... --warehouse duckdb`, then `dbt build` and `dbt source freshness` | the whole chain on the synthetic reports | PASS=41 (5 models, 35 data tests, 1 unit test), freshness 3 of 3 pass |

`make bigquery-check` installs `requirements/prod.txt`, compiles the dbt project for the BigQuery target and parses every compiled model and test with sqlglot's BigQuery dialect (40 of 40 pass). It needs no Google Cloud project and executes nothing. sqlglot is a lenient parser: it catches broken SQL such as unbalanced parentheses or a bad macro expansion, but it accepts some SQL that BigQuery would reject and checks no types or permissions.

**About the fake API and the test fixtures.** `src/amazon_ads_pipeline/replay.py` implements the endpoints above as Amazon's specification and Postman collection describe them (token grant, regional profiles, media type check, 31-day limit, PENDING to PROCESSING to COMPLETED, 425 on duplicates, pre-signed download that rejects an Authorization header, GZIP_JSON); the wording of its 425 message follows what developers report. The reports it serves in `tests/fixtures/amazon_ads/` are synthetic: two seller profiles (US and UK), five campaigns, 1 to 14 September 2026, built by `scripts/generate_fixtures.py` so that campaign spend equals the sum of the product ads, as in a real account. A test checks the script reproduces them byte for byte. `tests/fixtures/amazon_ads/api/` holds API responses in the documented shapes used by the client tests.

## Sample output

`make demo` prints the marts for 7 September 2026. Numbers come from the synthetic accounts.

`fct_sp_campaign_performance_daily`

| date_day | country | currency | campaign_name | campaign_status | impressions | clicks | cost | purchases_7d | sales_7d | acos_7d | roas_7d | budget_utilization |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-07 | US | USD | SP - Manual - Exact - Brand | ENABLED | 1389 | 14 | 15.69 | 0 | 0.00 |  | 0 | 0.52 |
| 2026-09-07 | US | USD | SP - Auto - Tents | ENABLED | 1087 | 9 | 14.00 | 1 | 149.99 | 0.0933 | 10.71 | 0.28 |
| 2026-09-07 | US | USD | SP - Manual - Broad - Camping | PAUSED | 1114 | 7 | 11.62 | 1 | 59.99 | 0.1937 | 5.16 | 0.46 |
| 2026-09-07 | UK | GBP | SP - Auto - UK | ENABLED | 1509 | 10 | 10.83 | 1 | 59.98 | 0.1806 | 5.54 | 0.27 |
| 2026-09-07 | UK | GBP | SP - Manual - UK - Brand | ENABLED | 982 | 2 | 1.55 | 1 | 159.98 | 0.0097 | 103.21 | 0.08 |

`fct_sp_product_performance_daily` (US)

| date_day | advertised_asin | campaigns | ads | impressions | clicks | cost | purchases_7d | sales_7d | acos_7d |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-07 | B0DEMO0003 | 2 | 2 | 1167 | 10 | 15.10 | 1 | 59.99 | 0.2517 |
| 2026-09-07 | B0DEMO0002 | 1 | 1 | 830 | 8 | 12.89 | 1 | 149.99 | 0.0859 |
| 2026-09-07 | B0DEMO0001 | 2 | 2 | 1283 | 11 | 11.99 | 0 | 0.00 |  |
| 2026-09-07 | B0DEMO0004 | 1 | 1 | 310 | 1 | 1.33 | 0 | 0.00 |  |

A day with spend and no sales has no ACOS (blank), not zero and not a division error.

## dbt tests

| Test | Type | Fails when |
|---|---|---|
| `unique_combination` on every model's grain, `unique` on profile | generic (in `transform/tests/generic`) and built-in | a row is duplicated, e.g. a load appended instead of replacing |
| `not_null` on keys, dates, currency and spend | built-in | a key or amount is missing |
| `accepted_values` on campaign status, region and account type | built-in | an unexpected value appears |
| `relationships` from the campaign mart to profiles | built-in | reports exist for a profile the profile list does not know |
| source freshness on `_loaded_at` | built-in | no load for 26 hours (warn) or 50 hours (error) |
| `assert_campaign_spend_matches_advertised_products` | singular | a campaign's spend differs from the sum of its product ads by more than 0.5% (or one cent) on any day, or a campaign-day with spend is in one report only |
| `ratios_are_null_without_a_denominator` | dbt unit test | ACOS, ROAS, CPC or CTR handling of zero denominators changes |

## Project layout

```
src/amazon_ads_pipeline/
  regions.py      API host and token endpoint per region
  auth.py         Login with Amazon token provider
  client.py       profiles, create report, status, download; retries and 425 handling
  reports.py      report definitions, 31-day windows, row mappers
  warehouse.py    BigQuery and DuckDB loaders (same contract)
  pipeline.py     one run: profiles, request, poll, load
  replay.py       offline fake of the API, and the recorder for real runs
  cli.py          the amazon-ads-pipeline command
transform/        dbt project (profiles.yml: local = DuckDB, prod = BigQuery)
tests/            pytest suite, API response samples and synthetic reports
scripts/          fixture generator, mart printer, BigQuery SQL check
requirements/     pinned lock files (dev: tests and DuckDB, prod: runtime and dbt-bigquery)
```

## Limitations

- Tested against Amazon's documented behaviour through the fake API, not a live account. The first real run should be watched, and `--record` makes its reports reusable as fixtures.
- Sponsored Products only. Sponsored Brands and Display use other `reportTypeId` values and columns; adding one is a new `ReportSpec` and a staging model.
- Amazon's reporting specification does not say whether `campaignStatus` and `campaignBudgetAmount` in a daily report are each day's values or the values when the report ran. Treat them as current values, and `budget_utilization` for past days as approximate.
- Nor does it list the values `campaignStatus` can take. The accepted-values test allows `ENABLED`, `PAUSED` and `ARCHIVED` and fails the build on anything else, so a new value stops the marts until the list is updated.
- Amounts are in each profile's currency; there is no currency conversion.
- Raw table schemas are created once. A new column needs an `ALTER TABLE ... ADD COLUMN` before the deploy that starts filling it.
- Run one load per table at a time: BigQuery cancels one of two transactions that change the same table concurrently.

## License

MIT, see [LICENSE](LICENSE).
