"""An offline stand-in for the Amazon Ads API that serves recorded reports.

FakeAmazonAdsApi implements the endpoints this pipeline calls, with the behaviour Amazon
documents for them:

- Login with Amazon: POST /auth/o2/token with grant_type=refresh_token.
- GET /v2/profiles lists only the profiles of the region whose host was called.
- POST /reporting/reports needs the v3 media type and a profile scope, rejects ranges
  over 31 days, answers PENDING, and answers 425 to a duplicate of a report in progress.
- GET /reporting/reports/{id} moves PENDING -> PROCESSING -> COMPLETED and then returns a
  pre-signed S3 URL, which rejects requests that carry an Authorization header.
- The download is GZIP_JSON: a gzipped JSON array with the requested columns.

Use it through httpx: `httpx.Client(transport=httpx.MockTransport(api.handle))`.
Recordings live in `<directory>/api/profiles_<REGION>.json` and
`<directory>/reports/<profileId>/<reportTypeId>.json`.
"""

from __future__ import annotations

import gzip
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx

from amazon_ads_pipeline.client import CREATE_REPORT_MEDIA_TYPE
from amazon_ads_pipeline.regions import REGIONS
from amazon_ads_pipeline.reports import MAX_DAYS_PER_REPORT

_REGION_BY_HOST = {httpx.URL(region.api_url).host: code for code, region in REGIONS.items()}
_TOKEN_HOSTS = {httpx.URL(region.token_url).host for region in REGIONS.values()}
_GROUP_BY = {"spCampaigns": ["campaign"], "spAdvertisedProduct": ["advertiser"]}


def _error(status: int, detail: str) -> httpx.Response:
    return httpx.Response(status, json={"code": str(status), "detail": detail})


class FakeAmazonAdsApi:
    def __init__(
        self,
        directory: str | Path,
        *,
        client_id: str = "amzn1.application-oa2-client.demo",
        polls_until_complete: int = 2,
    ) -> None:
        self._dir = Path(directory)
        self.client_id = client_id
        self.polls_until_complete = polls_until_complete
        self.requests: list[httpx.Request] = []
        self._tokens: set[str] = set()
        self._reports: dict[str, dict[str, Any]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        host, path = request.url.host, request.url.path
        if host in _TOKEN_HOSTS and path == "/auth/o2/token":
            return self._issue_token(request)
        if host.endswith(".s3.amazonaws.com"):
            return self._download(request)
        region = _REGION_BY_HOST.get(host)
        if region is None:
            return _error(404, f"Unknown host {host}")
        if request.headers.get("Amazon-Advertising-API-ClientId") != self.client_id:
            return _error(401, "Missing or unknown Amazon-Advertising-API-ClientId")
        if request.headers.get("Authorization", "").removeprefix("Bearer ") not in self._tokens:
            return _error(401, "Invalid or expired access token")
        if request.method == "GET" and path == "/v2/profiles":
            return httpx.Response(200, json=self._profiles(region))
        if request.method == "POST" and path == "/reporting/reports":
            return self._create_report(request, region)
        if request.method == "GET" and path.startswith("/reporting/reports/"):
            return self._report_status(request, region, path.rsplit("/", 1)[1])
        return _error(404, f"No route for {request.method} {path}")

    # Login with Amazon

    def _issue_token(self, request: httpx.Request) -> httpx.Response:
        form = {key: values[0] for key, values in parse_qs(request.content.decode()).items()}
        if form.get("grant_type") != "refresh_token" or form.get("client_id") != self.client_id:
            return httpx.Response(400, json={"error": "invalid_request"})
        if not form.get("refresh_token") or not form.get("client_secret"):
            return httpx.Response(400, json={"error": "invalid_grant"})
        token = f"Atza|offline-{len(self._tokens) + 1}"
        self._tokens.add(token)
        return httpx.Response(
            200,
            json={
                "access_token": token,
                "refresh_token": form["refresh_token"],
                "token_type": "bearer",
                "expires_in": 3600,
            },
        )

    # Profiles

    def _profiles(self, region: str) -> list[dict[str, Any]]:
        path = self._dir / "api" / f"profiles_{region}.json"
        return json.loads(path.read_text()) if path.exists() else []

    def _scoped_profile(self, request: httpx.Request, region: str) -> int | None:
        scope = request.headers.get("Amazon-Advertising-API-Scope", "")
        known = {int(profile["profileId"]) for profile in self._profiles(region)}
        return int(scope) if scope.isdigit() and int(scope) in known else None

    # Reporting v3

    def _create_report(self, request: httpx.Request, region: str) -> httpx.Response:
        if request.headers.get("Content-Type") != CREATE_REPORT_MEDIA_TYPE:
            return _error(415, f"Content-Type must be {CREATE_REPORT_MEDIA_TYPE}")
        profile_id = self._scoped_profile(request, region)
        if profile_id is None:
            return _error(403, "Not authorized to access scope")
        body = json.loads(request.content)
        problem = self._validate(body, profile_id)
        if problem:
            return _error(400, problem)

        key = json.dumps([profile_id, body["startDate"], body["endDate"], body["configuration"]])
        for report_id, report in self._reports.items():
            if report["key"] == key and report["status"] != "COMPLETED":
                return _error(425, f"The Request is a duplicate of : {report_id}")
        report_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{key}#{len(self._reports)}"))
        now = datetime.now(timezone.utc).isoformat()
        self._reports[report_id] = {
            "key": key,
            "profile_id": profile_id,
            "body": body,
            "status": "PENDING",
            "polls": 0,
            "createdAt": now,
        }
        return httpx.Response(200, json=self._describe(report_id))

    def _validate(self, body: dict[str, Any], profile_id: int) -> str | None:
        config = body.get("configuration", {})
        report_type = config.get("reportTypeId")
        if config.get("adProduct") != "SPONSORED_PRODUCTS":
            return "adProduct must be SPONSORED_PRODUCTS"
        if config.get("format") != "GZIP_JSON" or config.get("timeUnit") not in {
            "DAILY",
            "SUMMARY",
        }:
            return "format must be GZIP_JSON and timeUnit DAILY or SUMMARY"
        if config.get("groupBy") != _GROUP_BY.get(report_type):
            return f"groupBy {config.get('groupBy')} is not valid for {report_type}"
        start, end = date.fromisoformat(body["startDate"]), date.fromisoformat(body["endDate"])
        if not 0 <= (end - start).days < MAX_DAYS_PER_REPORT:
            return f"Date range must be 1 to {MAX_DAYS_PER_REPORT} days"
        records = self._records(profile_id, report_type)
        available = set(records[0]) if records else set(config.get("columns", []))
        unknown = [column for column in config.get("columns", []) if column not in available]
        return f"Invalid columns for {report_type}: {unknown}" if unknown else None

    def _report_status(self, request: httpx.Request, region: str, report_id: str) -> httpx.Response:
        report = self._reports.get(report_id)
        if report is None or self._scoped_profile(request, region) != report["profile_id"]:
            return _error(404, f"Report {report_id} not found")
        report["polls"] += 1
        if report["polls"] >= self.polls_until_complete:
            report["status"] = "COMPLETED"
        elif report["status"] == "PENDING":
            report["status"] = "PROCESSING"
        return httpx.Response(200, json=self._describe(report_id))

    def _describe(self, report_id: str) -> dict[str, Any]:
        report = self._reports[report_id]
        body = report["body"]
        completed = report["status"] == "COMPLETED"
        now = datetime.now(timezone.utc)
        return {
            "reportId": report_id,
            "name": body.get("name"),
            "status": report["status"],
            "startDate": body["startDate"],
            "endDate": body["endDate"],
            "configuration": {**body["configuration"], "filters": None},
            "createdAt": report["createdAt"],
            "updatedAt": now.isoformat(),
            "generatedAt": now.isoformat() if completed else None,
            "fileSize": len(self._file(report_id)) if completed else None,
            "url": (
                f"https://offline-report-storage-us-east-1-prod.s3.amazonaws.com/{report_id}"
                f"/report-{report_id}.json.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256"
                f"&X-Amz-Expires=3600&X-Amz-Signature=offline"
                if completed
                else None
            ),
            "urlExpiresAt": (now + timedelta(hours=1)).isoformat() if completed else None,
            "failureReason": None,
        }

    def _download(self, request: httpx.Request) -> httpx.Response:
        if "Authorization" in request.headers:
            # What S3 says when a pre-signed URL also carries an Authorization header.
            return httpx.Response(400, text="<Error><Code>InvalidArgument</Code></Error>")
        report_id = request.url.path.split("/")[1]
        if report_id not in self._reports:
            return httpx.Response(404, text="<Error><Code>NoSuchKey</Code></Error>")
        return httpx.Response(200, content=self._file(report_id))

    def _file(self, report_id: str) -> bytes:
        report = self._reports[report_id]
        body, config = report["body"], report["body"]["configuration"]
        records = [
            {column: record[column] for column in config["columns"]}
            for record in self._records(report["profile_id"], config["reportTypeId"])
            if body["startDate"] <= record["date"] <= body["endDate"]
        ]
        return gzip.compress(json.dumps(records).encode(), mtime=0)

    def _records(self, profile_id: int, report_type: str) -> list[dict[str, Any]]:
        path = self._dir / "reports" / str(profile_id) / f"{report_type}.json"
        return json.loads(path.read_text()) if path.exists() else []


class Recorder:
    """Saves live API payloads in the layout FakeAmazonAdsApi replays.

    Recordings contain the client's data: anonymise them before committing.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    def profiles(self, region: str, profiles: list[dict[str, Any]]) -> None:
        self._write(self._dir / "api" / f"profiles_{region}.json", profiles)

    def report(
        self,
        profile_id: int,
        report_type: str,
        start: date,
        end: date,
        records: list[dict[str, Any]],
    ) -> None:
        """Merge one downloaded window into the recording, replacing those dates."""
        path = self._dir / "reports" / str(profile_id) / f"{report_type}.json"
        kept = [
            record
            for record in (json.loads(path.read_text()) if path.exists() else [])
            if not start.isoformat() <= record["date"] <= end.isoformat()
        ]
        self._write(path, sorted([*kept, *records], key=lambda record: record["date"]))

    @staticmethod
    def _write(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Report amounts arrive as Decimal; a float's repr gives the same digits back.
        path.write_text(json.dumps(payload, indent=1, default=float) + "\n")
