"""Compile the dbt project for BigQuery and syntax-check every compiled statement, offline.

dbt-bigquery creates a BigQuery client at start-up, so it wants credentials even though
`dbt compile --no-populate-cache` sends no queries. This script hands it a throwaway
service-account key generated on the fly and never used for a request, compiles the
`prod` target, and parses each compiled model and test with sqlglot's BigQuery dialect.

It catches BigQuery syntax errors without a Google Cloud project. It executes nothing, so
type and permission errors still need a real run.

Usage: python scripts/check_bigquery_sql.py   (from an environment with requirements/prod.txt)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import sqlglot
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlglot.errors import ParseError

TRANSFORM = Path(__file__).resolve().parents[1] / "transform"


def write_throwaway_key(path: Path) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": "offline-check",
                "private_key_id": "offline-check",
                "private_key": pem,
                "client_email": "offline-check@offline-check.iam.gserviceaccount.com",
                "client_id": "0",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        write_throwaway_key(work / "key.json")
        env = {
            **os.environ,
            "GOOGLE_APPLICATION_CREDENTIALS": str(work / "key.json"),
            "BQ_PROJECT": "offline-check",
            "DBT_TARGET": "prod",
        }
        dbt = Path(sys.executable).with_name("dbt")
        subprocess.run(
            [
                str(dbt), "compile", "--target", "prod", "--no-populate-cache",
                "--project-dir", str(TRANSFORM),
                "--profiles-dir", str(TRANSFORM),
                "--target-path", str(work / "target"),
                "--log-path", str(work / "logs"),
            ],
            env=env,
            check=True,
        )  # fmt: skip
        compiled = sorted((work / "target" / "compiled").rglob("*.sql"))
        failures = []
        for path in compiled:
            try:
                sqlglot.parse(path.read_text(), read="bigquery")
            except ParseError as error:
                failures.append(f"{path.relative_to(work)}: {error}")

    for failure in failures:
        print(f"FAIL {failure}")
    passed = len(compiled) - len(failures)
    print(f"{passed} of {len(compiled)} compiled files parse as BigQuery SQL")
    return 1 if failures or not compiled else 0


if __name__ == "__main__":
    raise SystemExit(main())
