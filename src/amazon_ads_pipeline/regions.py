"""Amazon Ads API regions. A profile lives in one region and is only served by that host."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Region:
    code: str
    api_url: str
    token_url: str  # Login with Amazon; any regional endpoint can refresh a token


REGIONS = {
    "NA": Region(
        "NA", "https://advertising-api.amazon.com", "https://api.amazon.com/auth/o2/token"
    ),
    "EU": Region(
        "EU", "https://advertising-api-eu.amazon.com", "https://api.amazon.co.uk/auth/o2/token"
    ),
    "FE": Region(
        "FE", "https://advertising-api-fe.amazon.com", "https://api.amazon.co.jp/auth/o2/token"
    ),
}


def get_region(code: str) -> Region:
    try:
        return REGIONS[code.upper()]
    except KeyError:
        raise ValueError(f"Unknown region {code!r}: expected one of {', '.join(REGIONS)}") from None
