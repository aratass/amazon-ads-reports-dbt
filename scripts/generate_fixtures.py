"""Generate the synthetic Amazon Ads recordings used by the tests and the offline demo.

The data is invented; the shapes follow Amazon's reporting v3 and profile responses. Two
seller profiles (US in region NA, UK in region EU), five Sponsored Products campaigns,
1 to 14 September 2026. The campaign report is the sum of the advertised product report,
cent for cent, as it is in a real account. An agency (DSP) profile is listed in NA to show
that the pipeline skips it.

Usage: python scripts/generate_fixtures.py [output_dir]
"""

from __future__ import annotations

import json
import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

FIRST_DAY = date(2026, 9, 1)
LAST_DAY = date(2026, 9, 14)
CENT = Decimal("0.01")


@dataclass(frozen=True)
class Ad:
    ad_id: int
    asin: str
    sku: str
    price: Decimal


@dataclass(frozen=True)
class Campaign:
    profile_id: int
    campaign_id: int
    name: str
    status: str
    budget: Decimal
    last_day: date
    ad_group_id: int
    ad_group_name: str
    cpc: float
    ads: tuple[Ad, ...]


US, UK = 3000000000000001, 3000000000000002

PROFILES = {
    "NA": [
        {
            "profileId": US,
            "countryCode": "US",
            "currencyCode": "USD",
            "dailyBudget": 999999999.0,
            "timezone": "America/Los_Angeles",
            "accountInfo": {
                "marketplaceStringId": "ATVPDKIKX0DER",
                "id": "A1DEMOSELLERUS",
                "type": "seller",
                "name": "",
            },
        },
        {
            "profileId": 3000000000000009,
            "countryCode": "US",
            "currencyCode": "USD",
            "timezone": "America/Los_Angeles",
            "accountInfo": {
                "marketplaceStringId": "ATVPDKIKX0DER",
                "id": "ENTITYDEMOAGENCY",
                "type": "agency",
                "name": "Demo Agency (DSP)",
            },
        },
    ],
    "EU": [
        {
            "profileId": UK,
            "countryCode": "UK",
            "currencyCode": "GBP",
            "dailyBudget": 999999999.0,
            "timezone": "Europe/London",
            "accountInfo": {
                "marketplaceStringId": "A1F83G8C2ARO7P",
                "id": "A1DEMOSELLERUK",
                "type": "seller",
                "name": "",
            },
        }
    ],
}

TENT_2P = Ad(700000000000001, "B0DEMO0001", "TENT-2P", Decimal("89.99"))
TENT_4P = Ad(700000000000002, "B0DEMO0002", "TENT-4P", Decimal("149.99"))
SLEEPING_BAG = Ad(700000000000004, "B0DEMO0003", "SLEEPBAG-R", Decimal("59.99"))
LANTERN = Ad(700000000000005, "B0DEMO0004", "LANTERN-1", Decimal("24.99"))

CAMPAIGNS = (
    Campaign(
        profile_id=US,
        campaign_id=500000000000001,
        name="SP - Auto - Tents",
        status="ENABLED",
        budget=Decimal("50.00"),
        last_day=LAST_DAY,
        ad_group_id=600000000000001,
        ad_group_name="Auto - all tents",
        cpc=1.35,
        ads=(TENT_2P, TENT_4P),
    ),
    Campaign(
        profile_id=US,
        campaign_id=500000000000002,
        name="SP - Manual - Exact - Brand",
        status="ENABLED",
        budget=Decimal("30.00"),
        last_day=LAST_DAY,
        ad_group_id=600000000000002,
        ad_group_name="Brand exact",
        cpc=1.05,
        # The same ASIN as TENT_2P, advertised from a second campaign.
        ads=(Ad(700000000000003, "B0DEMO0001", "TENT-2P", Decimal("89.99")), SLEEPING_BAG),
    ),
    # Paused after the 7th: no rows after that in either report.
    Campaign(
        profile_id=US,
        campaign_id=500000000000003,
        name="SP - Manual - Broad - Camping",
        status="PAUSED",
        budget=Decimal("25.00"),
        last_day=date(2026, 9, 7),
        ad_group_id=600000000000003,
        ad_group_name="Camping broad",
        cpc=1.45,
        ads=(LANTERN, Ad(700000000000006, "B0DEMO0003", "SLEEPBAG-R", Decimal("59.99"))),
    ),
    Campaign(
        profile_id=UK,
        campaign_id=510000000000001,
        name="SP - Auto - UK",
        status="ENABLED",
        budget=Decimal("40.00"),
        last_day=LAST_DAY,
        ad_group_id=610000000000001,
        ad_group_name="Auto - UK",
        cpc=1.10,
        ads=(
            Ad(710000000000001, "B0DEMOUK01", "TENT-2P-UK", Decimal("79.99")),
            Ad(710000000000002, "B0DEMOUK02", "MAT-UK", Decimal("29.99")),
        ),
    ),
    Campaign(
        profile_id=UK,
        campaign_id=510000000000002,
        name="SP - Manual - UK - Brand",
        status="ENABLED",
        budget=Decimal("20.00"),
        last_day=LAST_DAY,
        ad_group_id=610000000000002,
        ad_group_name="Brand UK",
        cpc=0.85,
        ads=(Ad(710000000000003, "B0DEMOUK01", "TENT-2P-UK", Decimal("79.99")),),
    ),
)


def _ad_day(rng: random.Random, campaign: Campaign, ad: Ad, day: date) -> dict:
    impressions = rng.randint(250, 1400)
    clicks = max(0, round(impressions * rng.uniform(0.002, 0.012)))
    cost = (Decimal(str(campaign.cpc * rng.uniform(0.8, 1.2))) * clicks).quantize(CENT)
    purchases_7d = sum(rng.random() < 0.08 for _ in range(clicks))
    purchases_14d = purchases_7d + (1 if purchases_7d and rng.random() < 0.3 else 0)
    units_7d = purchases_7d + (1 if purchases_7d and rng.random() < 0.2 else 0)
    return {
        "date": day.isoformat(),
        "campaignId": campaign.campaign_id,
        "adGroupId": campaign.ad_group_id,
        "adGroupName": campaign.ad_group_name,
        "adId": ad.ad_id,
        "advertisedAsin": ad.asin,
        "advertisedSku": ad.sku,
        "impressions": impressions,
        "clicks": clicks,
        "cost": cost,
        "purchases7d": purchases_7d,
        "sales7d": ad.price * units_7d,
        "unitsSoldClicks7d": units_7d,
        "purchases14d": purchases_14d,
        "sales14d": ad.price * (units_7d + purchases_14d - purchases_7d),
    }


_SUMMED = (
    "impressions",
    "clicks",
    "cost",
    "purchases7d",
    "sales7d",
    "unitsSoldClicks7d",
    "purchases14d",
    "sales14d",
)


def build_reports() -> dict[int, dict[str, list[dict]]]:
    rng = random.Random(20260901)
    reports: dict[int, dict[str, list[dict]]] = {
        US: {"spCampaigns": [], "spAdvertisedProduct": []},
        UK: {"spCampaigns": [], "spAdvertisedProduct": []},
    }
    day = FIRST_DAY
    while day <= LAST_DAY:
        for campaign in CAMPAIGNS:
            if day > campaign.last_day:
                continue
            ad_rows = [_ad_day(rng, campaign, ad, day) for ad in campaign.ads]
            reports[campaign.profile_id]["spAdvertisedProduct"].extend(ad_rows)
            reports[campaign.profile_id]["spCampaigns"].append(
                {
                    "date": day.isoformat(),
                    "campaignId": campaign.campaign_id,
                    "campaignName": campaign.name,
                    "campaignStatus": campaign.status,
                    "campaignBudgetAmount": campaign.budget,
                    **{key: sum(row[key] for row in ad_rows) for key in _SUMMED},
                }
            )
        day += timedelta(days=1)
    return reports


def _dump(payload: object, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, default=float) + "\n")
    return path


def write_fixtures(directory: Path) -> list[Path]:
    written = [
        _dump(profiles, directory / "api" / f"profiles_{region}.json")
        for region, profiles in PROFILES.items()
    ]
    for profile_id, by_type in build_reports().items():
        for report_type, records in by_type.items():
            written.append(
                _dump(records, directory / "reports" / str(profile_id) / f"{report_type}.json")
            )
    return written


if __name__ == "__main__":
    default = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "amazon_ads"
    for path in write_fixtures(Path(sys.argv[1]) if len(sys.argv) > 1 else default):
        print(f"wrote {path}")
