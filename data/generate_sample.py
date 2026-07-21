#!/usr/bin/env python3
"""Generate a clearly-synthetic replay dataset for Market Pulse.

Writes ~200 fictional financial-news items to data/sample/news_sample.jsonl,
one JSON object per line, conforming to schemas/news.raw.schema.json.

Everything here is invented: fictional companies, tickers, and events. There is
NO copyrighted article text. This exists so the whole pipeline runs offline with
zero API keys (replay mode).

Deterministic: a fixed seed means the same dataset every run.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0.0"
SEED = 20260720
ITEM_COUNT = 200

# Fictional companies + tickers (invented, to keep the dataset clearly synthetic).
COMPANIES = [
    ("Nimbus Robotics", "NMBS"),
    ("Helios Energy", "HLES"),
    ("Aster Biosciences", "ASTB"),
    ("Quill Software", "QUIL"),
    ("Meridian Freight", "MRDF"),
    ("Cobalt Semiconductors", "CBLT"),
    ("Vireo Foods", "VIRO"),
    ("Larkspur Financial", "LKSP"),
    ("Onyx Motors", "ONYX"),
    ("Pallas Pharmaceuticals", "PLLS"),
    ("Sable Retail", "SABL"),
    ("Tessellate Cloud", "TSSL"),
]

FEEDS = ["synthetic-markets", "synthetic-wire", "synthetic-tech-desk", "synthetic-earnings"]

# Templates grouped by rough sentiment polarity so the downstream FinBERT stage
# has a realistic spread to score. These are hints for realism only — the labels
# are NOT written to the message (entities/sentiment come from Python inference).
POSITIVE = [
    ("{co} beats estimates as quarterly revenue jumps {pct}%",
     "SYNTHETIC SAMPLE - {co} ({tk}) reported quarterly revenue up {pct}% versus a year earlier, topping internal targets on stronger demand. All figures in this item are fictional."),
    ("{co} raises full-year guidance after record bookings",
     "SYNTHETIC SAMPLE - {co} ({tk}) lifted its full-year outlook, citing record new bookings and expanding margins. This is invented text for pipeline testing."),
    ("{co} lands major contract worth an estimated ${amt}M",
     "SYNTHETIC SAMPLE - {co} ({tk}) said it signed a multi-year contract valued at roughly ${amt} million, its largest to date. Fictional scenario, no real event."),
]
NEGATIVE = [
    ("{co} shares slide after it cuts outlook on weak demand",
     "SYNTHETIC SAMPLE - {co} ({tk}) lowered its guidance, warning that softening demand would pressure margins next quarter. Entirely synthetic content."),
    ("{co} misses revenue expectations, flags rising costs",
     "SYNTHETIC SAMPLE - {co} ({tk}) fell short of revenue expectations and pointed to rising input costs. All details are fabricated for testing."),
    ("{co} recalls product line, warns of ${amt}M charge",
     "SYNTHETIC SAMPLE - {co} ({tk}) announced a product recall and said it expects a one-time charge of about ${amt} million. Fictional scenario."),
]
NEUTRAL = [
    ("{co} names new chief financial officer",
     "SYNTHETIC SAMPLE - {co} ({tk}) said its board appointed a new chief financial officer effective next month. Invented text; no real people implied."),
    ("{co} to present at synthetic industry conference",
     "SYNTHETIC SAMPLE - {co} ({tk}) will present at a fictional industry conference and host a webcast for interested parties. Placeholder content."),
    ("{co} completes previously announced organizational review",
     "SYNTHETIC SAMPLE - {co} ({tk}) said it completed a routine organizational review with no material changes expected. Synthetic sample only."),
]
BUCKETS = POSITIVE + NEUTRAL + NEGATIVE


def build(rng: random.Random, i: int, published: datetime) -> dict:
    name, ticker = rng.choice(COMPANIES)
    title_t, body_t = rng.choice(BUCKETS)
    fields = {
        "co": name,
        "tk": ticker,
        "pct": rng.randint(3, 42),
        "amt": rng.choice([25, 40, 75, 120, 210, 350]),
    }
    published_iso = published.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    # ingested slightly after publish; the Go ingestor overwrites this at stream
    # time anyway, but the field is required by the schema.
    ingested = published + timedelta(seconds=rng.randint(1, 90))
    ingested_iso = ingested.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    item = {
        "schema_version": SCHEMA_VERSION,
        "id": f"replay-{i:06d}",
        "source": "replay",
        "feed": rng.choice(FEEDS),
        "url": f"https://example.invalid/replay/{i:06d}",
        "title": title_t.format(**fields),
        "body": body_t.format(**fields),
        "language": "en",
        "published_at": published_iso,
        "ingested_at": ingested_iso,
        "tickers": [ticker],
    }
    return item


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--count", type=int, default=ITEM_COUNT)
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "sample" / "news_sample.jsonl",
    )
    args = ap.parse_args()

    rng = random.Random(SEED)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    # Spread publish times over the ~24h before a fixed anchor for stable output.
    anchor = datetime(2026, 7, 20, 18, 0, 0, tzinfo=timezone.utc)
    with args.out.open("w", encoding="utf-8") as f:
        for i in range(args.count):
            published = anchor - timedelta(minutes=(args.count - i) * rng.randint(3, 9))
            f.write(json.dumps(build(rng, i, published), ensure_ascii=False) + "\n")

    print(f"wrote {args.count} synthetic items to {args.out}")


if __name__ == "__main__":
    main()
