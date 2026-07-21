"""Heuristic entity extraction.

This is deliberately a lightweight gazetteer matcher, NOT a trained NER model —
labeled as heuristic to keep claims honest. It combines:

  1. ticker hints already carried on the message (`tickers`), and
  2. gazetteer matches of known tickers / company names found in the text.

Phase 8's live sources can extend the gazetteer; a real NER model could replace
this behind the same function signature.
"""
from __future__ import annotations

import re

from .messages import ProcessedItem
from .results import Entity

# Fictional companies from the synthetic replay dataset (data/generate_sample.py).
# Keys are tickers, values are company names.
DEFAULT_GAZETTEER: dict[str, str] = {
    "NMBS": "Nimbus Robotics",
    "HLES": "Helios Energy",
    "ASTB": "Aster Biosciences",
    "QUIL": "Quill Software",
    "MRDF": "Meridian Freight",
    "CBLT": "Cobalt Semiconductors",
    "VIRO": "Vireo Foods",
    "LKSP": "Larkspur Financial",
    "ONYX": "Onyx Motors",
    "PLLS": "Pallas Pharmaceuticals",
    "SABL": "Sable Retail",
    "TSSL": "Tessellate Cloud",
}


def extract_entities(
    item: ProcessedItem, gazetteer: dict[str, str] | None = None
) -> list[Entity]:
    """Return de-duplicated entities for an item, preserving first-seen order."""
    gaz = gazetteer if gazetteer is not None else DEFAULT_GAZETTEER
    text = f"{item.title} {item.body}".lower()

    seen: set[tuple[str, str]] = set()
    out: list[Entity] = []

    def add(text_value: str, type_: str) -> None:
        key = (text_value.upper() if type_ == "ticker" else text_value, type_)
        if key in seen:
            return
        seen.add(key)
        out.append(Entity(text=text_value, type=type_))

    # 1) Ticker hints carried on the message.
    for tk in item.tickers:
        add(tk.upper(), "ticker")

    # 2) Gazetteer matches in the text.
    for ticker, company in gaz.items():
        if company.lower() in text:
            add(company, "company")
        # Ticker as a standalone token (word boundary), case-insensitive.
        if re.search(rf"\b{re.escape(ticker.lower())}\b", text):
            add(ticker, "ticker")

    return out
