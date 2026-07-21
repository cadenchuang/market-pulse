from inference.entities import extract_entities

from .helpers import make_item


def test_ticker_hint_included():
    item = make_item(tickers=["NMBS"], title="Update", body="routine update")
    ents = extract_entities(item)
    assert any(e.type == "ticker" and e.text == "NMBS" for e in ents)


def test_company_name_match_in_text():
    item = make_item(body="Helios Energy announced a synthetic milestone today")
    ents = extract_entities(item)
    assert any(e.type == "company" and e.text == "Helios Energy" for e in ents)


def test_ticker_token_match_case_insensitive():
    item = make_item(body="shares of cblt moved on the synthetic news")
    ents = extract_entities(item)
    assert any(e.type == "ticker" and e.text == "CBLT" for e in ents)


def test_dedup_of_repeated_ticker():
    item = make_item(tickers=["NMBS", "NMBS"], body="nmbs nmbs nmbs")
    tickers = [e for e in extract_entities(item) if e.type == "ticker" and e.text == "NMBS"]
    assert len(tickers) == 1


def test_no_entities_when_nothing_matches():
    item = make_item(title="", body="a generic sentence with no known names")
    assert extract_entities(item) == []
