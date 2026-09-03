"""Tests for the shared pagination helper."""

from ra_ingest.pagination import paginate


def test_single_page():
    pages = {1: (["a", "b"], 1)}
    assert paginate(lambda p: pages[p]) == ["a", "b"]


def test_multiple_pages():
    pages = {
        1: (["a", "b"], 3),
        2: (["c", "d"], 3),
        3: (["e"], 3),
    }
    assert paginate(lambda p: pages[p]) == ["a", "b", "c", "d", "e"]


def test_stops_at_total_pages():
    calls = []

    def fetch(page):
        calls.append(page)
        return (["x"], 2)

    paginate(fetch)
    assert calls == [1, 2]  # never fetches page 3


def test_none_stops_immediately():
    assert paginate(lambda p: None) == []


def test_none_midway_returns_partial():
    def fetch(page):
        if page == 1:
            return (["a"], 5)
        return None  # e.g. an error response on page 2

    assert paginate(fetch) == ["a"]
