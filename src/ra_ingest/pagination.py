"""Accumulate every page of a paginated ZMS list endpoint into one list.

The generated ZMS clients (and the zms-ra REST API) return one page at a time
and none of them auto-paginate. This is the shared page loop every `list_*`
helper uses: the caller supplies a `fetch_page` that returns `(items, pages)`
for a given 1-based page number, or `None` to stop (e.g. an error response).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def paginate(fetch_page: Callable[[int], tuple[list[T], int] | None]) -> list[T]:
    """Call fetch_page(1), fetch_page(2), ... accumulating items until the last
    page (or until fetch_page returns None)."""
    items: list[T] = []
    page = 1
    while True:
        result = fetch_page(page)
        if result is None:
            break
        page_items, total_pages = result
        items.extend(page_items)
        if page >= total_pages:
            break
        page += 1
    return items
