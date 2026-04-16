"""Picks the appropriate Spectrum for an Observation based on its freq range.

The service doesn't need a hardcoded spectrum_id -- at reconcile time it
looks up the element's spectrums and finds the narrowest one whose freq
range covers the observation.

This means: if the facility adds a new (narrower) spectrum later, events
that match it will automatically use it without any config change.
"""

from __future__ import annotations

import logging
from typing import cast

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import Spectrum, SpectrumList

LOG = logging.getLogger(__name__)


class SpectrumPicker:
    """Caches the element's spectrums and picks the best match for a freq range."""

    def __init__(self, client: ZmsZmcClient, element_id: str) -> None:
        self._client = client
        self._element_id = element_id
        self._spectrums: list[Spectrum] = []

    def refresh(self) -> int:
        """Reload spectrums from ZMC. Returns count loaded."""
        self._spectrums = _list_spectrums(self._client, self._element_id)
        LOG.info(
            "Loaded %d spectrums for element %s",
            len(self._spectrums),
            self._element_id,
        )
        return len(self._spectrums)

    def pick(self, min_freq_hz: int, max_freq_hz: int) -> Spectrum | None:
        """Return the narrowest spectrum whose range contains [min, max], or None."""
        candidates: list[tuple[int, Spectrum]] = []
        for spectrum in self._spectrums:
            bounds = _spectrum_bounds(spectrum)
            if bounds is None:
                continue
            lo, hi = bounds
            if lo <= min_freq_hz and hi >= max_freq_hz:
                candidates.append((hi - lo, spectrum))

        if not candidates:
            return None
        # Narrowest (most specific) spectrum wins
        candidates.sort(key=lambda t: t[0])
        return candidates[0][1]


def _list_spectrums(client: ZmsZmcClient, element_id: str) -> list[Spectrum]:
    """Fetch all spectrums for the element, elaborated with constraints."""
    out: list[Spectrum] = []
    page = 1
    while True:
        resp = client.list_spectrum(
            element_id=element_id,
            page=page,
            items_per_page=100,
            x_api_elaborate="true",
        )
        if not resp.is_success or not isinstance(resp.parsed, SpectrumList):
            LOG.error("Failed to list spectrums (page %d): %s", page, resp.status_code)
            break
        spec_list = cast(SpectrumList, resp.parsed)
        out.extend(spec_list.spectrum)
        if page >= spec_list.pages:
            break
        page += 1
    return out


def _spectrum_bounds(spectrum: Spectrum) -> tuple[int, int] | None:
    """Return (min_freq, max_freq) spanning all the spectrum's constraints."""
    constraints = spectrum.constraints
    if not constraints or not isinstance(constraints, list):
        return None
    lo = None
    hi = None
    for sc in constraints:
        c = sc.constraint
        if c is None:
            continue
        min_f = c.min_freq
        max_f = c.max_freq
        if min_f is None or max_f is None:
            continue
        lo = min_f if lo is None else min(lo, min_f)
        hi = max_f if hi is None else max(hi, max_f)
    if lo is None or hi is None:
        return None
    return lo, hi
