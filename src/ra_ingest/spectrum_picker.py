"""Match an observation's frequency range to a Spectrum on the element.

pick(min, max) returns the spectrum whose narrowest single Constraint fully
contains [min, max]. Constraints are matched one at a time -- a gap between two
constraints is not covered.
"""

from __future__ import annotations

import logging

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import Spectrum, SpectrumList

from .pagination import paginate

LOG = logging.getLogger(__name__)


class SpectrumPicker:
    """Caches the element's spectrums and picks the best fit for a freq range."""

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
        """Return the spectrum whose narrowest constraint fully contains
        [min_freq_hz, max_freq_hz], or None if no single constraint does."""
        best: tuple[int, Spectrum] | None = None  # (constraint width, spectrum)
        for spectrum in self._spectrums:
            constraints = spectrum.constraints
            if not isinstance(constraints, list):
                continue
            for sc in constraints:
                c = getattr(sc, "constraint", None)
                if c is None or c.min_freq is None or c.max_freq is None:
                    continue
                if c.min_freq <= min_freq_hz and c.max_freq >= max_freq_hz:
                    width = c.max_freq - c.min_freq
                    if best is None or width < best[0]:
                        best = (width, spectrum)
        return best[1] if best else None


def _list_spectrums(client: ZmsZmcClient, element_id: str) -> list[Spectrum]:
    """Fetch all spectrums for the element, elaborated with constraints."""

    def fetch(page: int) -> tuple[list[Spectrum], int] | None:
        resp = client.list_spectrum(
            element_id=element_id,
            page=page,
            items_per_page=100,
            x_api_elaborate="true",
        )
        if not resp.is_success or not isinstance(resp.parsed, SpectrumList):
            LOG.error("Failed to list spectrums (page %d): %s", page, resp.status_code)
            return None
        return resp.parsed.spectrum, resp.parsed.pages

    return paginate(fetch)
