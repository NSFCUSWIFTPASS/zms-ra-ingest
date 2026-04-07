"""Standard radio astronomy frequency bands.

Maps observation frequencies to the band they belong to. Used to determine
which spectrum resource a claim should be attached to.
"""

from __future__ import annotations

from dataclasses import dataclass

# Standard radio astronomy bands (Hz)
BANDS: list[Band] = []


@dataclass(frozen=True)
class Band:
    name: str
    min_freq_hz: int
    max_freq_hz: int


# Default bands -- can be overridden via config
DEFAULT_BANDS = [
    Band("L-band", 1_000_000_000, 2_000_000_000),
    Band("S-band", 2_000_000_000, 4_000_000_000),
    Band("C-band", 4_000_000_000, 8_000_000_000),
    Band("X-band", 8_000_000_000, 12_000_000_000),
]


def find_band(freq_hz: int, bands: list[Band] | None = None) -> Band | None:
    """Find the band that contains the given frequency."""
    for band in bands or DEFAULT_BANDS:
        if band.min_freq_hz <= freq_hz < band.max_freq_hz:
            return band
    return None


def find_band_for_range(
    min_freq_hz: int, max_freq_hz: int, bands: list[Band] | None = None
) -> Band | None:
    """Find a band that fully contains the given frequency range."""
    for band in bands or DEFAULT_BANDS:
        if band.min_freq_hz <= min_freq_hz and max_freq_hz <= band.max_freq_hz:
            return band
    return None
