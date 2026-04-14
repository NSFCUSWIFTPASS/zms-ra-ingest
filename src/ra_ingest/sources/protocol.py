from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Observation:
    """A scheduled RA observation from an external source.

    This is the common representation that all sources produce. The
    reconciler POSTs these into zms-ra as RAObservation records, attached
    to the matching gcal grant.
    """

    ext_id: str
    name: str
    start: datetime.datetime
    end: datetime.datetime
    min_freq_hz: int
    max_freq_hz: int

    description: str = ""

    # ODS-specific fields for the zms-ra RAObservation model
    site_id: str = ""
    site_lat: float = 0.0
    site_lon: float = 0.0
    site_elevation: float = 0.0
    source_id: str = ""
    ra_j2000_deg: float = 0.0
    dec_j2000_deg: float = 0.0
    slew_sec: float = 1.0
    corr_int_sec: float = 1.0
    trk_rate_ra: float | None = None
    trk_rate_dec: float | None = None
    subarray: int = 0


class RASource(Protocol):
    """Interface that each RA data source implements."""

    @property
    def source_type(self) -> str:
        """The source type identifier, e.g. 'ra-ods'."""
        ...

    @property
    def source_name(self) -> str:
        """The facility identifier, e.g. 'hcro'."""
        ...

    def fetch_observations(self) -> list[Observation]:
        """Fetch current observations from this source.

        Returns all observations that should currently exist in zms-ra.
        Past/expired observations should not be returned.
        """
        ...
