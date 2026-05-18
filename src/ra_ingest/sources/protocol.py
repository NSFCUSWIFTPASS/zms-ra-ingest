from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ObsTarget:
    """Sky-pointing + site metadata for a single observation.

    Only sources that produce per-pointing RA observations (ODS, future
    TardyS4-shaped inputs) populate this. Calendar-source observations
    leave it unset.
    """

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
    dish_diameter_m: float | None = None


@dataclass(frozen=True)
class Observation:
    """A scheduled RA observation from an external source.

    Core fields are common to every source. `target` is populated only by
    sources that carry sky-pointing info (ODS); calendar events leave it None.
    """

    ext_id: str
    name: str
    start: datetime.datetime
    end: datetime.datetime
    min_freq_hz: int
    max_freq_hz: int

    description: str = ""
    target: ObsTarget | None = None


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
