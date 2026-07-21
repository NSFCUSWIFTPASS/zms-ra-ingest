from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Protocol


class SourceFetchError(Exception):
    """Raised by a RASource when it could not retrieve the desired state.

    Distinct from an empty result. A reconciler that sees this MUST NOT
    treat the desired state as empty -- doing so would soft-delete every
    future record in ZMS during a source outage.
    """


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
    """Interface that each RA data source implements.

    Beyond fetching observations, a source declares three reconcile policies:
    how its grants are scoped (`ext_id_prefix`), when a live grant is protected
    from teardown (`protect_started`), and whether it records an RAObservation
    in zms-ra (`writes_observations`).
    """

    @property
    def source_type(self) -> str:
        """The source type identifier, e.g. 'ra-ods'."""
        ...

    @property
    def source_name(self) -> str:
        """The facility identifier, e.g. 'hcro'."""
        ...

    @property
    def ext_id_prefix(self) -> str:
        """Prefix scoping this source's claims/RAObservation rows, e.g. 'gcal-'.

        Must be unique per source: the reconciler treats any claim under this
        prefix that the source no longer lists as vanished, so two sources
        sharing a prefix would delete each other's records.
        """
        ...

    @property
    def protect_started(self) -> bool:
        """Teardown guard. True: a claim is protected once it has STARTED
        (the source flaps; absence is not a cancel -- ODS). False: protected
        only once it has ENDED (the source is authoritative; active edits take
        effect -- gcal)."""
        ...

    @property
    def writes_observations(self) -> bool:
        """True if this source also records an RAObservation in zms-ra
        (sources with sky-pointing metadata -- ODS). False mints a grant only."""
        ...

    def fetch_observations(self) -> list[Observation]:
        """Fetch current observations from this source.

        Returns all observations that should currently exist in zms-ra.
        Past/expired observations should not be returned.
        """
        ...
