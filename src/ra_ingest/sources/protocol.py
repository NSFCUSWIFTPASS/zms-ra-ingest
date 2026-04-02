from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Observation:
    """A scheduled RA observation from an external source.

    This is the common representation that all sources produce.
    The reconciler turns these into ZMS Claims.
    """

    ext_id: str
    name: str
    start: datetime.datetime
    end: datetime.datetime
    min_freq_hz: int
    max_freq_hz: int
    # Optional - sources may provide these
    description: str = ""
    max_eirp: float = 0.0


class RASource(Protocol):
    """Interface that each RA data source implements."""

    @property
    def source_type(self) -> str:
        """The claim 'type' field, e.g. 'ra-calendar'."""
        ...

    @property
    def source_name(self) -> str:
        """The claim 'source' field, e.g. 'hcro'."""
        ...

    @property
    def priority(self) -> int:
        """Grant priority for claims from this source. Range: -1023 to 1023."""
        ...

    def fetch_observations(self) -> list[Observation]:
        """Fetch current observations from this source.

        Returns all observations that should currently have claims in ZMS.
        Past/expired observations should not be returned.
        """
        ...
