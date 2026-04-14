"""RA source that fetches events from a Google Calendar.

Uses zmsclient.tools.grant.gcal.get_events as a library to do the actual
fetch, then converts events into our internal Observation type.

Event summaries from some facilities (e.g. HCRO Transmission) include
frequency info in a labeled multi-line format:

    Activity Title: HCRO Transmission
    Start Date: 04/14/2026
    Start Time: 10:00
    End Date: 04/14/2026
    End Time: 16:00
    Center Frequency: 915 (MHz)
    Bandwidth: 26 MHz

When present, we extract Center Frequency + Bandwidth and derive a
min/max range. Otherwise we fall back to configured defaults.
"""

from __future__ import annotations

import datetime
import logging
import re
from re import Pattern

from zmsclient.tools.grant.gcal import get_events

from .protocol import Observation

LOG = logging.getLogger(__name__)

_CENTER_FREQ_RE = re.compile(
    r"Center Frequency:\s*(\d+(?:\.\d+)?)\s*\(?\s*MHz\s*\)?", re.IGNORECASE
)
_BANDWIDTH_RE = re.compile(r"Bandwidth:\s*(\d+(?:\.\d+)?)\s*MHz", re.IGNORECASE)
_ACTIVITY_TITLE_RE = re.compile(r"Activity Title:\s*(.+)", re.IGNORECASE)


class GcalSource:
    """Fetches Google Calendar events and converts them to Observations."""

    def __init__(
        self,
        source_type: str,
        source_name: str,
        calendar_id: str,
        calendar_token: str,
        default_min_freq_hz: int,
        default_max_freq_hz: int,
        ext_id_prefix: str = "gcal-",
        filter_exc: list[Pattern] | None = None,
        filter_inc: list[Pattern] | None = None,
    ) -> None:
        self._source_type = source_type
        self._source_name = source_name
        self._calendar_id = calendar_id
        self._calendar_token = calendar_token
        self._default_min_freq_hz = default_min_freq_hz
        self._default_max_freq_hz = default_max_freq_hz
        self._ext_id_prefix = ext_id_prefix
        self._filter_exc = filter_exc or []
        self._filter_inc = filter_inc or []

    @property
    def source_type(self) -> str:
        return self._source_type

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def ext_id_prefix(self) -> str:
        return self._ext_id_prefix

    def fetch_observations(self) -> list[Observation]:
        """Fetch future events from gcal and return them as Observations."""
        now = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        try:
            events = get_events(
                self._calendar_id,
                self._calendar_token,
                now,
                None,
                self._filter_exc,
                self._filter_inc,
            )
        except Exception:
            LOG.exception("Failed to fetch gcal events")
            return []

        observations: list[Observation] = []
        for event in events:
            try:
                obs = _event_to_observation(
                    event,
                    self._ext_id_prefix,
                    self._default_min_freq_hz,
                    self._default_max_freq_hz,
                )
                if obs is not None:
                    observations.append(obs)
            except Exception:
                LOG.exception("Failed to convert gcal event %s", event.get("id", "?"))

        LOG.info("Fetched %d gcal observations", len(observations))
        return observations


def _event_to_observation(
    event: dict,
    ext_id_prefix: str,
    default_min_freq_hz: int,
    default_max_freq_hz: int,
) -> Observation | None:
    """Convert a gcal event dict into an Observation."""
    event_id = event.get("id")
    if not event_id:
        return None

    start = event.get("startDateTime")
    end = event.get("endDateTime")
    if start is None or end is None:
        return None

    # Handle date-only events (no time component)
    if not isinstance(start, datetime.datetime):
        start = datetime.datetime.combine(start, datetime.time.min, tzinfo=datetime.UTC)
    if not isinstance(end, datetime.datetime):
        end = datetime.datetime.combine(end, datetime.time.min, tzinfo=datetime.UTC)

    summary = event.get("summary") or ""

    # Prefer "Activity Title:" line for name; fall back to first summary line.
    name = summary.split("\n", 1)[0].strip() if summary else event_id
    m = _ACTIVITY_TITLE_RE.search(summary)
    if m:
        name = m.group(1).strip()

    min_freq_hz, max_freq_hz = _parse_freq_from_summary(
        summary, default_min_freq_hz, default_max_freq_hz
    )

    return Observation(
        ext_id=f"{ext_id_prefix}{event_id}",
        name=name,
        start=start,
        end=end,
        min_freq_hz=min_freq_hz,
        max_freq_hz=max_freq_hz,
        description=event.get("description") or summary,
    )


def _parse_freq_from_summary(
    summary: str,
    default_min_freq_hz: int,
    default_max_freq_hz: int,
) -> tuple[int, int]:
    """Extract center freq + bandwidth from the summary, or fall back to defaults.

    Matches patterns like:
      Center Frequency: 915 (MHz)
      Bandwidth: 26 MHz
    """
    cf = _CENTER_FREQ_RE.search(summary)
    bw = _BANDWIDTH_RE.search(summary)
    if cf and bw:
        cf_mhz = float(cf.group(1))
        bw_mhz = float(bw.group(1))
        min_mhz = cf_mhz - bw_mhz / 2
        max_mhz = cf_mhz + bw_mhz / 2
        return int(min_mhz * 1_000_000), int(max_mhz * 1_000_000)
    return default_min_freq_hz, default_max_freq_hz
