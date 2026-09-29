"""RA source that polls the ODS (Operational Data Sharing) JSON endpoint.

Example: https://ods.hcro.org/ods.json
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import httpx

from .protocol import Observation, ObsTarget, SourceFetchError

LOG = logging.getLogger(__name__)


class OdsSource:
    """Fetches scheduled observations from an ODS JSON endpoint.

    Expects the endpoint to return:
    {
      "ods_data": [
        {
          "site_id": "ATA",
          "src_id": "1436+636",
          "src_start_utc": "2026-03-28T20:23:39",
          "src_end_utc": "2026-03-28T20:33:39",
          "freq_lower_hz": 1990000000,
          "freq_upper_hz": 1995000000,
          "freq_actual_hz": [
            {"freq_lower_hz": 1000000000, "freq_upper_hz": 1672000000},
            ...
          ],
          ...
        }
      ]
    }
    """

    def __init__(
        self,
        source_type: str,
        source_name: str,
        url: str,
        priority: int = 1023,
    ) -> None:
        self._url = url
        self._source_type = source_type
        self._source_name = source_name
        self._priority = priority
        # Fold source_name into the prefix so multiple ODS facilities stay
        # isolated -- otherwise one facility's reconcile would treat another's
        # claims as vanished and delete them.
        self._ext_id_prefix = f"ods-{source_name}-"
        self._client = httpx.Client(timeout=30.0)

    @property
    def source_type(self) -> str:
        return self._source_type

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def ext_id_prefix(self) -> str:
        return self._ext_id_prefix

    @property
    def protect_started(self) -> bool:
        return True  # ODS feed flaps; a live observation must not be torn down.

    @property
    def writes_observations(self) -> bool:
        return True  # ODS carries sky-pointing metadata -> RAObservation.

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def correlate_repushes(self) -> bool:
        return True  # ODS has no record id; a re-push slides the start time.

    def fetch_observations(self) -> list[Observation]:
        try:
            resp = self._client.get(self._url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            LOG.exception("Failed to fetch from %s", self._url)
            raise SourceFetchError(f"ODS fetch failed: {e}") from e

        raw = resp.json()
        ods_data = raw.get("ods_data", [])
        observations: list[Observation] = []
        for item in ods_data:
            try:
                observations.extend(_parse_ods_entry(item, self._ext_id_prefix))
            except Exception:
                LOG.exception("Failed to parse ODS entry: %r", item)

        LOG.info("Fetched %d observations from %s", len(observations), self._url)
        return observations


def _parse_ods_entry(item: dict[str, Any], ext_id_prefix: str) -> list[Observation]:
    """Parse one ODS entry into one Observation per observed band.

    Each entry in freq_actual_hz becomes its own Observation, so the gaps
    between tunings stay unclaimed. Without it, a single Observation on the
    freq_lower/upper_hz pair.
    """
    start = datetime.datetime.fromisoformat(item["src_start_utc"]).replace(
        tzinfo=datetime.UTC
    )
    end = datetime.datetime.fromisoformat(item["src_end_utc"]).replace(
        tzinfo=datetime.UTC
    )

    site_id = item.get("site_id", "")
    src_id = item.get("src_id", "")
    subarray = int(item.get("subarray", 0))

    # ODS has no record id; compose a stable one from the identifying fields.
    ext_id = f"{ext_id_prefix}{site_id}:{src_id}:{item['src_start_utc']}:{subarray}"

    target = ObsTarget(
        site_id=site_id,
        site_lat=float(item.get("site_lat_deg", 0) or 0),
        site_lon=float(item.get("site_lon_deg", 0) or 0),
        site_elevation=float(item.get("site_el_m", 0) or 0),
        source_id=src_id,
        ra_j2000_deg=float(item.get("src_ra_j2000_deg", 0) or 0),
        dec_j2000_deg=float(item.get("src_dec_j2000_deg", 0) or 0),
        slew_sec=float(item.get("slew_sec", 1) or 1),
        corr_int_sec=float(item.get("corr_integ_time_sec", 1) or 1),
        trk_rate_ra=item.get("trk_rate_ra_deg_per_sec"),
        trk_rate_dec=item.get("trk_rate_dec_deg_per_sec"),
        subarray=subarray,
        dish_diameter_m=(
            float(item["dish_diameter_m"])
            if item.get("dish_diameter_m") is not None
            else None
        ),
    )

    def _observation(min_freq_hz: int, max_freq_hz: int, band_tag: str) -> Observation:
        return Observation(
            ext_id=ext_id + band_tag,
            name=f"{src_id} ({site_id})" + band_tag,
            start=start,
            end=end,
            min_freq_hz=min_freq_hz,
            max_freq_hz=max_freq_hz,
            description=f"site={site_id} src={src_id} subarray={subarray}",
            target=target,
        )

    bands = item.get("freq_actual_hz")
    if bands:
        return [
            _observation(int(b["freq_lower_hz"]), int(b["freq_upper_hz"]), f":b{i}")
            for i, b in enumerate(bands)
        ]
    # Fall back to the avoidance band, untagged so existing claims keep their ext_ids.
    return [_observation(int(item["freq_lower_hz"]), int(item["freq_upper_hz"]), "")]
