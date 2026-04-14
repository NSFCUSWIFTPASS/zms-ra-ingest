"""RA source that polls the ODS (Operational Data Sharing) JSON endpoint.

Example: https://ods.hcro.org/ods.json
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import httpx

from .protocol import Observation

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
    ) -> None:
        self._url = url
        self._source_type = source_type
        self._source_name = source_name
        self._client = httpx.Client(timeout=30.0)

    @property
    def source_type(self) -> str:
        return self._source_type

    @property
    def source_name(self) -> str:
        return self._source_name

    def fetch_observations(self) -> list[Observation]:
        try:
            resp = self._client.get(self._url)
            resp.raise_for_status()
        except httpx.HTTPError:
            LOG.exception("Failed to fetch from %s", self._url)
            return []

        raw = resp.json()
        ods_data = raw.get("ods_data", [])
        observations: list[Observation] = []
        for item in ods_data:
            try:
                obs = _parse_ods_entry(item)
                observations.append(obs)
            except Exception:
                LOG.exception("Failed to parse ODS entry: %r", item)

        LOG.info("Fetched %d observations from %s", len(observations), self._url)
        return observations


def _parse_ods_entry(item: dict[str, Any]) -> Observation:
    """Parse a single ODS observation entry."""
    start = datetime.datetime.fromisoformat(item["src_start_utc"]).replace(
        tzinfo=datetime.UTC
    )
    end = datetime.datetime.fromisoformat(item["src_end_utc"]).replace(
        tzinfo=datetime.UTC
    )

    site_id = item.get("site_id", "")
    src_id = item.get("src_id", "")
    subarray = int(item.get("subarray", 0))

    # Build a stable ext_id from the fields that uniquely identify this observation.
    # ODS doesn't have an explicit ID, so we compose one.
    ext_id = f"{site_id}:{src_id}:{item['src_start_utc']}:{subarray}"

    return Observation(
        ext_id=ext_id,
        name=f"{src_id} ({site_id})",
        start=start,
        end=end,
        min_freq_hz=int(item["freq_lower_hz"]),
        max_freq_hz=int(item["freq_upper_hz"]),
        description=f"site={site_id} src={src_id} subarray={subarray}",
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
    )
