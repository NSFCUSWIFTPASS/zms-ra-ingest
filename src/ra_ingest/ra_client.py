"""Thin client for the zms-ra service.

zms-ra has its own REST API for storing/retrieving RAObservation records.
We use httpx directly since there's no Python client for it yet.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import httpx

from .sources.protocol import Observation

LOG = logging.getLogger(__name__)


class ZmsRaClient:
    """Minimal client for posting and querying zms-ra RAObservation records."""

    def __init__(self, base_url: str, token: str, verify_ssl: bool = True) -> None:
        self._base = base_url.rstrip("/")
        self._client = httpx.Client(
            timeout=30.0,
            verify=verify_ssl,
            headers={"X-Api-Token": token},
        )

    def list_observations(
        self,
        page: int = 1,
        items_per_page: int = 100,
    ) -> list[dict[str, Any]]:
        """List all observations (paginated)."""
        observations: list[dict[str, Any]] = []
        while True:
            resp = self._client.get(
                f"{self._base}/v1/raobservations",
                params={"page": page, "items_per_page": items_per_page},
            )
            if resp.status_code != 200:
                LOG.error(
                    "Failed to list raobservations (page %d): %s %s",
                    page,
                    resp.status_code,
                    resp.text[:200],
                )
                break
            body = resp.json()
            observations.extend(body.get("ra_observations", []))
            if page >= body.get("pages", 1):
                break
            page += 1
        return observations

    def create_observation(self, body: dict[str, Any]) -> dict[str, Any] | None:
        resp = self._client.post(f"{self._base}/v1/raobservations", json=body)
        if resp.status_code in (200, 201):
            return resp.json()
        LOG.error(
            "Failed to create raobservation: %s %s", resp.status_code, resp.text[:300]
        )
        return None

    def delete_observation(self, observation_id: str) -> bool:
        resp = self._client.delete(f"{self._base}/v1/raobservations/{observation_id}")
        if resp.status_code in (200, 204):
            return True
        LOG.error(
            "Failed to delete raobservation %s: %s %s",
            observation_id,
            resp.status_code,
            resp.text[:200],
        )
        return False


def observation_to_ra_body(
    obs: Observation,
    grant_id: str,
) -> dict[str, Any]:
    """Convert an internal Observation into a zms-ra POST body."""
    body: dict[str, Any] = {
        "GrantId": grant_id,
        "TransactionId": obs.ext_id,
        "DateTimeStart": obs.start.isoformat(),
        "DateTimeStop": obs.end.isoformat(),
        "DpaName": obs.site_id or "unknown",
        "DpaId": obs.site_id or "unknown",
        "LocLat": obs.site_lat,
        "LocLong": obs.site_lon,
        "LocElevation": obs.site_elevation,
        "LocRadius": 0.0,
        "CoordType": "radec",
        "RegionX": obs.ra_j2000_deg / 15.0,  # degrees -> hours for RA
        "RegionY": obs.dec_j2000_deg,
        "RegionSize": 0.5,
        "FreqStart": float(obs.min_freq_hz),
        "FreqStop": float(obs.max_freq_hz),
        "SourceId": obs.source_id,
        "ObsType": "spectral",
        "EventId": 1,
        "NumberEvents": 1,
        "EventStatus": "projected",
        "Acquisition": obs.slew_sec,
        "CorrInt": obs.corr_int_sec,
        "Checksum": f"ra-ingest:{obs.ext_id}",
        "DateTimePublished": datetime.datetime.now(datetime.UTC).isoformat(),
        "DateTimeCreated": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    if obs.trk_rate_ra is not None:
        body["TrkRateRaDegPerSec"] = obs.trk_rate_ra
    if obs.trk_rate_dec is not None:
        body["TrkRateDecDegPerSec"] = obs.trk_rate_dec
    return body
