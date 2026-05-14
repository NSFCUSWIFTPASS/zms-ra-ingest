"""Thin client for the zms-ra service.

zms-ra has its own REST API for storing/retrieving RAObservation records.
We use httpx directly since there's no Python client for it yet.

ra-ingest posts ODS-shaped JSON to /v1/ods/observations; zms-ra translates
that into a canonical RAObservation row internally.
"""

from __future__ import annotations

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
            observations.extend(body.get("ra_observations") or [])
            if page >= body.get("pages", 1):
                break
            page += 1
        return observations

    def create_observation(self, body: dict[str, Any]) -> dict[str, Any] | None:
        resp = self._client.post(f"{self._base}/v1/ods/observations", json=body)
        if resp.status_code in (200, 201):
            return resp.json()
        LOG.error(
            "Failed to create observation: %s %s", resp.status_code, resp.text[:300]
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
    """Convert an internal Observation into an ODS-shaped POST body for zms-ra.

    Field names match the ODS schema; degrees throughout, no fabricated
    TardyS4 fields.
    """
    body: dict[str, Any] = {
        "GrantId": grant_id,
        "TransactionId": obs.ext_id,
        "site_id": obs.site_id,
        "site_lat_deg": obs.site_lat,
        "site_lon_deg": obs.site_lon,
        "site_el_m": obs.site_elevation,
        "src_id": obs.source_id,
        "src_start_utc": obs.start.isoformat(),
        "src_end_utc": obs.end.isoformat(),
        "src_ra_j2000_deg": obs.ra_j2000_deg,
        "src_dec_j2000_deg": obs.dec_j2000_deg,
        "src_radius": 0.5,
        "freq_lower_hz": float(obs.min_freq_hz),
        "freq_upper_hz": float(obs.max_freq_hz),
        "slew_sec": obs.slew_sec,
        "corr_integ_time_sec": obs.corr_int_sec,
        "obs_type": "spectral",
        "subarray": obs.subarray,
    }
    if obs.trk_rate_ra is not None:
        body["trk_rate_ra_deg_per_sec"] = obs.trk_rate_ra
    if obs.trk_rate_dec is not None:
        body["trk_rate_dec_deg_per_sec"] = obs.trk_rate_dec
    if obs.dish_diameter_m is not None:
        body["dish_diameter_m"] = obs.dish_diameter_m
    return body
