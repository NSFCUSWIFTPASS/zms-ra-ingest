"""Manages spectrum resources in ZMS, one per radio band.

On startup, loads existing spectra created by this service. When the
reconciler needs a spectrum for a given frequency range, it calls
get_spectrum_id() which returns an existing one or creates it.
"""

from __future__ import annotations

import datetime
import logging
from typing import cast

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import (
    Constraint,
    Policy,
    Spectrum,
    SpectrumConstraint,
    SpectrumList,
)

from .bands import Band, find_band_for_range

LOG = logging.getLogger(__name__)

EXT_ID_PREFIX = "ra-ingest"


class SpectrumManager:
    """Lazily creates and caches spectrum resources, one per band."""

    def __init__(self, client: ZmsZmcClient, element_id: str) -> None:
        self._client = client
        self._element_id = element_id
        self._cache: dict[str, str] = {}  # band name -> spectrum_id
        self._load_existing()

    def _load_existing(self) -> None:
        """Load spectra previously created by this service."""
        resp = self._client.list_spectrum(
            element_id=self._element_id,
            items_per_page=100,
            x_api_elaborate="True",
        )
        if not resp.is_success or not isinstance(resp.parsed, SpectrumList):
            LOG.warning("Could not list existing spectra")
            return
        for s in resp.parsed.spectrum:
            if s.ext_id and s.ext_id.startswith(EXT_ID_PREFIX) and not s.deleted_at:
                band_name = s.ext_id.removeprefix(f"{EXT_ID_PREFIX}:")
                self._cache[band_name] = str(s.id)
                LOG.info("Found existing spectrum %s (%s)", s.id, band_name)

    def get_spectrum_id(self, min_freq_hz: int, max_freq_hz: int) -> str | None:
        """Get or create a spectrum for the band containing this freq range."""
        band = find_band_for_range(min_freq_hz, max_freq_hz)
        if band is None:
            LOG.error(
                "No band covers %d-%d Hz, cannot create claim", min_freq_hz, max_freq_hz
            )
            return None

        if band.name in self._cache:
            return self._cache[band.name]

        return self._create_spectrum(band)

    def _create_spectrum(self, band: Band) -> str | None:
        """Create a spectrum resource for a band."""
        ext_id = f"{EXT_ID_PREFIX}:{band.name}"
        LOG.info(
            "Creating spectrum '%s' (%d-%d Hz)",
            band.name,
            band.min_freq_hz,
            band.max_freq_hz,
        )

        spectrum = Spectrum(
            element_id=self._element_id,
            name=band.name,
            starts_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            ext_id=ext_id,
            url="https://ods.hcro.org",
            description=f"Auto-created by zms-ra-ingest for {band.name}",
            enabled=True,
            constraints=[
                SpectrumConstraint(
                    constraint=Constraint(
                        min_freq=band.min_freq_hz,
                        max_freq=band.max_freq_hz,
                        max_eirp=0,
                        exclusive=True,
                    )
                )
            ],
            policies=[
                Policy(
                    element_id=self._element_id,
                    allowed=True,
                    auto_approve=True,
                    priority=1023,
                    allow_skip_acks=True,
                    allow_inactive=False,
                    allow_conflicts=False,
                    when_unoccupied=False,
                    disable_emit_check=True,
                )
            ],
        )

        resp = self._client.create_spectrum(body=spectrum)
        if resp.is_success:
            created = cast(Spectrum, resp.parsed)
            self._cache[band.name] = str(created.id)
            LOG.info("Created spectrum %s for %s", created.id, band.name)
            return str(created.id)

        LOG.error("Failed to create spectrum for %s: %s", band.name, resp.status_code)
        return None
