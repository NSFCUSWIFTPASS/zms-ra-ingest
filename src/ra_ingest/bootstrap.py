"""Ensures required ZMS resources (spectrum) exist on startup."""

from __future__ import annotations

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

from .config import Settings

LOG = logging.getLogger(__name__)

# Used as ext_id to find our spectrum across restarts.
SPECTRUM_EXT_ID_PREFIX = "ra-ingest"


def ensure_spectrum(client: ZmsZmcClient, settings: Settings) -> str:
    """Find or create the spectrum for this ingest service. Returns spectrum_id."""
    ext_id = f"{SPECTRUM_EXT_ID_PREFIX}:{settings.spectrum_name}"

    # Check if it already exists
    resp = client.list_spectrum(
        element_id=settings.element_id,
        items_per_page=100,
        x_api_elaborate="True",
    )
    if resp.is_success and isinstance(resp.parsed, SpectrumList):
        for s in resp.parsed.spectrum:
            if s.ext_id == ext_id and not s.deleted_at:
                LOG.info("Found existing spectrum %s (%s)", s.id, s.name)
                return str(s.id)

    # Create it
    LOG.info(
        "Creating spectrum '%s' (%d-%d Hz)",
        settings.spectrum_name,
        settings.spectrum_min_freq_hz,
        settings.spectrum_max_freq_hz,
    )
    spectrum = Spectrum(
        element_id=settings.element_id,
        name=settings.spectrum_name,
        ext_id=ext_id,
        url="https://ods.hcro.org",
        description=f"Auto-created by zms-ra-ingest for {settings.spectrum_name}",
        enabled=True,
        constraints=[
            SpectrumConstraint(
                constraint=Constraint(
                    min_freq=settings.spectrum_min_freq_hz,
                    max_freq=settings.spectrum_max_freq_hz,
                    max_eirp=0,
                    exclusive=True,
                )
            )
        ],
        policies=[
            Policy(
                element_id=settings.element_id,
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

    resp = client.create_spectrum(body=spectrum)
    if resp.is_success:
        created = cast(Spectrum, resp.parsed)
        LOG.info("Created spectrum %s", created.id)
        return str(created.id)

    LOG.error("Failed to create spectrum: %s", resp.status_code)
    raise RuntimeError(f"Could not create spectrum: {resp.status_code}")
