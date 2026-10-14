"""Hold an exclusive claim on guarded spectrums while the ATA is in use.

Each poll, given whether transmitting is allowed (ata_status):
  * In use: make sure every guarded spectrum has a live busy claim covering
    its whole range. Create one if missing; extend it by replacing its grant
    once less than half of HOLD remains.
  * Not in use: delete the busy claims so transmitters are released at once.

Claims always end within HOLD, so if this service stops, protection lapses on
its own instead of blocking transmitters forever. ZMS is the only state.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import Claim, Grant, Spectrum

from .claims import (
    build_claim,
    build_grant,
    claim_ended,
    create_claim,
    delete_claim,
    list_claims,
    replace_grant,
)
from .spectrum_picker import SpectrumPicker

LOG = logging.getLogger(__name__)

EXT_ID_PREFIX = "ata-busy-"
ALL_SPECTRUMS = "*"


@dataclass
class GuardStats:
    created: int = 0
    extended: int = 0
    released: int = 0
    errors: int = 0


def guard(
    zmc_client: ZmsZmcClient,
    element_id: str,
    picker: SpectrumPicker,
    spectrum_names: list[str],
    allowed: bool,
    hold: datetime.timedelta,
    priority: int,
    now: datetime.datetime | None = None,
) -> GuardStats:
    """Run one guard cycle. spectrum_names may be ["*"] for every spectrum."""
    stats = GuardStats()
    now = now or datetime.datetime.now(datetime.UTC)

    picker.refresh()
    guarded = _select_spectrums(picker.spectrums, spectrum_names)
    guarded_ids = {str(s.id) for s in guarded}

    claims = list_claims(zmc_client, element_id, EXT_ID_PREFIX)

    # Clear out claims that no longer protect anything: ended, denied, or on a
    # spectrum that is no longer guarded.
    live: dict[str, Claim] = {}
    for claim in claims:
        spectrum_id = str(claim.grant.spectrum_id)
        if (
            claim_ended(claim, now)
            or claim.denied_at
            or spectrum_id not in guarded_ids
            or spectrum_id in live
        ):
            _release(zmc_client, claim, stats)
        else:
            live[spectrum_id] = claim

    for spectrum in guarded:
        claim = live.get(str(spectrum.id))
        try:
            if allowed:
                if claim:
                    _release(zmc_client, claim, stats)
            elif claim is None:
                _create(
                    zmc_client, spectrum, element_id, now, now + hold, priority, stats
                )
            elif _ends_within(claim, now, hold / 2):
                _extend(
                    zmc_client, claim, spectrum, element_id, now + hold, priority, stats
                )
        except Exception:
            LOG.exception("Error guarding spectrum %s", spectrum.name)
            stats.errors += 1

    return stats


def _select_spectrums(spectrums: list[Spectrum], names: list[str]) -> list[Spectrum]:
    """The spectrums to guard: all of them for ["*"], else those named."""
    if ALL_SPECTRUMS in names:
        return list(spectrums)
    by_name = {s.name: s for s in spectrums}
    for name in names:
        if name not in by_name:
            LOG.error("Guarded spectrum %r not found on the element", name)
    return [by_name[n] for n in names if n in by_name]


def _ends_within(
    claim: Claim, now: datetime.datetime, window: datetime.timedelta
) -> bool:
    end = claim.grant.expires_at
    if end.tzinfo is None:
        end = end.replace(tzinfo=datetime.UTC)
    return end - now < window


def _create(
    zmc_client: ZmsZmcClient,
    spectrum: Spectrum,
    element_id: str,
    start: datetime.datetime,
    end: datetime.datetime,
    priority: int,
    stats: GuardStats,
) -> None:
    grant = _busy_grant(spectrum, element_id, start, end, priority)
    created = create_claim(zmc_client, build_claim(grant, "ata-status", "ata"))
    if created is None:
        stats.errors += 1
        return
    if created.denied_at:
        # Released and retried next cycle; denial means an equal-or-higher
        # priority exclusive grant already holds this spectrum.
        LOG.error("Busy claim on %s was denied", spectrum.name)
        stats.errors += 1
        return
    LOG.info("ATA in use: created busy claim on %s until %s", spectrum.name, end)
    stats.created += 1


def _extend(
    zmc_client: ZmsZmcClient,
    claim: Claim,
    spectrum: Spectrum,
    element_id: str,
    end: datetime.datetime,
    priority: int,
    stats: GuardStats,
) -> None:
    grant = _busy_grant(spectrum, element_id, claim.grant.starts_at, end, priority)
    replacement = replace_grant(zmc_client, claim, grant)
    if replacement is None:
        stats.errors += 1
        return
    LOG.info(
        "ATA still in use: extended busy claim on %s until %s (grant %s)",
        spectrum.name,
        end,
        replacement.id,
    )
    stats.extended += 1


def _release(zmc_client: ZmsZmcClient, claim: Claim, stats: GuardStats) -> None:
    if delete_claim(zmc_client, claim):
        stats.released += 1
    else:
        stats.errors += 1


def _busy_grant(
    spectrum: Spectrum,
    element_id: str,
    start: datetime.datetime,
    end: datetime.datetime,
    priority: int,
) -> Grant:
    """An exclusive grant covering every range of the spectrum."""
    return build_grant(
        element_id=element_id,
        spectrum_id=str(spectrum.id),
        ext_id=f"{EXT_ID_PREFIX}{spectrum.id}",
        name=f"ATA in use ({spectrum.name})",
        description="Held while the ATA is observing, from its antenna status.",
        priority=priority,
        start=start,
        end=end,
        ranges=[
            (sc.constraint.min_freq, sc.constraint.max_freq)
            for sc in spectrum.constraints
        ],
    )
