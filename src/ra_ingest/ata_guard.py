"""Hold an exclusive claim on guarded spectrums while the ATA is in use.

Each poll, given whether transmitting is allowed (ata_status):
  * In use: make sure every guarded spectrum has a live busy claim covering
    its whole range. Create one if missing; extend it by replacing its grant
    once less than half of HOLD remains.
  * Not in use: end the busy claims early (replace with an end release_margin
    away) so transmitters are released.

Ended claims are kept, so each busy period stays in ZMS history with its real
start and end. ZMC refuses a claim whose ext_id is taken, so each busy period's
ext_id includes its start. Denied claims protected nothing and are deleted.

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

# Busy claims older than this are history; must exceed the longest busy period.
LOOKBACK = datetime.timedelta(days=7)


@dataclass
class GuardStats:
    created: int = 0
    extended: int = 0
    released: int = 0
    removed: int = 0
    errors: int = 0


def guard(
    zmc_client: ZmsZmcClient,
    element_id: str,
    picker: SpectrumPicker,
    spectrum_names: list[str],
    allowed: bool,
    hold: datetime.timedelta,
    priority: int,
    release_margin: datetime.timedelta,
    now: datetime.datetime | None = None,
) -> GuardStats:
    """Run one guard cycle. spectrum_names may be ["*"] for every spectrum.
    release_margin is how soon an ended-early claim actually ends."""
    stats = GuardStats()
    now = now or datetime.datetime.now(datetime.UTC)

    picker.refresh()
    guarded = _select_spectrums(picker.spectrums, spectrum_names)
    guarded_ids = {str(s.id) for s in guarded}

    claims = list_claims(zmc_client, element_id, EXT_ID_PREFIX, since=now - LOOKBACK)

    # Keep one live claim per guarded spectrum. Ended claims stay as history;
    # denied ones are deleted; extras and unguarded ones are ended early.
    live: dict[str, Claim] = {}
    for claim in claims:
        spectrum_id = str(claim.grant.spectrum_id)
        try:
            if claim.denied_at:
                _remove(zmc_client, claim, stats)
            elif claim_ended(claim, now):
                continue
            elif spectrum_id not in guarded_ids or spectrum_id in live:
                _release(zmc_client, claim, now, release_margin, stats)
            else:
                live[spectrum_id] = claim
        except Exception:
            LOG.exception("Error tidying busy claim %s", claim.ext_id)
            stats.errors += 1

    for spectrum in guarded:
        claim = live.get(str(spectrum.id))
        try:
            if allowed:
                if claim:
                    _release(zmc_client, claim, now, release_margin, stats)
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
    ext_id = _busy_ext_id(spectrum, start)
    grant = _busy_grant(spectrum, element_id, ext_id, start, end, priority)
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
    grant = _busy_grant(
        spectrum, element_id, claim.ext_id, claim.grant.starts_at, end, priority
    )
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


def _release(
    zmc_client: ZmsZmcClient,
    claim: Claim,
    now: datetime.datetime,
    margin: datetime.timedelta,
    stats: GuardStats,
) -> None:
    """End the claim margin from now, unless it already ends sooner."""
    if _ends_within(claim, now, margin):
        return
    old = claim.grant
    end = now + margin
    grant = build_grant(
        element_id=old.element_id,
        spectrum_id=str(old.spectrum_id),
        ext_id=claim.ext_id,
        name=old.name,
        description=old.description,
        priority=old.priority,
        start=old.starts_at,
        end=end,
        ranges=[
            (gc.constraint.min_freq, gc.constraint.max_freq) for gc in old.constraints
        ],
    )
    if replace_grant(zmc_client, claim, grant) is None:
        stats.errors += 1
        return
    LOG.info("ATA not in use: ending busy claim %s at %s", claim.ext_id, end)
    stats.released += 1


def _remove(zmc_client: ZmsZmcClient, claim: Claim, stats: GuardStats) -> None:
    """Delete a denied claim; it never protected anything."""
    if delete_claim(zmc_client, claim):
        stats.removed += 1
    else:
        stats.errors += 1


def _busy_ext_id(spectrum: Spectrum, start: datetime.datetime) -> str:
    """One ext_id per busy period, e.g. ata-busy-<spectrum id>-20261016T050000Z."""
    return f"{EXT_ID_PREFIX}{spectrum.id}-{start:%Y%m%dT%H%M%SZ}"


def _busy_grant(
    spectrum: Spectrum,
    element_id: str,
    ext_id: str,
    start: datetime.datetime,
    end: datetime.datetime,
    priority: int,
) -> Grant:
    """An exclusive grant covering every range of the spectrum."""
    return build_grant(
        element_id=element_id,
        spectrum_id=str(spectrum.id),
        ext_id=ext_id,
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
