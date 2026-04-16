"""Reconciler for gcal source -> ZMC claims.

Unlike the ODS reconciler (which posts to zms-ra), this one creates
top-level claims in ZMC directly. ODS observations can then reference
these claims' grants via grant_id.

Follows the same stateless-diff pattern as the ODS reconciler:
  1. Fetch desired Observations from the source
  2. Fetch current claims from ZMC (scoped by ext_id prefix)
  3. Create new claims, delete cancelled ones (only if not started),
     recreate claims whose time/freq has drifted
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import cast

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import (
    Claim,
    ClaimList,
    Constraint,
    Grant,
    GrantConstraint,
    GrantOpStatus,
)

from .sources.gcal import GcalSource
from .sources.protocol import Observation
from .spectrum_picker import SpectrumPicker

LOG = logging.getLogger(__name__)


@dataclass
class ReconcileStats:
    created: int = 0
    deleted: int = 0
    unchanged: int = 0
    errors: int = 0


def reconcile_gcal(
    client: ZmsZmcClient,
    source: GcalSource,
    element_id: str,
    picker: SpectrumPicker,
    now: datetime.datetime | None = None,
) -> ReconcileStats:
    """Run one reconciliation cycle for a gcal source against ZMC.

    Each observation gets routed to the narrowest spectrum whose freq range
    covers it. Observations with no matching spectrum are skipped.
    """
    stats = ReconcileStats()
    now = now or datetime.datetime.now(datetime.UTC)

    picker.refresh()

    desired = {obs.ext_id: obs for obs in source.fetch_observations()}
    current = {
        c.ext_id: c
        for c in _list_claims(client, element_id, source.ext_id_prefix)
        if c.ext_id
    }

    # New observations -> create
    for ext_id in desired.keys() - current.keys():
        _try_create(client, desired[ext_id], element_id, picker, source, stats)

    # Vanished observations -> delete if not yet started
    for ext_id in current.keys() - desired.keys():
        claim = current[ext_id]
        if _claim_started(claim, now):
            stats.unchanged += 1
        else:
            _try_delete(client, claim, stats)

    # Existing observations -> check for drift
    for ext_id in desired.keys() & current.keys():
        obs = desired[ext_id]
        claim = current[ext_id]
        if _claim_matches(claim, obs):
            stats.unchanged += 1
            continue
        if _claim_started(claim, now):
            LOG.warning(
                "Observation %s changed but claim already started -- leaving as-is",
                ext_id,
            )
            stats.unchanged += 1
        else:
            _try_delete(client, claim, stats)
            _try_create(client, obs, element_id, picker, source, stats)

    return stats


def _list_claims(
    client: ZmsZmcClient,
    element_id: str,
    ext_id_prefix: str,
) -> list[Claim]:
    """Fetch all non-deleted claims for element_id whose ext_id has the prefix."""
    claims: list[Claim] = []
    page = 1
    while True:
        resp = client.list_claims(
            element_id=element_id,
            ext_id=ext_id_prefix,
            page=page,
            items_per_page=100,
            x_api_elaborate="True",
        )
        if not resp.is_success or not isinstance(resp.parsed, ClaimList):
            LOG.error("Failed to list claims (page %d): %s", page, resp.status_code)
            break
        claim_list = cast(ClaimList, resp.parsed)
        # ext_id filter is an ILIKE substring match; enforce prefix ourselves.
        for claim in claim_list.claims:
            if claim.ext_id and claim.ext_id.startswith(ext_id_prefix):
                claims.append(claim)
        if page >= claim_list.pages:
            break
        page += 1
    return claims


def _try_create(
    client: ZmsZmcClient,
    obs: Observation,
    element_id: str,
    picker: SpectrumPicker,
    source: GcalSource,
    stats: ReconcileStats,
) -> None:
    spectrum = picker.pick(obs.min_freq_hz, obs.max_freq_hz)
    if spectrum is None:
        LOG.warning(
            "No spectrum covers %s (%d-%d Hz); skipping",
            obs.ext_id,
            obs.min_freq_hz,
            obs.max_freq_hz,
        )
        return
    try:
        body = _build_claim(obs, element_id, str(spectrum.id), source)
        resp = client.create_claim(body=body, x_api_elaborate="true")
        if resp.is_success:
            LOG.info(
                "Created gcal claim for %s on spectrum %s",
                obs.ext_id,
                spectrum.name,
            )
            stats.created += 1
        else:
            LOG.error(
                "Failed to create gcal claim for %s: %s",
                obs.ext_id,
                resp.status_code,
            )
            stats.errors += 1
    except Exception:
        LOG.exception("Error creating gcal claim for %s", obs.ext_id)
        stats.errors += 1


def _try_delete(
    client: ZmsZmcClient,
    claim: Claim,
    stats: ReconcileStats,
) -> None:
    try:
        resp = client.delete_claim(claim_id=str(claim.id))
        if resp.is_success:
            LOG.info("Deleted gcal claim %s (ext_id=%s)", claim.id, claim.ext_id)
            stats.deleted += 1
        else:
            LOG.error("Failed to delete gcal claim %s: %s", claim.id, resp.status_code)
            stats.errors += 1
    except Exception:
        LOG.exception("Error deleting gcal claim %s", claim.id)
        stats.errors += 1


def _build_claim(
    obs: Observation,
    element_id: str,
    spectrum_id: str,
    source: GcalSource,
) -> Claim:
    grant = Grant(
        name=obs.name,
        description=obs.description,
        element_id=element_id,
        spectrum_id=spectrum_id,
        ext_id=obs.ext_id,
        priority=1023,
        starts_at=obs.start,
        expires_at=obs.end,
        constraints=[
            GrantConstraint(
                constraint=Constraint(
                    min_freq=obs.min_freq_hz,
                    max_freq=obs.max_freq_hz,
                    max_eirp=0.0,
                    exclusive=True,
                )
            )
        ],
        op_status=GrantOpStatus.SUBMITTED,
    )
    return Claim(
        name=obs.name,
        description=obs.description,
        type=source.source_type,
        source=source.source_name,
        element_id=element_id,
        ext_id=obs.ext_id,
        grant=grant,
    )


def _claim_started(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already started."""
    if not isinstance(claim.grant, Grant):
        return True  # be conservative -- don't delete what we don't understand
    grant: Grant = claim.grant
    start = grant.starts_at
    if not isinstance(start, datetime.datetime):
        return True
    if start.tzinfo is None:
        start = start.replace(tzinfo=datetime.UTC)
    return start <= now


def _claim_matches(claim: Claim, obs: Observation) -> bool:
    """True if the claim's grant still matches the observation's time/freq."""
    if not isinstance(claim.grant, Grant):
        return False
    grant: Grant = claim.grant
    if grant.starts_at != obs.start or grant.expires_at != obs.end:
        return False
    constraints = grant.constraints
    if not constraints or not isinstance(constraints, list):
        return False
    c = constraints[0].constraint
    if not isinstance(c, Constraint):
        return False
    return c.min_freq == obs.min_freq_hz and c.max_freq == obs.max_freq_hz
