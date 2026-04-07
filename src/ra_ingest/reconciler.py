"""Stateless reconciler: diffs RA source observations against ZMS claims.

Each cycle fetches observations from a source, fetches current claims from
ZMS, then creates/deletes to converge. Claims that have already started are
never deleted -- they represent real spectrum usage.
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
)

from .sources.protocol import Observation, RASource

LOG = logging.getLogger(__name__)


@dataclass
class ReconcileStats:
    created: int = 0
    deleted: int = 0
    unchanged: int = 0
    errors: int = 0


def reconcile(
    client: ZmsZmcClient,
    source: RASource,
    element_id: str,
    spectrum_id: str,
    now: datetime.datetime | None = None,
) -> ReconcileStats:
    """Run one reconciliation cycle for a single source."""
    stats = ReconcileStats()
    now = now or datetime.datetime.now(datetime.UTC)

    desired = {obs.ext_id: obs for obs in source.fetch_observations()}
    current = {
        c.ext_id: c for c in _list_all_claims(client, element_id, source.source_type)
    }

    for ext_id in desired.keys() - current.keys():
        _try_create(client, desired[ext_id], source, element_id, spectrum_id, stats)

    for ext_id in current.keys() - desired.keys():
        claim = current[ext_id]
        if _started(claim, now):
            stats.unchanged += 1
        else:
            _try_delete(client, claim, stats)

    for ext_id in desired.keys() & current.keys():
        obs, claim = desired[ext_id], current[ext_id]
        if not _claim_matches(claim, obs):
            if _started(claim, now):
                LOG.warning("Observation %s changed but claim already started", ext_id)
                stats.unchanged += 1
            else:
                _try_delete(client, claim, stats)
                _try_create(client, obs, source, element_id, spectrum_id, stats)
        else:
            stats.unchanged += 1

    return stats


def _try_create(
    client: ZmsZmcClient,
    obs: Observation,
    source: RASource,
    element_id: str,
    spectrum_id: str,
    stats: ReconcileStats,
) -> None:
    try:
        claim = _build_claim(obs, source, element_id, spectrum_id)
        resp = client.create_claim(body=claim)
        if resp.is_success:
            created = cast(Claim, resp.parsed)
            LOG.info("Created claim %s (ext_id=%s)", created.id, obs.ext_id)
            stats.created += 1
        else:
            LOG.error(
                "Failed to create claim for %s: %s",
                obs.ext_id,
                resp.status_code,
            )
            stats.errors += 1
    except Exception:
        LOG.exception("Error creating claim for %s", obs.ext_id)
        stats.errors += 1


def _try_delete(client: ZmsZmcClient, claim: Claim, stats: ReconcileStats) -> None:
    try:
        resp = client.delete_claim(claim_id=str(claim.id))
        if resp.is_success:
            LOG.info("Deleted claim %s (ext_id=%s)", claim.id, claim.ext_id)
            stats.deleted += 1
        else:
            LOG.error("Failed to delete claim %s: %s", claim.id, resp.status_code)
            stats.errors += 1
    except Exception:
        LOG.exception("Error deleting claim %s", claim.id)
        stats.errors += 1


def _started(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already started (or has no grant/time data)."""
    try:
        return claim.grant.starts_at <= now
    except (AttributeError, TypeError):
        LOG.warning("Claim %s has no grant or starts_at, treating as started", claim.id)
        return True


def _claim_matches(claim: Claim, obs: Observation) -> bool:
    """True if the claim's grant still matches the observation."""
    grant = claim.grant
    if grant is None:
        return False
    if grant.starts_at != obs.start or grant.expires_at != obs.end:
        return False
    try:
        c = grant.constraints[0].constraint
        return c.min_freq == obs.min_freq_hz and c.max_freq == obs.max_freq_hz
    except (IndexError, TypeError, AttributeError):
        return False


def _list_all_claims(
    client: ZmsZmcClient, element_id: str, source_type: str
) -> list[Claim]:
    """Fetch all claims for this element + source type from ZMS."""
    claims: list[Claim] = []
    page = 1
    while True:
        resp = client.list_claims(
            element_id=element_id,
            ext=source_type,
            page=page,
            items_per_page=100,
            x_api_elaborate="True",
        )
        if not resp.is_success or not isinstance(resp.parsed, ClaimList):
            LOG.error("Failed to list claims (page %d): %s", page, resp.status_code)
            break
        result = cast(ClaimList, resp.parsed)
        claims.extend(result.claims)
        if page >= result.pages:
            break
        page += 1
    return claims


def _build_claim(
    obs: Observation,
    source: RASource,
    element_id: str,
    spectrum_id: str,
) -> Claim:
    """Convert an Observation into a ZMS Claim with inline Grant."""
    return Claim(
        element_id=element_id,
        ext_id=obs.ext_id,
        type=source.source_type,
        source=source.source_name,
        name=obs.name,
        description=obs.description,
        grant=Grant(
            element_id=element_id,
            spectrum_id=spectrum_id,
            name=obs.name,
            description=obs.description,
            starts_at=obs.start,
            expires_at=obs.end,
            priority=source.priority,
            constraints=[
                GrantConstraint(
                    constraint=Constraint(
                        min_freq=obs.min_freq_hz,
                        max_freq=obs.max_freq_hz,
                        max_eirp=obs.max_eirp,
                        exclusive=True,
                    )
                )
            ],
        ),
    )
