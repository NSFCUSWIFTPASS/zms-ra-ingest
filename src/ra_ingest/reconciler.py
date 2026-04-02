"""Stateless reconciler: diffs RA source observations against ZMS claims.

Each cycle:
  1. Fetch current observations from all RA sources
  2. Fetch current claims from ZMS (scoped by element_id + source type)
  3. Create claims for new observations
  4. Delete claims only if they are future (not yet started) and no longer in source
  5. Recreate claims whose time/freq changed (only if not yet started)

Claims for observations that have started or completed are never deleted --
they represent real spectrum usage that happened or is happening.
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
    Spectrum,
)

from .sources.protocol import Observation, RASource

LOG = logging.getLogger(__name__)


@dataclass
class ReconcileStats:
    created: int = 0
    deleted: int = 0
    unchanged: int = 0
    errors: int = 0


def _get_spectrum_freq_range(
    client: ZmsZmcClient,
    spectrum_id: str,
) -> tuple[int, int]:
    """Fetch the spectrum's frequency range for calendar events with no freq info."""
    resp = client.get_spectrum(spectrum_id, x_api_elaborate="True")
    if not resp.is_success or not isinstance(resp.parsed, Spectrum):
        LOG.error("Failed to fetch spectrum %s", spectrum_id)
        return (0, 0)
    spectrum = resp.parsed
    if spectrum.constraints and len(spectrum.constraints) > 0:
        c = spectrum.constraints[0].constraint
        if c:
            return (c.min_freq, c.max_freq)
    return (0, 0)


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

    # 0. Fetch spectrum freq range as fallback for sources with no freq info
    spectrum_freq = _get_spectrum_freq_range(client, spectrum_id)

    # 1. Fetch desired state from RA source
    desired = source.fetch_observations()
    desired_by_ext_id = {obs.ext_id: obs for obs in desired}

    # 2. Fetch current state from ZMS
    current_claims = _list_all_claims(client, element_id, source.source_type)
    current_by_ext_id: dict[str, Claim] = {}
    for claim in current_claims:
        current_by_ext_id[claim.ext_id] = claim

    # 3. Diff
    desired_ids = set(desired_by_ext_id.keys())
    current_ids = set(current_by_ext_id.keys())

    to_create = desired_ids - current_ids
    to_delete: set[str] = set()
    to_check = desired_ids & current_ids

    # Claims not in source anymore -- only delete if they haven't started yet
    missing_ids = current_ids - desired_ids
    for ext_id in missing_ids:
        claim = current_by_ext_id[ext_id]
        if _is_future_claim(claim, now):
            to_delete.add(ext_id)
        else:
            LOG.debug(
                "Keeping past/active claim %s (ext_id=%s) -- already started or completed",
                claim.id,
                ext_id,
            )
            stats.unchanged += 1

    # 4. Check existing claims for changes (time/freq drift)
    for ext_id in to_check:
        obs = desired_by_ext_id[ext_id]
        claim = current_by_ext_id[ext_id]
        if _claim_needs_update(claim, obs):
            if _is_future_claim(claim, now):
                LOG.info("Observation %s changed, will recreate claim", ext_id)
                to_delete.add(ext_id)
                to_create.add(ext_id)
            else:
                LOG.warning(
                    "Observation %s changed but claim already started -- leaving as-is",
                    ext_id,
                )
                stats.unchanged += 1
        else:
            stats.unchanged += 1

    # 5. Delete cancelled future claims
    for ext_id in to_delete:
        if ext_id in current_by_ext_id:
            claim = current_by_ext_id[ext_id]
            claim_id = claim.id if hasattr(claim, "id") else None
            if claim_id:
                LOG.info("Deleting claim %s (ext_id=%s)", claim_id, ext_id)
                resp = client.delete_claim(claim_id=str(claim_id))
                if resp.is_success:
                    stats.deleted += 1
                else:
                    LOG.error(
                        "Failed to delete claim %s: %s", claim_id, resp.status_code
                    )
                    stats.errors += 1

    # 6. Create new claims
    for ext_id in to_create:
        obs = desired_by_ext_id[ext_id]
        LOG.info("Creating claim for observation %s (%s)", ext_id, obs.name)
        claim = _build_claim(obs, source, element_id, spectrum_id, spectrum_freq)
        resp = client.create_claim(body=claim)
        if resp.is_success:
            created = cast(Claim, resp.parsed)
            LOG.info("Created claim %s for observation %s", created.id, ext_id)
            stats.created += 1
        else:
            LOG.error("Failed to create claim for %s: %s", ext_id, resp.status_code)
            stats.errors += 1

    return stats


def _is_future_claim(claim: Claim, now: datetime.datetime) -> bool:
    """Return True if the claim's grant hasn't started yet."""
    grant = claim.grant
    if grant is None:
        return True
    if grant.starts_at is None:
        return True
    return grant.starts_at > now


def _list_all_claims(
    client: ZmsZmcClient,
    element_id: str,
    source_type: str,
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
        claim_list = cast(ClaimList, resp.parsed)
        claims.extend(claim_list.claims)
        if page >= claim_list.pages:
            break
        page += 1
    return claims


def _build_claim(
    obs: Observation,
    source: RASource,
    element_id: str,
    spectrum_id: str,
    spectrum_freq: tuple[int, int],
) -> Claim:
    """Convert an RA Observation into a ZMS Claim with inline Grant.

    If the observation has no frequency info (e.g. calendar events),
    falls back to the full spectrum frequency range.
    """
    min_freq = obs.min_freq_hz if obs.min_freq_hz else spectrum_freq[0]
    max_freq = obs.max_freq_hz if obs.max_freq_hz else spectrum_freq[1]

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
                        min_freq=min_freq,
                        max_freq=max_freq,
                        max_eirp=obs.max_eirp,
                        exclusive=True,
                    )
                )
            ],
        ),
    )


def _claim_needs_update(claim: Claim, obs: Observation) -> bool:
    """Check if an existing claim's grant differs from the desired observation."""
    grant = claim.grant
    if grant is None:
        return True

    # Check time window
    if grant.starts_at != obs.start:
        return True
    if grant.expires_at != obs.end:
        return True

    # Check frequency constraints
    if grant.constraints and len(grant.constraints) > 0:
        c = grant.constraints[0].constraint
        if c and (c.min_freq != obs.min_freq_hz or c.max_freq != obs.max_freq_hz):
            return True

    return False
