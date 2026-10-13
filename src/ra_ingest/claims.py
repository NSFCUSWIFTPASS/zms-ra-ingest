"""Claim and grant helpers shared by the reconciler and the ATA guard.

Every claim ra-ingest makes is an exclusive, zero-EIRP claim on one spectrum,
covering one or more frequency ranges.
"""

from __future__ import annotations

import datetime
import logging
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

from .pagination import paginate

LOG = logging.getLogger(__name__)


def list_claims(
    client: ZmsZmcClient,
    element_id: str,
    ext_id_prefix: str,
    since: datetime.datetime | None = None,
) -> list[Claim]:
    """Fetch all non-deleted claims for element_id whose ext_id has the prefix,
    created at or after since if given."""
    window = {"start": since} if since is not None else {}

    def fetch(page: int) -> tuple[list[Claim], int] | None:
        resp = client.list_claims(
            element_id=element_id,
            ext_id=ext_id_prefix,
            page=page,
            items_per_page=100,
            x_api_elaborate="True",
            **window,
        )
        if not resp.is_success or not isinstance(resp.parsed, ClaimList):
            LOG.error("Failed to list claims (page %d): %s", page, resp.status_code)
            return None
        return resp.parsed.claims, resp.parsed.pages

    # ext_id filter is an ILIKE substring match; enforce prefix ourselves.
    return [
        c for c in paginate(fetch) if c.ext_id and c.ext_id.startswith(ext_id_prefix)
    ]


def build_grant(
    *,
    element_id: str,
    spectrum_id: str,
    ext_id: str,
    name: str,
    description: str,
    priority: int,
    start: datetime.datetime,
    end: datetime.datetime,
    ranges: list[tuple[int, int]],
) -> Grant:
    """An exclusive grant with one constraint per (min_hz, max_hz) range."""
    return Grant(
        name=name,
        description=description,
        element_id=element_id,
        spectrum_id=spectrum_id,
        ext_id=ext_id,
        priority=priority,
        starts_at=start,
        expires_at=end,
        constraints=[
            GrantConstraint(
                constraint=Constraint(
                    min_freq=min_hz, max_freq=max_hz, max_eirp=0.0, exclusive=True
                )
            )
            for min_hz, max_hz in ranges
        ],
        op_status=GrantOpStatus.SUBMITTED,
    )


def build_claim(grant: Grant, claim_type: str, claim_source: str) -> Claim:
    """A claim wrapping grant, sharing its name, description and ext_id."""
    return Claim(
        name=grant.name,
        description=grant.description,
        type=claim_type,
        source=claim_source,
        element_id=grant.element_id,
        ext_id=grant.ext_id,
        grant=grant,
    )


def create_claim(client: ZmsZmcClient, claim: Claim) -> Claim | None:
    """Create the claim and its grant. Returns the created claim, or None."""
    resp = client.create_claim(body=claim, x_api_elaborate="true")
    if not resp.is_success:
        LOG.error("Failed to create claim for %s: %s", claim.ext_id, resp.status_code)
        return None
    return cast(Claim, resp.parsed)


def replace_grant(client: ZmsZmcClient, claim: Claim, grant: Grant) -> Grant | None:
    """Replace the claim's grant; the claim then points at the new one.
    Returns the new grant, or None."""
    resp = client.replace_grant(
        grant_id=grant_id(claim), body=grant, x_api_elaborate="true"
    )
    if not resp.is_success:
        LOG.error("Failed to replace grant for %s: %s", claim.ext_id, resp.status_code)
        return None
    return cast(Grant, resp.parsed)


def delete_claim(client: ZmsZmcClient, claim: Claim) -> bool:
    """Delete a claim (and its grant). True on success."""
    resp = client.delete_claim(claim_id=str(claim.id))
    if resp.is_success:
        LOG.info("Deleted claim %s (ext_id=%s)", claim.id, claim.ext_id)
        return True
    LOG.error("Failed to delete claim %s: %s", claim.id, resp.status_code)
    return False


def grant_id(claim: Claim) -> str:
    """The grant id of an elaborated claim, as a string."""
    return str(claim.grant.id)


def claim_started(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already started."""
    start = claim.grant.starts_at
    if start.tzinfo is None:
        start = start.replace(tzinfo=datetime.UTC)
    return start <= now


def claim_ended(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already ended (expired)."""
    end = claim.grant.expires_at
    if end.tzinfo is None:
        end = end.replace(tzinfo=datetime.UTC)
    return end <= now
