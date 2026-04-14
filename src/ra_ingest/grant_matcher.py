"""Finds the gcal grant in ZMC that covers a given observation's time window."""

from __future__ import annotations

import datetime
import logging
from typing import cast

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import ClaimList

LOG = logging.getLogger(__name__)

GCAL_EXT_ID_PREFIX = "gcal-"


def fetch_gcal_grants(
    client: ZmsZmcClient,
    element_id: str,
) -> list:
    """Fetch all current/future gcal claims (with their inline grants)."""
    grants = []
    page = 1
    while True:
        resp = client.list_claims(
            element_id=element_id,
            ext=GCAL_EXT_ID_PREFIX,
            page=page,
            items_per_page=100,
            x_api_elaborate="True",
        )
        if not resp.is_success:
            LOG.error(
                "Failed to list gcal claims (page %d): %s", page, resp.status_code
            )
            break
        if not isinstance(resp.parsed, ClaimList):
            break
        result = cast(ClaimList, resp.parsed)
        for claim in result.claims:
            if claim.grant is not None:
                grants.append(claim.grant)
        if page >= result.pages:
            break
        page += 1
    return grants


def find_matching_grant(
    grants: list,
    obs_start: datetime.datetime,
    obs_end: datetime.datetime,
) -> str | None:
    """Find the grant whose time window contains the observation.

    Returns the grant.id (as string) or None if no match.
    """
    for grant in grants:
        if grant.starts_at is None or grant.expires_at is None:
            continue
        if grant.starts_at <= obs_start and grant.expires_at >= obs_end:
            return str(grant.id)
    return None
