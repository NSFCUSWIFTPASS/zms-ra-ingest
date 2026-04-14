"""Stateless reconciler: diffs RA source observations against zms-ra records.

Each cycle fetches observations from a source, fetches current RAObservation
records from zms-ra, then creates/deletes to converge. Each new observation
is linked to the matching gcal grant in ZMC via grant_id.

Records for observations that have already started are never deleted -- they
represent real spectrum usage that happened or is happening.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

from zmsclient.zmc.client import ZmsZmcClient

from .grant_matcher import fetch_gcal_grants, find_matching_grant
from .ra_client import ZmsRaClient, observation_to_ra_body
from .sources.protocol import Observation, RASource

LOG = logging.getLogger(__name__)


@dataclass
class ReconcileStats:
    created: int = 0
    deleted: int = 0
    unchanged: int = 0
    errors: int = 0
    unmatched: int = 0  # observations with no parent gcal grant


def reconcile(
    zmc_client: ZmsZmcClient,
    ra_client: ZmsRaClient,
    source: RASource,
    element_id: str,
    now: datetime.datetime | None = None,
) -> ReconcileStats:
    """Run one reconciliation cycle for a single source."""
    stats = ReconcileStats()
    now = now or datetime.datetime.now(datetime.UTC)

    # Fetch desired state from the source
    desired = {obs.ext_id: obs for obs in source.fetch_observations()}

    # Fetch current state from zms-ra. zms-ra has no type/source filter,
    # so we list everything and trust we own these records.
    current = {
        rec["TransactionId"]: rec
        for rec in ra_client.list_observations()
        if rec.get("TransactionId")
    }

    # Fetch gcal grants once for matching
    gcal_grants = fetch_gcal_grants(zmc_client, element_id)
    LOG.info("Loaded %d gcal grants for matching", len(gcal_grants))

    # New observations: create
    for ext_id in desired.keys() - current.keys():
        _try_create(ra_client, desired[ext_id], gcal_grants, stats)

    # Vanished observations: delete (only if not yet started)
    for ext_id in current.keys() - desired.keys():
        rec = current[ext_id]
        if _record_started(rec, now):
            stats.unchanged += 1
        else:
            _try_delete(ra_client, rec, stats)

    # Existing observations: check for drift
    for ext_id in desired.keys() & current.keys():
        obs, rec = desired[ext_id], current[ext_id]
        if not _record_matches(rec, obs):
            if _record_started(rec, now):
                LOG.warning("Observation %s changed but record already started", ext_id)
                stats.unchanged += 1
            else:
                _try_delete(ra_client, rec, stats)
                _try_create(ra_client, obs, gcal_grants, stats)
        else:
            stats.unchanged += 1

    return stats


def _try_create(
    ra_client: ZmsRaClient,
    obs: Observation,
    gcal_grants: list,
    stats: ReconcileStats,
) -> None:
    try:
        grant_id = find_matching_grant(gcal_grants, obs.start, obs.end)
        if grant_id is None:
            LOG.warning(
                "No gcal grant covers observation %s (%s -> %s); skipping",
                obs.ext_id,
                obs.start,
                obs.end,
            )
            stats.unmatched += 1
            return

        body = observation_to_ra_body(obs, grant_id)
        result = ra_client.create_observation(body)
        if result is not None:
            LOG.info("Created raobservation for %s (grant=%s)", obs.ext_id, grant_id)
            stats.created += 1
        else:
            stats.errors += 1
    except Exception:
        LOG.exception("Error creating raobservation for %s", obs.ext_id)
        stats.errors += 1


def _try_delete(ra_client: ZmsRaClient, rec: dict, stats: ReconcileStats) -> None:
    try:
        rec_id = rec.get("TransactionId") or rec.get("Id")
        if not rec_id:
            stats.errors += 1
            return
        if ra_client.delete_observation(str(rec_id)):
            LOG.info("Deleted raobservation %s", rec_id)
            stats.deleted += 1
        else:
            stats.errors += 1
    except Exception:
        LOG.exception("Error deleting raobservation")
        stats.errors += 1


def _record_started(rec: dict, now: datetime.datetime) -> bool:
    """True if this record's observation window has already started."""
    start_str = rec.get("DateTimeStart")
    if not start_str:
        return True  # be conservative -- don't delete what we don't understand
    try:
        start = datetime.datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=datetime.UTC)
        return start <= now
    except (ValueError, TypeError):
        LOG.warning(
            "Could not parse DateTimeStart on record: %r", rec.get("TransactionId")
        )
        return True


def _record_matches(rec: dict, obs: Observation) -> bool:
    """True if this record's time/freq still matches the observation."""
    try:
        start = datetime.datetime.fromisoformat(
            str(rec["DateTimeStart"]).replace("Z", "+00:00")
        )
        stop = datetime.datetime.fromisoformat(
            str(rec["DateTimeStop"]).replace("Z", "+00:00")
        )
        if start.tzinfo is None:
            start = start.replace(tzinfo=datetime.UTC)
        if stop.tzinfo is None:
            stop = stop.replace(tzinfo=datetime.UTC)
        return (
            start == obs.start
            and stop == obs.end
            and float(rec.get("FreqStart", 0)) == float(obs.min_freq_hz)
            and float(rec.get("FreqStop", 0)) == float(obs.max_freq_hz)
        )
    except (KeyError, ValueError, TypeError):
        return False
