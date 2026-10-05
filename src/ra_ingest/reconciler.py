"""Stateless reconciler: one loop, every source mints its own grants.

Each cycle, for a single source:
  1. Fetch desired Observations from the source.
  2. Fetch current ZMC claims scoped by the source's ext_id_prefix.
  3. Create new claims, delete vanished ones, recreate drifted ones -- each
     guarded so a live grant is never torn down (see `source.protect_started`).
     A drifted claim that is protected but still live has its grant replaced
     instead, so protection continues without a gap.

A source that also carries sky-pointing metadata (`source.writes_observations`)
gets an RAObservation in zms-ra alongside each grant, referencing the grant
this reconciler just minted. The RAObservation lifecycle mirrors the claim:
created with it, deleted with it, recreated on drift, and re-POSTed if found
missing (a heal after a partial failure).

Structure: the first pass classifies each observation into `to_delete` /
`to_replace` / `to_create` (or counts it unchanged); execution passes then do
the I/O.
"""

from __future__ import annotations

import dataclasses
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
    Spectrum,
)

from .pagination import paginate
from .ra_client import ZmsRaClient, observation_to_ra_body
from .sources.protocol import Observation, RASource, SourceFetchError
from .spectrum_picker import SpectrumPicker

LOG = logging.getLogger(__name__)


@dataclass
class ReconcileStats:
    created: int = 0
    deleted: int = 0
    replaced: int = 0
    unchanged: int = 0
    errors: int = 0
    ra_created: int = 0
    ra_deleted: int = 0


def reconcile(
    zmc_client: ZmsZmcClient,
    ra_client: ZmsRaClient,
    source: RASource,
    element_id: str,
    picker: SpectrumPicker,
    now: datetime.datetime | None = None,
) -> ReconcileStats:
    """Run one reconciliation cycle for a single source against ZMC (+ zms-ra)."""
    stats = ReconcileStats()
    now = now or datetime.datetime.now(datetime.UTC)

    picker.refresh()

    # A failed source fetch must NOT fall through to set-diff, e.g. desired={}
    # would delete every future Claim+Grant for this source during a brief
    # source outage.
    try:
        desired = {obs.ext_id: obs for obs in source.fetch_observations()}
    except SourceFetchError:
        LOG.error("Source fetch failed; skipping reconcile to preserve ZMS state")
        stats.errors += 1
        return stats

    lookback = source.claim_lookback
    since = now - lookback if lookback is not None else None
    current = {
        c.ext_id: c
        for c in _list_claims(zmc_client, element_id, source.ext_id_prefix, since)
        if c.ext_id
    }
    current_raobs = _list_raobs(ra_client, source)

    if source.correlate_repushes:
        desired = _correlate_repushes(desired, current)

    # Classify: sort each observation into to_delete / to_create (no I/O here).
    to_delete: list[tuple[str, Claim]] = []
    to_replace: list[tuple[Claim, Observation]] = []
    to_create: list[Observation] = []

    # Vanished: delete unless the grant is protected.
    for ext_id in current.keys() - desired.keys():
        claim = current[ext_id]
        try:
            if _protected(claim, source, now):
                stats.unchanged += 1
            else:
                to_delete.append((ext_id, claim))
        except Exception:
            LOG.exception("Error on vanished claim %s", ext_id)
            stats.errors += 1

    # Existing: matched (maybe heal a missing raobs), drifted (recreate), or
    # drifted but protected (replace the grant while it is still live).
    for ext_id in desired.keys() & current.keys():
        obs, claim = desired[ext_id], current[ext_id]
        try:
            if not _claim_matches(claim, obs):
                if not _protected(claim, source, now):
                    to_delete.append((ext_id, claim))
                    to_create.append(obs)
                elif not _claim_ended(claim, now) and obs.end > now:
                    to_replace.append((claim, obs))
                else:
                    LOG.warning("Observation %s changed but grant is protected", ext_id)
                    stats.unchanged += 1
            else:
                stats.unchanged += 1
                if source.writes_observations and ext_id not in current_raobs:
                    # Grant is correct but its raobs is missing (an earlier POST
                    # failed) -- re-post it.
                    if _post_raobs(ra_client, obs, _grant_id(claim)):
                        stats.ra_created += 1
                    else:
                        stats.errors += 1
        except Exception:
            LOG.exception("Error on changed claim %s", ext_id)
            stats.errors += 1

    # New observations.
    to_create.extend(desired[ext_id] for ext_id in desired.keys() - current.keys())

    # Execute: all deletes before any create, so a re-planned window's old row
    # is gone before its replacement is posted (avoids zms-ra 409s).
    for ext_id, claim in to_delete:
        try:
            if source.writes_observations and ext_id in current_raobs:
                if ra_client.delete_observation(ext_id):
                    stats.ra_deleted += 1
                else:
                    stats.errors += 1
            if _delete_claim(zmc_client, claim):
                stats.deleted += 1
            else:
                stats.errors += 1
        except Exception:
            LOG.exception("Error deleting claim %s", ext_id)
            stats.errors += 1

    # Replace keeps the claim and swaps its grant, so the raobs must be
    # re-posted to reference the new grant.
    for claim, obs in to_replace:
        try:
            grant_id = _replace_grant(
                zmc_client, claim, obs, element_id, picker, source
            )
            if grant_id is None:
                stats.errors += 1
                continue
            stats.replaced += 1
            if source.writes_observations:
                if obs.ext_id in current_raobs:
                    if ra_client.delete_observation(obs.ext_id):
                        stats.ra_deleted += 1
                    else:
                        stats.errors += 1
                if _post_raobs(ra_client, obs, grant_id):
                    stats.ra_created += 1
                else:
                    stats.errors += 1
        except Exception:
            LOG.exception("Error replacing grant for %s", obs.ext_id)
            stats.errors += 1

    for obs in to_create:
        try:
            grant_id = _create_claim(zmc_client, obs, element_id, picker, source)
            if grant_id is None:
                stats.errors += 1
                continue
            stats.created += 1
            if source.writes_observations:
                if _post_raobs(ra_client, obs, grant_id):
                    stats.ra_created += 1
                else:
                    stats.errors += 1
        except Exception:
            LOG.exception("Error creating grant for %s", obs.ext_id)
            stats.errors += 1

    return stats


def _correlate_repushes(
    desired: dict[str, Observation], current: dict[str, Claim]
) -> dict[str, Observation]:
    """Fold re-published records into the claim they replace.

    A source without stable ids (ODS) re-publishes the same observation with
    its window slid forward, so it arrives under a new ext_id. Match it to a
    claim whose record has vanished, with the same name, description and band
    and an overlapping window, then adopt that claim's ext_id and start. The
    claim stays one observation instead of two overlapping claims that deny
    each other.
    """
    vanished = [claim for ext_id, claim in current.items() if ext_id not in desired]
    result: dict[str, Observation] = {}
    for ext_id, obs in desired.items():
        claim = None if ext_id in current else _find_repushed(obs, vanished)
        if claim is None:
            result[ext_id] = obs
            continue
        vanished.remove(claim)
        LOG.info("Matched re-push %s to claim %s", ext_id, claim.ext_id)
        result[claim.ext_id] = dataclasses.replace(
            obs, ext_id=claim.ext_id, start=claim.grant.starts_at
        )
    return result


def _find_repushed(obs: Observation, claims: list[Claim]) -> Claim | None:
    """The claim obs is a re-push of: not denied, same name, description and
    band, and an overlapping window. None if there isn't one."""
    for claim in claims:
        grant = claim.grant
        c = grant.constraints[0].constraint
        if (
            not claim.denied_at
            and claim.name == obs.name
            and claim.description == obs.description
            and c.min_freq == obs.min_freq_hz
            and c.max_freq == obs.max_freq_hz
            and grant.starts_at < obs.end
            and obs.start < grant.expires_at
        ):
            return claim
    return None


def _protected(claim: Claim, source: RASource, now: datetime.datetime) -> bool:
    """True if this claim's grant must not be torn down.

    ODS-style sources protect a grant once it has STARTED (the feed flaps;
    absence is not a cancel). Calendar-style sources protect only once the
    grant has ENDED (an edit is authoritative and should take effect).
    """
    if source.protect_started:
        return _claim_started(claim, now)
    return _claim_ended(claim, now)


def _list_claims(
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


def _list_raobs(ra_client: ZmsRaClient, source: RASource) -> dict[str, dict]:
    """Current RAObservation rows for this source, keyed by TransactionId.

    Empty for sources that don't record observations. The zms-ra list is global,
    so we keep only the rows under this source's prefix.
    """
    if not source.writes_observations:
        return {}
    return {
        tid: rec
        for rec in ra_client.list_observations()
        if (tid := rec.get("TransactionId")) and tid.startswith(source.ext_id_prefix)
    }


def _create_claim(
    zmc_client: ZmsZmcClient,
    obs: Observation,
    element_id: str,
    picker: SpectrumPicker,
    source: RASource,
) -> str | None:
    """Pick a spectrum and create the Claim+Grant. Returns the new grant id, or
    None if no spectrum covers the band or the create failed."""
    spectrum = _pick_spectrum(picker, obs)
    if spectrum is None:
        return None
    body = _build_claim(obs, element_id, str(spectrum.id), source)
    resp = zmc_client.create_claim(body=body, x_api_elaborate="true")
    if not resp.is_success:
        LOG.error("Failed to create claim for %s: %s", obs.ext_id, resp.status_code)
        return None
    LOG.info("Created claim for %s on spectrum %s", obs.ext_id, spectrum.name)
    return str(cast(Claim, resp.parsed).grant.id)


def _replace_grant(
    zmc_client: ZmsZmcClient,
    claim: Claim,
    obs: Observation,
    element_id: str,
    picker: SpectrumPicker,
    source: RASource,
) -> str | None:
    """Replace a claim's grant with one matching obs. The claim is kept and
    points at the new grant. Returns the new grant id, or None if no spectrum
    covers the band or the replace failed."""
    spectrum = _pick_spectrum(picker, obs)
    if spectrum is None:
        return None
    body = _build_grant(obs, element_id, str(spectrum.id), source)
    resp = zmc_client.replace_grant(
        grant_id=_grant_id(claim), body=body, x_api_elaborate="true"
    )
    if not resp.is_success:
        LOG.error("Failed to replace grant for %s: %s", obs.ext_id, resp.status_code)
        return None
    LOG.info("Replaced grant for %s (now ends %s)", obs.ext_id, obs.end)
    return str(cast(Grant, resp.parsed).id)


def _pick_spectrum(picker: SpectrumPicker, obs: Observation) -> Spectrum | None:
    """The spectrum covering obs's band, or None (logged) if there isn't one."""
    spectrum = picker.pick(obs.min_freq_hz, obs.max_freq_hz)
    if spectrum is None:
        LOG.error(
            "No spectrum covers %s (%d-%d Hz); skipping",
            obs.ext_id,
            obs.min_freq_hz,
            obs.max_freq_hz,
        )
    return spectrum


def _delete_claim(zmc_client: ZmsZmcClient, claim: Claim) -> bool:
    """Delete a claim (and its grant). True on success."""
    resp = zmc_client.delete_claim(claim_id=str(claim.id))
    if resp.is_success:
        LOG.info("Deleted claim %s (ext_id=%s)", claim.id, claim.ext_id)
        return True
    LOG.error("Failed to delete claim %s: %s", claim.id, resp.status_code)
    return False


def _post_raobs(ra_client: ZmsRaClient, obs: Observation, grant_id: str) -> bool:
    """POST the RAObservation for obs, referencing its grant. True on success."""
    if ra_client.create_observation(observation_to_ra_body(obs, grant_id)) is not None:
        LOG.info("Posted raobservation for %s (grant=%s)", obs.ext_id, grant_id)
        return True
    return False


def _build_grant(
    obs: Observation,
    element_id: str,
    spectrum_id: str,
    source: RASource,
) -> Grant:
    return Grant(
        name=obs.name,
        description=obs.description,
        element_id=element_id,
        spectrum_id=spectrum_id,
        ext_id=obs.ext_id,
        priority=source.priority,
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


def _build_claim(
    obs: Observation,
    element_id: str,
    spectrum_id: str,
    source: RASource,
) -> Claim:
    return Claim(
        name=obs.name,
        description=obs.description,
        type=source.source_type,
        source=source.source_name,
        element_id=element_id,
        ext_id=obs.ext_id,
        grant=_build_grant(obs, element_id, spectrum_id, source),
    )


def _grant_id(claim: Claim) -> str:
    """The grant id of an elaborated claim, as a string."""
    return str(claim.grant.id)


def _claim_started(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already started."""
    start = claim.grant.starts_at
    if start.tzinfo is None:
        start = start.replace(tzinfo=datetime.UTC)
    return start <= now


def _claim_ended(claim: Claim, now: datetime.datetime) -> bool:
    """True if the claim's grant has already ended (expired)."""
    end = claim.grant.expires_at
    if end.tzinfo is None:
        end = end.replace(tzinfo=datetime.UTC)
    return end <= now


def _claim_matches(claim: Claim, obs: Observation) -> bool:
    """True if the claim's grant still matches the observation's time/freq."""
    grant = claim.grant
    if grant.starts_at != obs.start or grant.expires_at != obs.end:
        return False
    c = grant.constraints[0].constraint
    return c.min_freq == obs.min_freq_hz and c.max_freq == obs.max_freq_hz
