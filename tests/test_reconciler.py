"""Tests for the unified reconciler.

One loop, every source mints its own Claim+Grant in ZMC. ODS-style sources
(protect_started + writes_observations) also get an RAObservation whose
lifecycle mirrors the claim. gcal-style sources mint a grant only and guard on
grant end, not start.

Clients are mocked to avoid needing running instances.
"""

import datetime
from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import (
    Claim,
    ClaimList,
    Constraint,
    Grant,
    GrantConstraint,
    Spectrum,
)

from ra_ingest.reconciler import (
    _claim_ended,
    _claim_matches,
    _claim_started,
    reconcile,
)
from ra_ingest.sources.protocol import Observation, ObsTarget

UTC = datetime.UTC
NOW = datetime.datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC)
ELEMENT_ID = "elem-1"
SPECTRUM_ID = "spec-1"
MIN_FREQ = 1_990_000_000
MAX_FREQ = 1_995_000_000

ODS_PREFIX = "ods-hcro-"
GCAL_PREFIX = "gcal-"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _make_obs(
    ext_id,
    start_offset_hours=1,
    end_offset_hours=2,
    min_freq=MIN_FREQ,
    max_freq=MAX_FREQ,
    with_target=True,
):
    return Observation(
        ext_id=ext_id,
        name=f"obs-{ext_id}",
        start=NOW + datetime.timedelta(hours=start_offset_hours),
        end=NOW + datetime.timedelta(hours=end_offset_hours),
        min_freq_hz=min_freq,
        max_freq_hz=max_freq,
        target=ObsTarget(site_id="ATA", source_id="ASP") if with_target else None,
    )


def _make_grant(grant_id, starts_at, expires_at, min_freq=MIN_FREQ, max_freq=MAX_FREQ):
    c = Constraint(min_freq=min_freq, max_freq=max_freq, max_eirp=0.0, exclusive=True)
    g = Grant(
        name=f"grant-{grant_id}",
        description="",
        element_id=ELEMENT_ID,
        spectrum_id=SPECTRUM_ID,
        starts_at=starts_at,
        expires_at=expires_at,
        constraints=[GrantConstraint(constraint=c)],
    )
    g.id = grant_id
    return g


def _make_claim(
    ext_id, starts_at, expires_at, min_freq=MIN_FREQ, max_freq=MAX_FREQ, grant_id=None
):
    g = _make_grant(
        grant_id or f"grant-{ext_id}", starts_at, expires_at, min_freq, max_freq
    )
    claim = Claim(
        element_id=ELEMENT_ID,
        ext_id=ext_id,
        type="ra-ods",
        source="hcro",
        name=f"claim-{ext_id}",
        description="",
        grant=g,
    )
    claim.id = f"claim-id-{ext_id}"
    return claim


def _make_claim_for(obs, grant_id=None):
    """A claim that exactly matches obs (same window + freq)."""
    return _make_claim(
        obs.ext_id, obs.start, obs.end, obs.min_freq_hz, obs.max_freq_hz, grant_id
    )


def _make_source(observations, *, ods=True, prefix=None):
    source = MagicMock()
    source.source_type = "ra-ods" if ods else "gcal"
    source.source_name = "hcro" if ods else "ata"
    source.ext_id_prefix = prefix or (ODS_PREFIX if ods else GCAL_PREFIX)
    source.protect_started = ods
    source.writes_observations = ods
    source.fetch_observations.return_value = observations
    return source


def _make_zmc_client(existing_claims=None, created_grant_id="new-grant-id"):
    claim_list = MagicMock(spec=ClaimList)
    claim_list.claims = existing_claims or []
    claim_list.pages = 1
    list_resp = MagicMock(is_success=True, parsed=claim_list, status_code=200)

    # create_claim returns the elaborated claim carrying the new grant id.
    created = _make_claim(
        "created", NOW, NOW + datetime.timedelta(hours=1), grant_id=created_grant_id
    )
    create_resp = MagicMock(is_success=True, parsed=created, status_code=201)
    delete_resp = MagicMock(is_success=True, status_code=200)

    client = MagicMock()
    client.list_claims.return_value = list_resp
    client.create_claim.return_value = create_resp
    client.delete_claim.return_value = delete_resp
    return client


def _make_ra_client(existing_raobs=None):
    client = MagicMock()
    client.list_observations.return_value = existing_raobs or []
    client.create_observation.return_value = {"id": "new-ra-id"}
    client.delete_observation.return_value = True
    return client


def _make_raobs(ext_id):
    return {"TransactionId": ext_id, "Id": f"id-{ext_id}"}


def _make_picker():
    spec = MagicMock(spec=Spectrum)
    spec.id = SPECTRUM_ID
    spec.name = "test-spec"
    picker = MagicMock()
    picker.pick.return_value = spec
    picker.refresh.return_value = 1
    return picker


def _run(zmc, ra, source, picker=None):
    return reconcile(zmc, ra, source, ELEMENT_ID, picker or _make_picker(), now=NOW)


# ---------------------------------------------------------------------------
# Guard predicates
# ---------------------------------------------------------------------------


class TestClaimStarted:
    def test_future(self):
        c = _make_claim(
            "x", NOW + datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=2)
        )
        assert _claim_started(c, NOW) is False

    def test_active(self):
        c = _make_claim(
            "x", NOW - datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=1)
        )
        assert _claim_started(c, NOW) is True

    def test_starting_now(self):
        c = _make_claim("x", NOW, NOW + datetime.timedelta(hours=1))
        assert _claim_started(c, NOW) is True


class TestClaimEnded:
    def test_future(self):
        c = _make_claim(
            "x", NOW + datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=2)
        )
        assert _claim_ended(c, NOW) is False

    def test_active(self):
        c = _make_claim(
            "x", NOW - datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=1)
        )
        assert _claim_ended(c, NOW) is False

    def test_past(self):
        c = _make_claim(
            "x", NOW - datetime.timedelta(hours=2), NOW - datetime.timedelta(hours=1)
        )
        assert _claim_ended(c, NOW) is True


class TestClaimMatches:
    def test_identical(self):
        obs = _make_obs("x")
        assert _claim_matches(_make_claim_for(obs), obs) is True

    def test_time_changed(self):
        obs = _make_obs("x")
        c = _make_claim("x", obs.start, obs.end + datetime.timedelta(minutes=30))
        assert _claim_matches(c, obs) is False

    def test_freq_changed(self):
        obs = _make_obs("x", min_freq=MIN_FREQ, max_freq=MAX_FREQ)
        c = _make_claim(
            "x", obs.start, obs.end, min_freq=MIN_FREQ, max_freq=MAX_FREQ + 5
        )
        assert _claim_matches(c, obs) is False


# ---------------------------------------------------------------------------
# Grant lifecycle (both source styles)
# ---------------------------------------------------------------------------


class TestReconcileGrants:
    def test_create_new_mints_claim(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.created == 1
        zmc.create_claim.assert_called_once()

    def test_create_new_gcal_no_raobs(self):
        obs = _make_obs(f"{GCAL_PREFIX}1", with_target=False)
        zmc = _make_zmc_client(existing_claims=[])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([obs], ods=False))

        assert stats.created == 1
        assert stats.ra_created == 0
        ra.create_observation.assert_not_called()

    def test_no_change(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[_make_claim_for(obs)])
        ra = _make_ra_client(existing_raobs=[_make_raobs(obs.ext_id)])

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.unchanged == 1
        assert stats.created == 0
        assert stats.deleted == 0
        zmc.create_claim.assert_not_called()
        zmc.delete_claim.assert_not_called()
        ra.create_observation.assert_not_called()

    def test_vanished_future_deleted(self):
        claim = _make_claim(
            f"{ODS_PREFIX}gone",
            NOW + datetime.timedelta(hours=3),
            NOW + datetime.timedelta(hours=4),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        ra = _make_ra_client(existing_raobs=[_make_raobs(f"{ODS_PREFIX}gone")])

        stats = _run(zmc, ra, _make_source([]))

        assert stats.deleted == 1
        assert stats.ra_deleted == 1
        zmc.delete_claim.assert_called_once_with(claim_id=f"claim-id-{ODS_PREFIX}gone")
        ra.delete_observation.assert_called_once_with(f"{ODS_PREFIX}gone")

    def test_vanished_started_kept_ods(self):
        claim = _make_claim(
            f"{ODS_PREFIX}live",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=1),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        ra = _make_ra_client(existing_raobs=[_make_raobs(f"{ODS_PREFIX}live")])

        stats = _run(zmc, ra, _make_source([]))

        assert stats.deleted == 0
        assert stats.unchanged == 1
        zmc.delete_claim.assert_not_called()
        ra.delete_observation.assert_not_called()

    def test_vanished_active_deleted_gcal(self):
        # gcal guards on end, not start: an active block removed from the
        # calendar frees the band.
        claim = _make_claim(
            f"{GCAL_PREFIX}live",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=1),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([], ods=False))

        assert stats.deleted == 1
        zmc.delete_claim.assert_called_once_with(claim_id=f"claim-id-{GCAL_PREFIX}live")

    def test_vanished_past_kept(self):
        claim = _make_claim(
            f"{GCAL_PREFIX}done",
            NOW - datetime.timedelta(hours=4),
            NOW - datetime.timedelta(hours=3),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([], ods=False))

        assert stats.deleted == 0
        assert stats.unchanged == 1

    def test_drift_future_recreated(self):
        old = _make_claim(
            f"{ODS_PREFIX}moved",
            NOW + datetime.timedelta(hours=2),
            NOW + datetime.timedelta(hours=3),
        )
        new_obs = _make_obs(
            f"{ODS_PREFIX}moved", start_offset_hours=4, end_offset_hours=5
        )
        zmc = _make_zmc_client(existing_claims=[old])
        ra = _make_ra_client(existing_raobs=[_make_raobs(f"{ODS_PREFIX}moved")])

        stats = _run(zmc, ra, _make_source([new_obs]))

        assert stats.deleted == 1
        assert stats.created == 1
        assert stats.ra_deleted == 1
        assert stats.ra_created == 1

    def test_drift_started_kept_ods(self):
        old = _make_claim(
            f"{ODS_PREFIX}live",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=1),
        )
        new_obs = _make_obs(
            f"{ODS_PREFIX}live", start_offset_hours=-1, end_offset_hours=2
        )
        zmc = _make_zmc_client(existing_claims=[old])
        ra = _make_ra_client(existing_raobs=[_make_raobs(f"{ODS_PREFIX}live")])

        stats = _run(zmc, ra, _make_source([new_obs]))

        assert stats.deleted == 0
        assert stats.created == 0
        assert stats.unchanged == 1

    def test_drift_active_recreated_gcal(self):
        old = _make_claim(
            f"{GCAL_PREFIX}live",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=3),
        )
        new_obs = _make_obs(
            f"{GCAL_PREFIX}live",
            start_offset_hours=-1,
            end_offset_hours=1,
            with_target=False,
        )
        zmc = _make_zmc_client(existing_claims=[old])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([new_obs], ods=False))

        assert stats.deleted == 1
        assert stats.created == 1

    def test_other_source_claims_ignored(self):
        # A claim under a different prefix is not ours -- never touch it.
        theirs = _make_claim(
            "gcal-theirs",
            NOW + datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=2),
        )
        zmc = _make_zmc_client(existing_claims=[theirs])
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([]))  # ODS source, prefix ods-hcro-

        assert stats.deleted == 0
        zmc.delete_claim.assert_not_called()


# ---------------------------------------------------------------------------
# RAObservation
# ---------------------------------------------------------------------------


class TestRaobs:
    def test_raobs_created_with_new_grant_id(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[], created_grant_id="grant-xyz")
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.ra_created == 1
        body = ra.create_observation.call_args.args[0]
        assert body["GrantId"] == "grant-xyz"
        assert body["TransactionId"] == obs.ext_id

    def test_heal_missing_raobs(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        # Claim exists and matches, but the RAObservation row is missing (earlier
        # partial failure). Reconcile should re-POST it.
        zmc = _make_zmc_client(
            existing_claims=[_make_claim_for(obs, grant_id="g-heal")]
        )
        ra = _make_ra_client(existing_raobs=[])

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.unchanged == 1
        assert stats.ra_created == 1
        body = ra.create_observation.call_args.args[0]
        assert body["GrantId"] == "g-heal"

    def test_no_heal_for_gcal(self):
        obs = _make_obs(f"{GCAL_PREFIX}1", with_target=False)
        zmc = _make_zmc_client(existing_claims=[_make_claim_for(obs)])
        ra = _make_ra_client(existing_raobs=[])

        stats = _run(zmc, ra, _make_source([obs], ods=False))

        assert stats.unchanged == 1
        assert stats.ra_created == 0
        ra.create_observation.assert_not_called()
        ra.list_observations.assert_not_called()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TestErrors:
    def test_create_claim_error(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[])
        zmc.create_claim.return_value = MagicMock(is_success=False, status_code=500)
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.created == 0
        assert stats.errors == 1
        ra.create_observation.assert_not_called()

    def test_no_covering_spectrum(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[])
        ra = _make_ra_client()
        picker = _make_picker()
        picker.pick.return_value = None

        stats = _run(zmc, ra, _make_source([obs]), picker=picker)

        assert stats.created == 0
        assert stats.errors == 1
        zmc.create_claim.assert_not_called()

    def test_delete_claim_error(self):
        claim = _make_claim(
            f"{GCAL_PREFIX}err",
            NOW + datetime.timedelta(hours=3),
            NOW + datetime.timedelta(hours=4),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        zmc.delete_claim.return_value = MagicMock(is_success=False, status_code=500)
        ra = _make_ra_client()

        stats = _run(zmc, ra, _make_source([], ods=False))

        assert stats.deleted == 0
        assert stats.errors == 1

    def test_raobs_create_error_counts(self):
        obs = _make_obs(f"{ODS_PREFIX}1")
        zmc = _make_zmc_client(existing_claims=[])
        ra = _make_ra_client()
        ra.create_observation.return_value = None  # RAObservation POST fails

        stats = _run(zmc, ra, _make_source([obs]))

        assert stats.created == 1  # claim still minted
        assert stats.ra_created == 0
        assert stats.errors == 1

    def test_source_fetch_failure_preserves_state(self):
        from ra_ingest.sources.protocol import SourceFetchError

        claim = _make_claim(
            f"{ODS_PREFIX}1",
            NOW + datetime.timedelta(hours=3),
            NOW + datetime.timedelta(hours=4),
        )
        zmc = _make_zmc_client(existing_claims=[claim])
        ra = _make_ra_client(existing_raobs=[_make_raobs(f"{ODS_PREFIX}1")])
        source = _make_source([])
        source.fetch_observations.side_effect = SourceFetchError("ODS down")

        stats = _run(zmc, ra, source)

        assert stats.errors == 1
        assert stats.deleted == 0
        assert stats.created == 0
        zmc.delete_claim.assert_not_called()
        zmc.create_claim.assert_not_called()
        ra.delete_observation.assert_not_called()
