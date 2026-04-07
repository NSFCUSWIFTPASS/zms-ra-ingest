"""Tests for the reconciler's diff and time-guard logic.

We mock the ZMS client to avoid needing a running instance.
"""

import datetime
from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import ClaimList as RealClaimList
from zmsclient.zmc.v1.models import Spectrum as RealSpectrum

from ra_ingest.reconciler import _claim_matches, _started, reconcile
from ra_ingest.sources.protocol import Observation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

UTC = datetime.UTC
NOW = datetime.datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC)


def _make_obs(
    ext_id, start_offset_hours=1, end_offset_hours=2, min_freq=1990, max_freq=1995
):
    """Create an Observation relative to NOW."""
    return Observation(
        ext_id=ext_id,
        name=f"obs-{ext_id}",
        start=NOW + datetime.timedelta(hours=start_offset_hours),
        end=NOW + datetime.timedelta(hours=end_offset_hours),
        min_freq_hz=min_freq,
        max_freq_hz=max_freq,
    )


def _make_claim(ext_id, starts_at, expires_at, min_freq=1990, max_freq=1995):
    """Create a mock Claim object with the fields the reconciler reads."""
    constraint = MagicMock()
    constraint.min_freq = min_freq
    constraint.max_freq = max_freq

    grant_constraint = MagicMock()
    grant_constraint.constraint = constraint

    grant = MagicMock()
    grant.starts_at = starts_at
    grant.expires_at = expires_at
    grant.constraints = [grant_constraint]

    claim = MagicMock()
    claim.ext_id = ext_id
    claim.id = f"claim-{ext_id}"
    claim.grant = grant
    return claim


def _make_source(observations, source_type="ra-ods", source_name="hcro"):
    source = MagicMock()
    source.source_type = source_type
    source.source_name = source_name
    source.priority = 1023
    source.fetch_observations.return_value = observations
    return source


def _make_client(existing_claims=None):
    """Create a mock ZMS client with list_claims and create/delete stubs."""
    client = MagicMock()

    claims = existing_claims or []
    claim_list = MagicMock(spec=RealClaimList)
    claim_list.claims = claims
    claim_list.total = len(claims)
    claim_list.pages = 1

    list_resp = MagicMock()
    list_resp.is_success = True
    list_resp.parsed = claim_list
    list_resp.status_code = 200
    client.list_claims.return_value = list_resp

    spectrum_constraint = MagicMock()
    spectrum_constraint.min_freq = 1000
    spectrum_constraint.max_freq = 2000
    spectrum_constraint_wrapper = MagicMock()
    spectrum_constraint_wrapper.constraint = spectrum_constraint
    spectrum = MagicMock(spec=RealSpectrum)
    spectrum.constraints = [spectrum_constraint_wrapper]
    spec_resp = MagicMock()
    spec_resp.is_success = True
    spec_resp.parsed = spectrum
    client.get_spectrum.return_value = spec_resp

    created_claim = MagicMock()
    created_claim.id = "new-claim-id"
    create_resp = MagicMock()
    create_resp.is_success = True
    create_resp.parsed = created_claim
    create_resp.status_code = 201
    client.create_claim.return_value = create_resp

    delete_resp = MagicMock()
    delete_resp.is_success = True
    delete_resp.status_code = 200
    client.delete_claim.return_value = delete_resp

    return client


# ---------------------------------------------------------------------------
# _started tests
# ---------------------------------------------------------------------------


class TestStarted:
    def test_future_claim(self):
        claim = _make_claim(
            "x",
            starts_at=NOW + datetime.timedelta(hours=1),
            expires_at=NOW + datetime.timedelta(hours=2),
        )
        assert _started(claim, NOW) is False

    def test_past_claim(self):
        claim = _make_claim(
            "x",
            starts_at=NOW - datetime.timedelta(hours=2),
            expires_at=NOW - datetime.timedelta(hours=1),
        )
        assert _started(claim, NOW) is True

    def test_active_claim(self):
        claim = _make_claim(
            "x",
            starts_at=NOW - datetime.timedelta(hours=1),
            expires_at=NOW + datetime.timedelta(hours=1),
        )
        assert _started(claim, NOW) is True

    def test_claim_starting_exactly_now(self):
        claim = _make_claim(
            "x", starts_at=NOW, expires_at=NOW + datetime.timedelta(hours=1)
        )
        assert _started(claim, NOW) is True

    def test_claim_with_no_grant(self):
        claim = MagicMock()
        claim.grant = None
        claim.id = "bad-claim"
        assert _started(claim, NOW) is True

    def test_claim_with_no_starts_at(self):
        claim = MagicMock()
        claim.grant = MagicMock()
        claim.grant.starts_at = None
        claim.id = "bad-claim"
        assert _started(claim, NOW) is True


# ---------------------------------------------------------------------------
# _claim_matches tests
# ---------------------------------------------------------------------------


class TestClaimMatches:
    def test_identical(self):
        obs = _make_obs("x", start_offset_hours=1, end_offset_hours=2)
        claim = _make_claim("x", starts_at=obs.start, expires_at=obs.end)
        assert _claim_matches(claim, obs) is True

    def test_time_changed(self):
        obs = _make_obs("x", start_offset_hours=1, end_offset_hours=2)
        claim = _make_claim(
            "x",
            starts_at=obs.start,
            expires_at=obs.end + datetime.timedelta(minutes=30),
        )
        assert _claim_matches(claim, obs) is False

    def test_freq_changed(self):
        obs = _make_obs("x", min_freq=1990, max_freq=1995)
        claim = _make_claim(
            "x", starts_at=obs.start, expires_at=obs.end, min_freq=1990, max_freq=2000
        )
        assert _claim_matches(claim, obs) is False

    def test_no_grant(self):
        obs = _make_obs("x")
        claim = MagicMock()
        claim.grant = None
        assert _claim_matches(claim, obs) is False

    def test_no_constraints(self):
        obs = _make_obs("x")
        claim = _make_claim("x", starts_at=obs.start, expires_at=obs.end)
        claim.grant.constraints = []
        assert _claim_matches(claim, obs) is False


# ---------------------------------------------------------------------------
# reconcile tests
# ---------------------------------------------------------------------------


class TestReconcile:
    def test_create_new_observation(self):
        """Source has an observation, ZMS has no claims -> create."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        client = _make_client(existing_claims=[])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.created == 1
        assert stats.deleted == 0
        client.create_claim.assert_called_once()

    def test_no_changes(self):
        """Source and ZMS match -> no creates or deletes."""
        obs = _make_obs("obs-1", start_offset_hours=1, end_offset_hours=2)
        claim = _make_claim("obs-1", starts_at=obs.start, expires_at=obs.end)
        source = _make_source([obs])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.created == 0
        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.create_claim.assert_not_called()
        client.delete_claim.assert_not_called()

    def test_delete_cancelled_future_observation(self):
        """ZMS has a future claim that source no longer lists -> delete."""
        future_start = NOW + datetime.timedelta(hours=3)
        future_end = NOW + datetime.timedelta(hours=4)
        claim = _make_claim(
            "obs-cancelled", starts_at=future_start, expires_at=future_end
        )
        source = _make_source([])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.deleted == 1
        client.delete_claim.assert_called_once_with(claim_id="claim-obs-cancelled")

    def test_keep_past_claim_not_in_source(self):
        """ZMS has a past claim that source no longer lists -> keep it."""
        past_start = NOW - datetime.timedelta(hours=4)
        past_end = NOW - datetime.timedelta(hours=3)
        claim = _make_claim("obs-done", starts_at=past_start, expires_at=past_end)
        source = _make_source([])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.delete_claim.assert_not_called()

    def test_keep_active_claim_not_in_source(self):
        """ZMS has an active (started) claim that source no longer lists -> keep it."""
        active_start = NOW - datetime.timedelta(hours=1)
        active_end = NOW + datetime.timedelta(hours=1)
        claim = _make_claim("obs-active", starts_at=active_start, expires_at=active_end)
        source = _make_source([])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.delete_claim.assert_not_called()

    def test_recreate_changed_future_claim(self):
        """Source has updated time for a future observation -> delete + recreate."""
        old_start = NOW + datetime.timedelta(hours=2)
        old_end = NOW + datetime.timedelta(hours=3)
        new_obs = _make_obs("obs-moved", start_offset_hours=4, end_offset_hours=5)
        claim = _make_claim("obs-moved", starts_at=old_start, expires_at=old_end)
        source = _make_source([new_obs])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.deleted == 1
        assert stats.created == 1

    def test_no_recreate_changed_active_claim(self):
        """Source shows different time for an already-started claim -> leave it."""
        active_start = NOW - datetime.timedelta(hours=1)
        active_end = NOW + datetime.timedelta(hours=1)
        new_obs = _make_obs("obs-active", start_offset_hours=-1, end_offset_hours=2)
        claim = _make_claim("obs-active", starts_at=active_start, expires_at=active_end)
        source = _make_source([new_obs])
        client = _make_client(existing_claims=[claim])

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.deleted == 0
        assert stats.created == 0
        assert stats.unchanged == 1

    def test_mixed_scenario(self):
        """Multiple observations: one new, one unchanged, one cancelled future, one past."""
        existing_obs = _make_obs(
            "obs-existing", start_offset_hours=1, end_offset_hours=2
        )
        new_obs = _make_obs("obs-new", start_offset_hours=5, end_offset_hours=6)

        claim_existing = _make_claim(
            "obs-existing", starts_at=existing_obs.start, expires_at=existing_obs.end
        )
        claim_cancelled = _make_claim(
            "obs-cancelled",
            starts_at=NOW + datetime.timedelta(hours=8),
            expires_at=NOW + datetime.timedelta(hours=9),
        )
        claim_past = _make_claim(
            "obs-past",
            starts_at=NOW - datetime.timedelta(hours=5),
            expires_at=NOW - datetime.timedelta(hours=4),
        )

        source = _make_source([existing_obs, new_obs])
        client = _make_client(
            existing_claims=[claim_existing, claim_cancelled, claim_past]
        )

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.created == 1  # obs-new
        assert stats.deleted == 1  # obs-cancelled (future, not in source)
        assert stats.unchanged == 2  # obs-existing (matched) + obs-past (kept)

    def test_create_error_doesnt_crash(self):
        """API error on create -> stats.errors incremented, loop continues."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        client = _make_client(existing_claims=[])
        fail_resp = MagicMock()
        fail_resp.is_success = False
        fail_resp.status_code = 500
        client.create_claim.return_value = fail_resp

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.errors == 1
        assert stats.created == 0

    def test_delete_error_doesnt_crash(self):
        """API error on delete -> stats.errors incremented, loop continues."""
        future_start = NOW + datetime.timedelta(hours=3)
        future_end = NOW + datetime.timedelta(hours=4)
        claim = _make_claim("obs-1", starts_at=future_start, expires_at=future_end)
        source = _make_source([])
        client = _make_client(existing_claims=[claim])
        fail_resp = MagicMock()
        fail_resp.is_success = False
        fail_resp.status_code = 500
        client.delete_claim.return_value = fail_resp

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.errors == 1
        assert stats.deleted == 0

    def test_create_exception_doesnt_crash(self):
        """Exception during create -> stats.errors incremented, loop continues."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        client = _make_client(existing_claims=[])
        client.create_claim.side_effect = RuntimeError("boom")

        stats = reconcile(client, source, "elem-1", "spec-1", now=NOW)

        assert stats.errors == 1
        assert stats.created == 0
