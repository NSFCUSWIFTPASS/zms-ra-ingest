"""Tests for the gcal reconciler: diff + time-guards against ZMC claims."""

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

from ra_ingest.gcal_reconciler import (
    _claim_matches,
    _claim_started,
    reconcile_gcal,
)
from ra_ingest.sources.protocol import Observation

UTC = datetime.UTC
NOW = datetime.datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
ELEMENT_ID = "elem-1"
SPECTRUM_ID = "spec-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_obs(
    ext_id, start_offset=1, end_offset=2, min_freq=902_000_000, max_freq=928_000_000
):
    return Observation(
        ext_id=ext_id,
        name=f"obs-{ext_id}",
        start=NOW + datetime.timedelta(hours=start_offset),
        end=NOW + datetime.timedelta(hours=end_offset),
        min_freq_hz=min_freq,
        max_freq_hz=max_freq,
    )


def _make_claim(
    ext_id, starts_at, expires_at, min_freq=902_000_000, max_freq=928_000_000
):
    c = Constraint(min_freq=min_freq, max_freq=max_freq, max_eirp=0.0, exclusive=True)
    gc = GrantConstraint(constraint=c)
    g = Grant(
        name=f"grant-{ext_id}",
        description="",
        element_id=ELEMENT_ID,
        spectrum_id=SPECTRUM_ID,
        starts_at=starts_at,
        expires_at=expires_at,
        constraints=[gc],
    )
    claim = Claim(
        element_id=ELEMENT_ID,
        ext_id=ext_id,
        type="gcal",
        source="ata",
        name=f"claim-{ext_id}",
        description="",
        grant=g,
    )
    claim.id = f"claim-id-{ext_id}"
    return claim


def _make_source(observations, ext_id_prefix="gcal-"):
    source = MagicMock()
    source.source_type = "gcal"
    source.source_name = "ata"
    source.ext_id_prefix = ext_id_prefix
    source.fetch_observations.return_value = observations
    return source


def _make_picker(spectrum_id=SPECTRUM_ID):
    """Build a mock SpectrumPicker that always returns the same spectrum."""
    spec = MagicMock(spec=Spectrum)
    spec.id = spectrum_id
    spec.name = "test-spec"
    picker = MagicMock()
    picker.pick.return_value = spec
    picker.refresh.return_value = 1
    return picker


def _make_client(existing_claims=None):
    claims = existing_claims or []
    claim_list = MagicMock(spec=ClaimList)
    claim_list.claims = claims
    claim_list.pages = 1

    list_resp = MagicMock()
    list_resp.is_success = True
    list_resp.parsed = claim_list
    list_resp.status_code = 200

    create_resp = MagicMock()
    create_resp.is_success = True
    create_resp.status_code = 201

    delete_resp = MagicMock()
    delete_resp.is_success = True
    delete_resp.status_code = 200

    client = MagicMock()
    client.list_claims.return_value = list_resp
    client.create_claim.return_value = create_resp
    client.delete_claim.return_value = delete_resp
    return client


# ---------------------------------------------------------------------------
# _claim_started
# ---------------------------------------------------------------------------


class TestClaimStarted:
    def test_future_claim(self):
        c = _make_claim(
            "x", NOW + datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=2)
        )
        assert _claim_started(c, NOW) is False

    def test_past_claim(self):
        c = _make_claim(
            "x", NOW - datetime.timedelta(hours=2), NOW - datetime.timedelta(hours=1)
        )
        assert _claim_started(c, NOW) is True

    def test_active_claim(self):
        c = _make_claim(
            "x", NOW - datetime.timedelta(hours=1), NOW + datetime.timedelta(hours=1)
        )
        assert _claim_started(c, NOW) is True

    def test_no_grant_is_conservative(self):
        c = MagicMock(spec=Claim)
        c.grant = None
        assert _claim_started(c, NOW) is True


# ---------------------------------------------------------------------------
# _claim_matches
# ---------------------------------------------------------------------------


class TestClaimMatches:
    def test_identical(self):
        obs = _make_obs("x")
        c = _make_claim("x", obs.start, obs.end)
        assert _claim_matches(c, obs) is True

    def test_time_changed(self):
        obs = _make_obs("x")
        c = _make_claim("x", obs.start, obs.end + datetime.timedelta(minutes=30))
        assert _claim_matches(c, obs) is False

    def test_freq_changed(self):
        obs = _make_obs("x", min_freq=902_000_000, max_freq=928_000_000)
        c = _make_claim(
            "x", obs.start, obs.end, min_freq=902_000_000, max_freq=950_000_000
        )
        assert _claim_matches(c, obs) is False


# ---------------------------------------------------------------------------
# reconcile_gcal
# ---------------------------------------------------------------------------


class TestReconcileGcal:
    def test_create_new(self):
        obs = _make_obs("gcal-new")
        source = _make_source([obs])
        client = _make_client(existing_claims=[])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.created == 1
        assert stats.deleted == 0
        client.create_claim.assert_called_once()

    def test_no_changes(self):
        obs = _make_obs("gcal-same")
        c = _make_claim("gcal-same", obs.start, obs.end)
        source = _make_source([obs])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.created == 0
        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.create_claim.assert_not_called()
        client.delete_claim.assert_not_called()

    def test_delete_cancelled_future(self):
        c = _make_claim(
            "gcal-cancelled",
            NOW + datetime.timedelta(hours=3),
            NOW + datetime.timedelta(hours=4),
        )
        source = _make_source([])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 1
        client.delete_claim.assert_called_once_with(claim_id="claim-id-gcal-cancelled")

    def test_keep_past_claim_not_in_source(self):
        c = _make_claim(
            "gcal-done",
            NOW - datetime.timedelta(hours=5),
            NOW - datetime.timedelta(hours=4),
        )
        source = _make_source([])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.delete_claim.assert_not_called()

    def test_keep_active_claim_not_in_source(self):
        c = _make_claim(
            "gcal-active",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=1),
        )
        source = _make_source([])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        client.delete_claim.assert_not_called()

    def test_recreate_drifted_future_claim(self):
        old_start = NOW + datetime.timedelta(hours=2)
        old_end = NOW + datetime.timedelta(hours=3)
        c = _make_claim("gcal-moved", old_start, old_end)
        new_obs = _make_obs("gcal-moved", start_offset=4, end_offset=5)
        source = _make_source([new_obs])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 1
        assert stats.created == 1

    def test_no_recreate_drifted_active_claim(self):
        active_start = NOW - datetime.timedelta(hours=1)
        active_end = NOW + datetime.timedelta(hours=1)
        c = _make_claim("gcal-active", active_start, active_end)
        new_obs = _make_obs("gcal-active", start_offset=-2, end_offset=2)
        source = _make_source([new_obs])
        client = _make_client(existing_claims=[c])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 0
        assert stats.created == 0
        assert stats.unchanged == 1

    def test_filters_by_ext_id_prefix(self):
        """list_claims returns claims that don't match our prefix -- filter them out."""
        ours = _make_claim(
            "gcal-ours",
            NOW + datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=2),
        )
        theirs = _make_claim(
            "ra-ods-theirs",
            NOW + datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=2),
        )
        source = _make_source([])  # nothing desired
        client = _make_client(existing_claims=[ours, theirs])

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        # Should only try to delete the "gcal-" prefixed claim, not the ra-ods one
        assert stats.deleted == 1
        client.delete_claim.assert_called_once_with(claim_id="claim-id-gcal-ours")

    def test_create_error_counted(self):
        obs = _make_obs("gcal-err")
        source = _make_source([obs])
        client = _make_client(existing_claims=[])
        client.create_claim.return_value = MagicMock(is_success=False, status_code=500)

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.created == 0
        assert stats.errors == 1

    def test_delete_error_counted(self):
        c = _make_claim(
            "gcal-err",
            NOW + datetime.timedelta(hours=3),
            NOW + datetime.timedelta(hours=4),
        )
        source = _make_source([])
        client = _make_client(existing_claims=[c])
        client.delete_claim.return_value = MagicMock(is_success=False, status_code=500)

        stats = reconcile_gcal(client, source, ELEMENT_ID, _make_picker(), now=NOW)

        assert stats.deleted == 0
        assert stats.errors == 1
