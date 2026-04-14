"""Tests for the reconciler's diff and time-guard logic.

We mock the ZMS clients to avoid needing running instances.
"""

import datetime
from unittest.mock import MagicMock

from ra_ingest.reconciler import _record_matches, _record_started, reconcile
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
        site_id="ATA",
        site_lat=40.8,
        site_lon=-121.5,
        site_elevation=1000.0,
        source_id="ASP",
    )


def _make_record(ext_id, start, stop, min_freq=1990, max_freq=1995):
    """Create a zms-ra record dict."""
    return {
        "TransactionId": ext_id,
        "Id": f"id-{ext_id}",
        "DateTimeStart": start.isoformat(),
        "DateTimeStop": stop.isoformat(),
        "FreqStart": float(min_freq),
        "FreqStop": float(max_freq),
    }


def _make_grant(grant_id, starts_at, expires_at):
    """Create a mock Grant object."""
    g = MagicMock()
    g.id = grant_id
    g.starts_at = starts_at
    g.expires_at = expires_at
    return g


def _make_source(observations, source_type="ra-ods", source_name="hcro"):
    source = MagicMock()
    source.source_type = source_type
    source.source_name = source_name
    source.fetch_observations.return_value = observations
    return source


def _make_zmc_client(grants=None):
    """Mock ZmsZmcClient that returns the given grants from list_claims."""
    from zmsclient.zmc.v1.models import ClaimList

    client = MagicMock()
    grants = grants or []
    claims = []
    for g in grants:
        c = MagicMock()
        c.grant = g
        claims.append(c)

    claim_list = MagicMock(spec=ClaimList)
    claim_list.claims = claims
    claim_list.pages = 1
    resp = MagicMock()
    resp.is_success = True
    resp.parsed = claim_list
    client.list_claims.return_value = resp
    return client


def _make_ra_client(existing_records=None):
    """Mock ZmsRaClient with create/delete/list stubs."""
    client = MagicMock()
    client.list_observations.return_value = existing_records or []
    client.create_observation.return_value = {"id": "new-ra-id"}
    client.delete_observation.return_value = True
    return client


# ---------------------------------------------------------------------------
# _record_started tests
# ---------------------------------------------------------------------------


class TestRecordStarted:
    def test_future_record(self):
        rec = _make_record(
            "x",
            NOW + datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=2),
        )
        assert _record_started(rec, NOW) is False

    def test_past_record(self):
        rec = _make_record(
            "x",
            NOW - datetime.timedelta(hours=2),
            NOW - datetime.timedelta(hours=1),
        )
        assert _record_started(rec, NOW) is True

    def test_active_record(self):
        rec = _make_record(
            "x",
            NOW - datetime.timedelta(hours=1),
            NOW + datetime.timedelta(hours=1),
        )
        assert _record_started(rec, NOW) is True

    def test_record_starting_now(self):
        rec = _make_record("x", NOW, NOW + datetime.timedelta(hours=1))
        assert _record_started(rec, NOW) is True

    def test_missing_start(self):
        assert _record_started({}, NOW) is True


# ---------------------------------------------------------------------------
# _record_matches tests
# ---------------------------------------------------------------------------


class TestRecordMatches:
    def test_identical(self):
        obs = _make_obs("x")
        rec = _make_record("x", obs.start, obs.end)
        assert _record_matches(rec, obs) is True

    def test_time_changed(self):
        obs = _make_obs("x")
        rec = _make_record("x", obs.start, obs.end + datetime.timedelta(minutes=30))
        assert _record_matches(rec, obs) is False

    def test_freq_changed(self):
        obs = _make_obs("x", min_freq=1990, max_freq=1995)
        rec = _make_record("x", obs.start, obs.end, min_freq=1990, max_freq=2000)
        assert _record_matches(rec, obs) is False

    def test_missing_fields(self):
        obs = _make_obs("x")
        assert _record_matches({}, obs) is False


# ---------------------------------------------------------------------------
# reconcile tests
# ---------------------------------------------------------------------------


def _grant_covers_obs(obs):
    """Grant covering the observation's time window."""
    return _make_grant(
        "test-grant-id",
        obs.start - datetime.timedelta(hours=1),
        obs.end + datetime.timedelta(hours=1),
    )


class TestReconcile:
    def test_create_new_observation(self):
        """Source has an observation, zms-ra has nothing -> create."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        zmc_client = _make_zmc_client(grants=[_grant_covers_obs(obs)])
        ra_client = _make_ra_client(existing_records=[])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.created == 1
        assert stats.deleted == 0
        ra_client.create_observation.assert_called_once()

    def test_no_changes(self):
        """Source and zms-ra match -> no creates or deletes."""
        obs = _make_obs("obs-1")
        rec = _make_record("obs-1", obs.start, obs.end)
        source = _make_source([obs])
        zmc_client = _make_zmc_client(grants=[_grant_covers_obs(obs)])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.created == 0
        assert stats.deleted == 0
        assert stats.unchanged == 1
        ra_client.create_observation.assert_not_called()
        ra_client.delete_observation.assert_not_called()

    def test_unmatched_observation_skipped(self):
        """Source has an observation but no gcal grant covers it -> skip."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        zmc_client = _make_zmc_client(grants=[])  # no grants
        ra_client = _make_ra_client(existing_records=[])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.created == 0
        assert stats.unmatched == 1
        ra_client.create_observation.assert_not_called()

    def test_delete_cancelled_future_observation(self):
        """zms-ra has a future record that source no longer lists -> delete."""
        future_start = NOW + datetime.timedelta(hours=3)
        future_end = NOW + datetime.timedelta(hours=4)
        rec = _make_record("obs-cancelled", future_start, future_end)
        source = _make_source([])
        zmc_client = _make_zmc_client(grants=[])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.deleted == 1
        ra_client.delete_observation.assert_called_once_with("obs-cancelled")

    def test_keep_past_record_not_in_source(self):
        """zms-ra has a past record that source no longer lists -> keep it."""
        past_start = NOW - datetime.timedelta(hours=4)
        past_end = NOW - datetime.timedelta(hours=3)
        rec = _make_record("obs-done", past_start, past_end)
        source = _make_source([])
        zmc_client = _make_zmc_client(grants=[])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        ra_client.delete_observation.assert_not_called()

    def test_keep_active_record_not_in_source(self):
        """Active record that source no longer lists -> keep it."""
        active_start = NOW - datetime.timedelta(hours=1)
        active_end = NOW + datetime.timedelta(hours=1)
        rec = _make_record("obs-active", active_start, active_end)
        source = _make_source([])
        zmc_client = _make_zmc_client(grants=[])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.deleted == 0
        assert stats.unchanged == 1
        ra_client.delete_observation.assert_not_called()

    def test_recreate_changed_future_record(self):
        """Source has updated time for a future observation -> delete + recreate."""
        old_start = NOW + datetime.timedelta(hours=2)
        old_end = NOW + datetime.timedelta(hours=3)
        new_obs = _make_obs("obs-moved", start_offset_hours=4, end_offset_hours=5)
        rec = _make_record("obs-moved", old_start, old_end)
        source = _make_source([new_obs])
        zmc_client = _make_zmc_client(grants=[_grant_covers_obs(new_obs)])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.deleted == 1
        assert stats.created == 1

    def test_no_recreate_changed_active_record(self):
        """Source shows different time for an already-started record -> leave it."""
        active_start = NOW - datetime.timedelta(hours=1)
        active_end = NOW + datetime.timedelta(hours=1)
        new_obs = _make_obs("obs-active", start_offset_hours=-1, end_offset_hours=2)
        rec = _make_record("obs-active", active_start, active_end)
        source = _make_source([new_obs])
        zmc_client = _make_zmc_client(grants=[_grant_covers_obs(new_obs)])
        ra_client = _make_ra_client(existing_records=[rec])

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.deleted == 0
        assert stats.created == 0
        assert stats.unchanged == 1

    def test_create_error_doesnt_crash(self):
        """API error on create -> stats.errors incremented, loop continues."""
        obs = _make_obs("obs-1")
        source = _make_source([obs])
        zmc_client = _make_zmc_client(grants=[_grant_covers_obs(obs)])
        ra_client = _make_ra_client(existing_records=[])
        ra_client.create_observation.return_value = None

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.errors == 1
        assert stats.created == 0

    def test_delete_error_doesnt_crash(self):
        """API error on delete -> stats.errors incremented, loop continues."""
        future_start = NOW + datetime.timedelta(hours=3)
        future_end = NOW + datetime.timedelta(hours=4)
        rec = _make_record("obs-1", future_start, future_end)
        source = _make_source([])
        zmc_client = _make_zmc_client(grants=[])
        ra_client = _make_ra_client(existing_records=[rec])
        ra_client.delete_observation.return_value = False

        stats = reconcile(zmc_client, ra_client, source, "elem-1", now=NOW)

        assert stats.errors == 1
        assert stats.deleted == 0
