"""Tests for the ATA busy-claim guard. ZMC is mocked."""

import datetime
from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import (
    Claim,
    ClaimList,
    Constraint,
    Grant,
    GrantConstraint,
    Spectrum,
    SpectrumConstraint,
)

from ra_ingest.ata_guard import EXT_ID_PREFIX, LOOKBACK, guard

UTC = datetime.UTC
NOW = datetime.datetime(2026, 10, 13, 18, 0, 0, tzinfo=UTC)
HOLD = datetime.timedelta(minutes=30)
RELEASE_MARGIN = datetime.timedelta(minutes=1)
ELEMENT_ID = "elem-1"


def _spectrum(spec_id, name, *ranges):
    s = Spectrum(
        element_id=ELEMENT_ID,
        name=name,
        url="http://example.com",
        enabled=True,
        starts_at=NOW,
    )
    s.id = spec_id
    s.constraints = [
        SpectrumConstraint(
            constraint=Constraint(
                min_freq=lo, max_freq=hi, max_eirp=0.0, exclusive=False
            )
        )
        for lo, hi in ranges
    ]
    return s


ISM = _spectrum("spec-ism", "ISM-915", (902_000_000, 928_000_000))
LBAND = _spectrum("spec-l", "ATA L-band", (1_000_000_000, 2_000_000_000))


def _claim(spectrum_id, start_offset_min=-10, end_offset_min=20, denied=False):
    g = Grant(
        name="busy",
        description="",
        element_id=ELEMENT_ID,
        spectrum_id=spectrum_id,
        starts_at=NOW + datetime.timedelta(minutes=start_offset_min),
        expires_at=NOW + datetime.timedelta(minutes=end_offset_min),
        constraints=[
            GrantConstraint(
                constraint=Constraint(
                    min_freq=1, max_freq=2, max_eirp=0.0, exclusive=True
                )
            )
        ],
    )
    g.id = f"grant-{spectrum_id}"
    c = Claim(
        element_id=ELEMENT_ID,
        ext_id=f"{EXT_ID_PREFIX}{spectrum_id}-{g.starts_at:%Y%m%dT%H%M%SZ}",
        type="ata-status",
        source="ata",
        name="busy",
        grant=g,
    )
    c.id = f"claim-{spectrum_id}"
    if denied:
        c.denied_at = NOW
    return c


def _zmc(claims=()):
    claim_list = MagicMock(spec=ClaimList)
    claim_list.claims = list(claims)
    claim_list.pages = 1
    client = MagicMock()
    client.list_claims.return_value = MagicMock(
        is_success=True, parsed=claim_list, status_code=200
    )
    created = MagicMock(denied_at=None)
    client.create_claim.return_value = MagicMock(
        is_success=True, parsed=created, status_code=201
    )
    taken = {c.ext_id for c in claims}

    def create_claim(body, **_):
        # Like ZMC: a claim whose ext_id is taken is refused.
        if body.ext_id in taken:
            return MagicMock(is_success=False, status_code=409)
        return client.create_claim.return_value

    client.create_claim.side_effect = create_claim
    client.replace_grant.return_value = MagicMock(
        is_success=True, parsed=MagicMock(id="new-grant"), status_code=201
    )
    client.delete_claim.return_value = MagicMock(is_success=True, status_code=200)
    return client


def _picker(*spectrums):
    picker = MagicMock()
    picker.spectrums = list(spectrums)
    return picker


def _run(zmc, allowed, names=("ISM-915",), spectrums=(ISM, LBAND), now=NOW):
    return guard(
        zmc,
        ELEMENT_ID,
        _picker(*spectrums),
        list(names),
        allowed=allowed,
        hold=HOLD,
        priority=1023,
        release_margin=RELEASE_MARGIN,
        now=now,
    )


class TestInUse:
    def test_creates_claim_covering_spectrum(self):
        zmc = _zmc()

        stats = _run(zmc, allowed=False)

        assert stats.created == 1
        body = zmc.create_claim.call_args.kwargs["body"]
        assert body.ext_id == f"{EXT_ID_PREFIX}ISM-915-20261013T180000Z"
        grant = body.grant
        assert grant.spectrum_id == "spec-ism"
        assert grant.priority == 1023
        assert grant.starts_at == NOW
        assert grant.expires_at == NOW + HOLD
        [gc] = grant.constraints
        assert (gc.constraint.min_freq, gc.constraint.max_freq) == (
            902_000_000,
            928_000_000,
        )
        assert gc.constraint.exclusive is True

    def test_one_constraint_per_spectrum_range(self):
        split = _spectrum("spec-x", "Split", (100, 200), (300, 400))
        zmc = _zmc()

        _run(zmc, allowed=False, names=("Split",), spectrums=(split,))

        grant = zmc.create_claim.call_args.kwargs["body"].grant
        assert [
            (gc.constraint.min_freq, gc.constraint.max_freq) for gc in grant.constraints
        ] == [
            (100, 200),
            (300, 400),
        ]

    def test_live_claim_with_time_left_is_kept(self):
        zmc = _zmc([_claim("spec-ism", end_offset_min=20)])

        stats = _run(zmc, allowed=False)

        assert (stats.created, stats.extended, stats.released) == (0, 0, 0)
        zmc.replace_grant.assert_not_called()

    def test_claim_near_end_is_extended(self):
        claim = _claim("spec-ism", start_offset_min=-25, end_offset_min=5)
        zmc = _zmc([claim])

        stats = _run(zmc, allowed=False)

        assert stats.extended == 1
        kwargs = zmc.replace_grant.call_args.kwargs
        assert kwargs["grant_id"] == "grant-spec-ism"
        assert kwargs["body"].ext_id == claim.ext_id
        assert kwargs["body"].starts_at == claim.grant.starts_at
        assert kwargs["body"].expires_at == NOW + HOLD

    def test_ended_claim_is_kept_and_a_new_one_created(self):
        zmc = _zmc([_claim("spec-ism", start_offset_min=-60, end_offset_min=-5)])

        stats = _run(zmc, allowed=False)

        assert stats.created == 1
        assert (stats.released, stats.removed) == (0, 0)
        zmc.delete_claim.assert_not_called()
        zmc.replace_grant.assert_not_called()

    def test_next_busy_period_gets_its_own_claim(self):
        first = _zmc()
        _run(first, allowed=False)
        ended = _claim("spec-ism", start_offset_min=0, end_offset_min=60)
        ended.ext_id = first.create_claim.call_args.kwargs["body"].ext_id
        zmc = _zmc([ended])

        stats = _run(zmc, allowed=False, now=NOW + datetime.timedelta(hours=2))

        assert (stats.created, stats.errors) == (1, 0)

    def test_denied_claim_is_deleted_and_recreated(self):
        zmc = _zmc([_claim("spec-ism", denied=True)])

        stats = _run(zmc, allowed=False)

        assert stats.removed == 1
        assert stats.created == 1
        zmc.delete_claim.assert_called_once_with(claim_id="claim-spec-ism")

    def test_denied_create_counts_as_error(self):
        zmc = _zmc()
        zmc.create_claim.return_value.parsed.denied_at = NOW

        stats = _run(zmc, allowed=False)

        assert stats.created == 0
        assert stats.errors == 1


class TestNotInUse:
    def test_live_claim_is_ended_early(self):
        claim = _claim("spec-ism")
        zmc = _zmc([claim])

        stats = _run(zmc, allowed=True)

        assert stats.released == 1
        kwargs = zmc.replace_grant.call_args.kwargs
        assert kwargs["grant_id"] == "grant-spec-ism"
        assert kwargs["body"].ext_id == claim.ext_id
        assert kwargs["body"].starts_at == claim.grant.starts_at
        assert kwargs["body"].expires_at == NOW + RELEASE_MARGIN
        zmc.delete_claim.assert_not_called()
        zmc.create_claim.assert_not_called()

    def test_claim_already_ending_is_left_alone(self):
        zmc = _zmc([_claim("spec-ism", end_offset_min=0.5)])

        stats = _run(zmc, allowed=True)

        assert stats.released == 0
        zmc.replace_grant.assert_not_called()

    def test_nothing_to_do(self):
        zmc = _zmc()

        stats = _run(zmc, allowed=True)

        assert (stats.created, stats.released, stats.errors) == (0, 0, 0)


class TestSpectrumSelection:
    def test_star_guards_every_spectrum(self):
        zmc = _zmc()

        stats = _run(zmc, allowed=False, names=("*",))

        assert stats.created == 2
        ext_ids = {c.kwargs["body"].ext_id for c in zmc.create_claim.call_args_list}
        assert ext_ids == {
            f"{EXT_ID_PREFIX}ISM-915-20261013T180000Z",
            f"{EXT_ID_PREFIX}ATA-L-band-20261013T180000Z",
        }

    def test_unknown_spectrum_is_skipped(self):
        zmc = _zmc()

        stats = _run(zmc, allowed=False, names=("Nope",))

        assert stats.created == 0
        zmc.create_claim.assert_not_called()

    def test_claim_on_unguarded_spectrum_is_ended_early(self):
        zmc = _zmc([_claim("spec-l")])

        stats = _run(zmc, allowed=False, names=("ISM-915",))

        assert stats.released == 1
        assert zmc.replace_grant.call_args.kwargs["grant_id"] == "grant-spec-l"
        assert stats.created == 1


class TestHistory:
    def test_claim_query_is_bounded_by_lookback(self):
        zmc = _zmc()

        _run(zmc, allowed=True)

        assert zmc.list_claims.call_args.kwargs["start"] == NOW - LOOKBACK
