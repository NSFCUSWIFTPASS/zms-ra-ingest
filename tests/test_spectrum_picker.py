"""Tests for SpectrumPicker: matches observations to the right spectrum."""

import datetime
from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import (
    Constraint,
    Spectrum,
    SpectrumConstraint,
    SpectrumList,
)

from ra_ingest.spectrum_picker import SpectrumPicker

STARTS_AT = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


def _make_spectrum(spec_id, name, *ranges):
    """Build a Spectrum with one or more freq constraints (min_hz, max_hz) tuples."""
    constraints = []
    for min_hz, max_hz in ranges:
        c = Constraint(min_freq=min_hz, max_freq=max_hz, max_eirp=0.0, exclusive=True)
        constraints.append(SpectrumConstraint(constraint=c))
    s = Spectrum(
        element_id="elem-1",
        name=name,
        url="http://example.com",
        enabled=True,
        starts_at=STARTS_AT,
    )
    s.id = spec_id
    s.constraints = constraints
    return s


def _make_client(spectrums):
    spec_list = MagicMock(spec=SpectrumList)
    spec_list.spectrum = spectrums
    spec_list.pages = 1
    resp = MagicMock(is_success=True, parsed=spec_list, status_code=200)
    client = MagicMock()
    client.list_spectrum.return_value = resp
    return client


# ---------------------------------------------------------------------------
# SpectrumPicker.pick
# ---------------------------------------------------------------------------


class TestPick:
    def test_picks_narrowest_matching(self):
        """When multiple spectrums cover an observation, pick the narrowest."""
        ism = _make_spectrum("ism", "ISM-915", (902_000_000, 928_000_000))
        ata = _make_spectrum("ata", "ATA L-band", (1_000_000_000, 2_000_000_000))
        wide = _make_spectrum("wide", "Wide", (100_000_000, 6_000_000_000))

        client = _make_client([ism, ata, wide])
        picker = SpectrumPicker(client, "elem-1")
        picker.refresh()

        # Event in ISM band -- should pick ISM (narrower than Wide)
        result = picker.pick(910_000_000, 920_000_000)
        assert result is not None
        assert result.id == "ism"

        # Event in ATA band -- should pick ATA (narrower than Wide)
        result = picker.pick(1_400_000_000, 1_420_000_000)
        assert result is not None
        assert result.id == "ata"

        # Event outside ISM/ATA but inside Wide
        result = picker.pick(3_000_000_000, 3_500_000_000)
        assert result is not None
        assert result.id == "wide"

    def test_no_match_returns_none(self):
        """Event freq outside all spectrums -> None."""
        ata = _make_spectrum("ata", "ATA", (1_000_000_000, 2_000_000_000))
        client = _make_client([ata])
        picker = SpectrumPicker(client, "elem-1")
        picker.refresh()

        # Below ATA range
        assert picker.pick(500_000_000, 600_000_000) is None
        # Above ATA range
        assert picker.pick(3_000_000_000, 4_000_000_000) is None
        # Straddles ATA boundary
        assert picker.pick(900_000_000, 1_500_000_000) is None

    def test_empty_before_refresh(self):
        """Picker returns None before refresh is called."""
        client = _make_client([])
        picker = SpectrumPicker(client, "elem-1")
        assert picker.pick(1_000_000_000, 2_000_000_000) is None

    def test_exact_bounds_match(self):
        """Event that exactly matches spectrum bounds is a match."""
        ata = _make_spectrum("ata", "ATA", (1_000_000_000, 2_000_000_000))
        client = _make_client([ata])
        picker = SpectrumPicker(client, "elem-1")
        picker.refresh()
        result = picker.pick(1_000_000_000, 2_000_000_000)
        assert result is not None
        assert result.id == "ata"

    def test_gap_between_constraints_not_covered(self):
        """A spectrum with two disjoint constraints does NOT cover the gap.

        Constraints 1000-1200 and 1500-1700; an observation at 1300-1400 sits
        in the gap. The old envelope (1000-1700) wrongly matched it; matching
        per-constraint correctly returns None.
        """
        gappy = _make_spectrum(
            "gappy",
            "two-band",
            (1_000_000_000, 1_200_000_000),
            (1_500_000_000, 1_700_000_000),
        )
        client = _make_client([gappy])
        picker = SpectrumPicker(client, "elem-1")
        picker.refresh()

        # In the gap between the two constraints -> no cover.
        assert picker.pick(1_300_000_000, 1_400_000_000) is None
        # Inside the lower constraint -> matches.
        assert picker.pick(1_050_000_000, 1_150_000_000) is gappy
        # Inside the upper constraint -> matches.
        assert picker.pick(1_550_000_000, 1_650_000_000) is gappy
