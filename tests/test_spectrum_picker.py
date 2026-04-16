"""Tests for SpectrumPicker: matches observations to the right spectrum."""

import datetime
from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import Constraint, Spectrum, SpectrumConstraint, SpectrumList

from ra_ingest.spectrum_picker import SpectrumPicker, _spectrum_bounds

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
# _spectrum_bounds
# ---------------------------------------------------------------------------


class TestSpectrumBounds:
    def test_single_constraint(self):
        s = _make_spectrum("s1", "ATA", (1_000_000_000, 2_000_000_000))
        assert _spectrum_bounds(s) == (1_000_000_000, 2_000_000_000)

    def test_multiple_constraints_spans_union(self):
        s = _make_spectrum(
            "s1",
            "multi",
            (1_000_000_000, 1_500_000_000),
            (1_800_000_000, 2_000_000_000),
        )
        # Current implementation uses union min/max -- matches if event fits the envelope
        assert _spectrum_bounds(s) == (1_000_000_000, 2_000_000_000)

    def test_no_constraints_returns_none(self):
        s = Spectrum(element_id="e", name="n", url="http://x", enabled=True, starts_at=STARTS_AT)
        s.constraints = []
        assert _spectrum_bounds(s) is None


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
