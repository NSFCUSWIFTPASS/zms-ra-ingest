"""Tests for the GcalSource summary parsing + event conversion."""

import datetime
from unittest.mock import patch

import pytest

from ra_ingest.sources.gcal import (
    GcalSource,
    _event_to_observation,
    _parse_freq,
)

UTC = datetime.UTC
NOW = datetime.datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# _parse_freq
# ---------------------------------------------------------------------------


class TestParseFreq:
    def test_hcro_transmission_format(self):
        summary = (
            "Activity Title: HCRO Transmission\n"
            "Start Date: 04/14/2026\n"
            "Start Time: 10:00\n"
            "End Date: 04/14/2026\n"
            "End Time: 16:00\n"
            "Center Frequency: 915 (MHz)\n"
            "Bandwidth: 26 MHz"
        )
        # 915 ± 13 MHz -> 902-928 MHz
        assert _parse_freq(summary) == (902_000_000, 928_000_000)

    def test_missing_freq_returns_none(self):
        assert _parse_freq("[ASP] Year 2 Session 225") is None

    def test_fractional_values(self):
        assert _parse_freq("Center Frequency: 1420.5 MHz\nBandwidth: 1.5 MHz") == (
            1_419_750_000,  # 1420.5 - 0.75
            1_421_250_000,  # 1420.5 + 0.75
        )

    def test_only_center_no_bandwidth_returns_none(self):
        assert _parse_freq("Center Frequency: 915 MHz") is None

    def test_only_bandwidth_no_center_returns_none(self):
        assert _parse_freq("Bandwidth: 26 MHz") is None

    def test_case_insensitive(self):
        assert _parse_freq("center frequency: 915 mhz\nbandwidth: 26 mhz") == (
            902_000_000,
            928_000_000,
        )


# ---------------------------------------------------------------------------
# _event_to_observation
# ---------------------------------------------------------------------------


def _mkevent(**kwargs):
    base = {
        "id": "evt-1",
        "summary": "[ASP] Year 2 Session 225",
        "startDateTime": NOW + datetime.timedelta(hours=1),
        "endDateTime": NOW + datetime.timedelta(hours=2),
    }
    base.update(kwargs)
    return base


class TestEventToObservation:
    def test_simple_event(self):
        event = _mkevent()
        obs = _event_to_observation(event, "gcal-", 100, 200)
        assert obs is not None
        assert obs.ext_id == "gcal-evt-1"
        assert obs.name == "[ASP] Year 2 Session 225"
        assert obs.min_freq_hz == 100
        assert obs.max_freq_hz == 200

    def test_hcro_transmission_event(self):
        event = _mkevent(
            id="hcro-1",
            summary=(
                "Activity Title: HCRO Transmission\n"
                "Center Frequency: 915 (MHz)\n"
                "Bandwidth: 26 MHz"
            ),
        )
        obs = _event_to_observation(event, "gcal-", 1000, 2000)
        assert obs is not None
        assert obs.ext_id == "gcal-hcro-1"
        # Name should come from Activity Title
        assert obs.name == "HCRO Transmission"
        assert obs.min_freq_hz == 902_000_000
        assert obs.max_freq_hz == 928_000_000

    def test_hcro_transmission_runon_single_line(self):
        # Real calendar events pack every field onto one line with no
        # delimiter between the title and the next label ("TransmissionStart").
        # The name must stop at "Start Date:", not swallow the whole blob.
        event = _mkevent(
            id="hcro-runon",
            summary=(
                "Activity Title: HCRO TransmissionStart Date: 05/28/2026 "
                "Start Time: 10:00 End Date: 05/28/2026 End Time: 16:00 "
                "Center Frequency: 915 (MHz) Bandwidth: 26 MHz"
            ),
        )
        obs = _event_to_observation(event, "gcal-", 1000, 2000)
        assert obs is not None
        assert obs.name == "HCRO Transmission"
        assert obs.min_freq_hz == 902_000_000
        assert obs.max_freq_hz == 928_000_000

    def test_activity_title_with_no_trailing_fields(self):
        # A clean "Activity Title: X" with no following labels still resolves
        # to X (the end-of-string branch of the title regex).
        event = _mkevent(id="clean", summary="Activity Title: Just A Title")
        obs = _event_to_observation(event, "gcal-", 0, 0)
        assert obs is not None
        assert obs.name == "Just A Title"

    def test_freq_from_description_fallback(self):
        # Observatory sets only a plain title; freq lives in the description.
        event = _mkevent(
            id="desc-freq",
            summary="[ASP] Year 2 Session 300",
            description="Center Frequency: 915 (MHz)\nBandwidth: 26 MHz",
        )
        obs = _event_to_observation(event, "gcal-", 1000, 2000)
        assert obs is not None
        assert obs.name == "[ASP] Year 2 Session 300"  # name from native title
        assert obs.min_freq_hz == 902_000_000  # freq from description
        assert obs.max_freq_hz == 928_000_000

    def test_summary_freq_wins_over_description(self):
        event = _mkevent(
            id="both",
            summary="Center Frequency: 915 (MHz)\nBandwidth: 26 MHz",
            description="Center Frequency: 1420 (MHz)\nBandwidth: 10 MHz",
        )
        obs = _event_to_observation(event, "gcal-", 0, 0)
        assert obs is not None
        assert obs.min_freq_hz == 902_000_000  # title wins, not description

    def test_no_id_returns_none(self):
        event = _mkevent(id=None)
        assert _event_to_observation(event, "gcal-", 0, 0) is None

    def test_no_times_returns_none(self):
        event = _mkevent(startDateTime=None, endDateTime=None)
        assert _event_to_observation(event, "gcal-", 0, 0) is None

    def test_ext_id_uses_prefix(self):
        event = _mkevent(id="abc")
        obs = _event_to_observation(event, "gcal-", 0, 0)
        assert obs is not None
        assert obs.ext_id == "gcal-abc"

        obs2 = _event_to_observation(event, "myprefix-", 0, 0)
        assert obs2 is not None
        assert obs2.ext_id == "myprefix-abc"


# ---------------------------------------------------------------------------
# GcalSource integration
# ---------------------------------------------------------------------------


class TestGcalSource:
    def test_properties(self):
        src = GcalSource(
            source_type="gcal",
            source_name="ata",
            calendar_id="cal-id",
            calendar_token="tok",
            default_min_freq_hz=100,
            default_max_freq_hz=200,
        )
        assert src.source_type == "gcal"
        assert src.source_name == "ata"
        assert src.ext_id_prefix == "gcal-"

    def test_custom_ext_id_prefix(self):
        src = GcalSource(
            source_type="gcal",
            source_name="ata",
            calendar_id="cal-id",
            calendar_token="tok",
            default_min_freq_hz=0,
            default_max_freq_hz=0,
            ext_id_prefix="ata-",
        )
        assert src.ext_id_prefix == "ata-"

    def test_fetch_converts_events(self):
        src = GcalSource(
            source_type="gcal",
            source_name="ata",
            calendar_id="cal-id",
            calendar_token="tok",
            default_min_freq_hz=1_000_000_000,
            default_max_freq_hz=2_000_000_000,
        )
        fake_events = [
            _mkevent(id="a", summary="regular"),
            _mkevent(
                id="b",
                summary=(
                    "Activity Title: HCRO Transmission\n"
                    "Center Frequency: 915 (MHz)\nBandwidth: 26 MHz"
                ),
            ),
        ]
        with patch("ra_ingest.sources.gcal.get_events", return_value=fake_events):
            obs_list = src.fetch_observations()

        assert len(obs_list) == 2
        assert obs_list[0].ext_id == "gcal-a"
        assert obs_list[0].min_freq_hz == 1_000_000_000  # default
        assert obs_list[1].ext_id == "gcal-b"
        assert obs_list[1].min_freq_hz == 902_000_000  # parsed

    def test_fetch_raises_on_network_error(self):
        from ra_ingest.sources.protocol import SourceFetchError

        src = GcalSource(
            source_type="gcal",
            source_name="ata",
            calendar_id="cal-id",
            calendar_token="tok",
            default_min_freq_hz=0,
            default_max_freq_hz=0,
        )
        with patch(
            "ra_ingest.sources.gcal.get_events", side_effect=RuntimeError("boom")
        ):
            with pytest.raises(SourceFetchError):
                src.fetch_observations()

    def test_fetch_raises_on_api_non_200(self):
        """David's get_events sys.exit()s on non-200; we convert to SourceFetchError."""
        from ra_ingest.sources.protocol import SourceFetchError

        src = GcalSource(
            source_type="gcal",
            source_name="ata",
            calendar_id="cal-id",
            calendar_token="tok",
            default_min_freq_hz=0,
            default_max_freq_hz=0,
        )
        with patch("ra_ingest.sources.gcal.get_events", side_effect=SystemExit("403")):
            with pytest.raises(SourceFetchError):
                src.fetch_observations()
