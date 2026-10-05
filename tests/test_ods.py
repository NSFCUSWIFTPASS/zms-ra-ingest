"""Tests for the ODS source parsing."""

import datetime
from unittest.mock import MagicMock, patch

from ra_ingest.sources.ods import OdsSource, _parse_ods_entry

PREFIX = "ods-hcro-"

SAMPLE_ODS_ENTRY = {
    "site_id": "ATA",
    "site_lat_deg": "40.817431",
    "site_lon_deg": "-121.470736",
    "site_el_m": "1019.222",
    "src_id": "ASP",
    "corr_integ_time_sec": 1,
    "src_ra_j2000_deg": 189.585,
    "src_dec_j2000_deg": -4.128,
    "src_start_utc": "2026-03-31T12:21:08",
    "src_end_utc": "2026-03-31T13:01:08",
    "slew_sec": 30,
    "trk_rate_dec_deg_per_sec": 0,
    "trk_rate_ra_deg_per_sec": 0,
    "freq_lower_hz": 1990000000,
    "freq_upper_hz": 1995000000,
    "version": "v1.0.0",
    "dish_diameter_m": 6.1,
    "subarray": 0,
}


class TestParseOdsEntry:
    def test_parses_all_fields(self):
        [obs] = _parse_ods_entry(SAMPLE_ODS_ENTRY, PREFIX)

        assert obs.min_freq_hz == 1990000000
        assert obs.max_freq_hz == 1995000000
        assert obs.name == "ASP (ATA)"
        assert obs.start == datetime.datetime(
            2026, 3, 31, 12, 21, 8, tzinfo=datetime.UTC
        )
        assert obs.end == datetime.datetime(2026, 3, 31, 13, 1, 8, tzinfo=datetime.UTC)

    def test_ext_id_is_composite(self):
        [obs] = _parse_ods_entry(SAMPLE_ODS_ENTRY, PREFIX)

        assert obs.ext_id == "ods-hcro-ATA:ASP:2026-03-31T12:21:08:0"

    def test_ext_id_includes_subarray(self):
        entry = {**SAMPLE_ODS_ENTRY, "subarray": 3}
        [obs] = _parse_ods_entry(entry, PREFIX)

        assert obs.ext_id.endswith(":3")

    def test_one_observation_per_actual_band(self):
        entry = {
            **SAMPLE_ODS_ENTRY,
            "freq_actual_hz": [
                {"freq_lower_hz": 1000000000, "freq_upper_hz": 1672000000},
                {"freq_lower_hz": 4200000000, "freq_upper_hz": 4872000000},
            ],
        }
        first, second = _parse_ods_entry(entry, PREFIX)

        assert (first.min_freq_hz, first.max_freq_hz) == (1000000000, 1672000000)
        assert (second.min_freq_hz, second.max_freq_hz) == (4200000000, 4872000000)
        assert first.ext_id == "ods-hcro-ATA:ASP:2026-03-31T12:21:08:0:b0"
        assert second.ext_id.endswith(":b1")
        assert first.name == "ASP (ATA):b0"
        assert first.start == second.start and first.target == second.target

    def test_empty_actual_bands_fall_back(self):
        entry = {**SAMPLE_ODS_ENTRY, "freq_actual_hz": []}
        [obs] = _parse_ods_entry(entry, PREFIX)

        assert obs.min_freq_hz == 1990000000

    def test_falls_back_to_avoidance_band(self):
        [obs] = _parse_ods_entry(SAMPLE_ODS_ENTRY, PREFIX)

        assert obs.min_freq_hz == 1990000000
        assert obs.max_freq_hz == 1995000000
        assert obs.ext_id == "ods-hcro-ATA:ASP:2026-03-31T12:21:08:0"

    def test_description_includes_metadata(self):
        [obs] = _parse_ods_entry(SAMPLE_ODS_ENTRY, PREFIX)

        assert "site=ATA" in obs.description
        assert "src=ASP" in obs.description
        assert "subarray=0" in obs.description

    def test_timestamps_are_utc(self):
        [obs] = _parse_ods_entry(SAMPLE_ODS_ENTRY, PREFIX)

        assert obs.start.tzinfo == datetime.UTC
        assert obs.end.tzinfo == datetime.UTC

    def test_missing_optional_fields(self):
        """src_id and subarray are optional in ODS spec."""
        entry = {**SAMPLE_ODS_ENTRY}
        del entry["src_id"]
        del entry["subarray"]

        [obs] = _parse_ods_entry(entry, PREFIX)

        assert obs.name == " (ATA)"
        assert obs.ext_id == "ods-hcro-ATA::2026-03-31T12:21:08:0"


class TestOdsSource:
    def test_properties(self):
        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com"
        )

        assert source.source_type == "ra-ods"
        assert source.source_name == "hcro"
        assert source.ext_id_prefix == "ods-hcro-"
        assert source.protect_started is True
        assert source.writes_observations is True
        assert source.priority == 1023
        assert source.correlate_repushes is True
        assert source.claim_lookback == datetime.timedelta(days=2)

    def test_priority_is_configurable(self):
        source = OdsSource(
            source_type="ra-ods",
            source_name="hcro",
            url="http://example.com",
            priority=500,
        )

        assert source.priority == 500

    def test_ext_id_prefix_folds_in_source_name(self):
        source = OdsSource(
            source_type="ra-ods", source_name="vla", url="http://example.com"
        )

        assert source.ext_id_prefix == "ods-vla-"

    @patch("ra_ingest.sources.ods.httpx.Client")
    def test_fetch_parses_response(self, mock_client_cls):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ods_data": [SAMPLE_ODS_ENTRY]}
        mock_resp.raise_for_status.return_value = None
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com/ods.json"
        )
        observations = source.fetch_observations()

        assert len(observations) == 1
        assert observations[0].min_freq_hz == 1990000000

    @patch("ra_ingest.sources.ods.httpx.Client")
    def test_fetch_empty_response(self, mock_client_cls):
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ods_data": []}
        mock_resp.raise_for_status.return_value = None
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com/ods.json"
        )
        observations = source.fetch_observations()

        assert len(observations) == 0

    @patch("ra_ingest.sources.ods.httpx.Client")
    def test_fetch_raises_on_http_error(self, mock_client_cls):
        import httpx
        import pytest

        from ra_ingest.sources.protocol import SourceFetchError

        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPError("connection failed")
        mock_client_cls.return_value = mock_client

        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com/ods.json"
        )

        with pytest.raises(SourceFetchError):
            source.fetch_observations()

    @patch("ra_ingest.sources.ods.httpx.Client")
    def test_fetch_skips_bad_entries(self, mock_client_cls):
        """One good entry, one bad -> returns only the good one."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "ods_data": [
                SAMPLE_ODS_ENTRY,
                {"bad": "entry"},
                {**SAMPLE_ODS_ENTRY, "freq_actual_hz": [{"wrong_keys": 1}]},
            ]
        }
        mock_resp.raise_for_status.return_value = None
        mock_client.get.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com/ods.json"
        )
        observations = source.fetch_observations()

        assert len(observations) == 1
