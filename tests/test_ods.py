"""Tests for the ODS source parsing."""

import datetime
from unittest.mock import MagicMock, patch

from ra_ingest.sources.ods import OdsSource, _parse_ods_entry

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
        obs = _parse_ods_entry(SAMPLE_ODS_ENTRY)

        assert obs.min_freq_hz == 1990000000
        assert obs.max_freq_hz == 1995000000
        assert obs.name == "ASP (ATA)"
        assert obs.start == datetime.datetime(
            2026, 3, 31, 12, 21, 8, tzinfo=datetime.UTC
        )
        assert obs.end == datetime.datetime(2026, 3, 31, 13, 1, 8, tzinfo=datetime.UTC)

    def test_ext_id_is_composite(self):
        obs = _parse_ods_entry(SAMPLE_ODS_ENTRY)

        assert obs.ext_id == "ATA:ASP:2026-03-31T12:21:08:0"

    def test_ext_id_includes_subarray(self):
        entry = {**SAMPLE_ODS_ENTRY, "subarray": 3}
        obs = _parse_ods_entry(entry)

        assert obs.ext_id.endswith(":3")

    def test_description_includes_metadata(self):
        obs = _parse_ods_entry(SAMPLE_ODS_ENTRY)

        assert "site=ATA" in obs.description
        assert "src=ASP" in obs.description
        assert "subarray=0" in obs.description

    def test_timestamps_are_utc(self):
        obs = _parse_ods_entry(SAMPLE_ODS_ENTRY)

        assert obs.start.tzinfo == datetime.UTC
        assert obs.end.tzinfo == datetime.UTC

    def test_missing_optional_fields(self):
        """src_id and subarray are optional in ODS spec."""
        entry = {**SAMPLE_ODS_ENTRY}
        del entry["src_id"]
        del entry["subarray"]

        obs = _parse_ods_entry(entry)

        assert obs.name == " (ATA)"
        assert obs.ext_id == "ATA::2026-03-31T12:21:08:0"


class TestOdsSource:
    def test_properties(self):
        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com"
        )

        assert source.source_type == "ra-ods"
        assert source.source_name == "hcro"
        assert source.priority == 1023

    def test_custom_priority(self):
        source = OdsSource(
            source_type="ra-ods",
            source_name="hcro",
            url="http://example.com",
            priority=500,
        )

        assert source.priority == 500

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
    def test_fetch_handles_http_error(self, mock_client_cls):
        import httpx

        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPError("connection failed")
        mock_client_cls.return_value = mock_client

        source = OdsSource(
            source_type="ra-ods", source_name="hcro", url="http://example.com/ods.json"
        )
        observations = source.fetch_observations()

        assert len(observations) == 0

    @patch("ra_ingest.sources.ods.httpx.Client")
    def test_fetch_skips_bad_entries(self, mock_client_cls):
        """One good entry, one bad -> returns only the good one."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "ods_data": [
                SAMPLE_ODS_ENTRY,
                {"bad": "entry"},
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
