"""Tests for the spectrum manager bootstrap logic."""

from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import Spectrum as RealSpectrum
from zmsclient.zmc.v1.models import SpectrumList as RealSpectrumList

from ra_ingest.bootstrap import EXT_ID_PREFIX, SpectrumManager


def _make_client(existing_spectra=None):
    client = MagicMock()

    spectra = existing_spectra or []
    spectrum_list = MagicMock(spec=RealSpectrumList)
    spectrum_list.spectrum = spectra

    list_resp = MagicMock()
    list_resp.is_success = True
    list_resp.parsed = spectrum_list
    client.list_spectrum.return_value = list_resp

    created = MagicMock(spec=RealSpectrum)
    created.id = "new-spectrum-id"
    create_resp = MagicMock()
    create_resp.is_success = True
    create_resp.parsed = created
    client.create_spectrum.return_value = create_resp

    return client


def _make_existing_spectrum(band_name):
    s = MagicMock()
    s.id = f"spectrum-{band_name}"
    s.ext_id = f"{EXT_ID_PREFIX}:{band_name}"
    s.deleted_at = None
    return s


class TestSpectrumManager:
    def test_creates_spectrum_for_new_band(self):
        client = _make_client()
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(1500000000, 1600000000)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

    def test_reuses_existing_spectrum(self):
        existing = _make_existing_spectrum("L-band")
        client = _make_client(existing_spectra=[existing])
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(1500000000, 1600000000)

        assert result == "spectrum-L-band"
        client.create_spectrum.assert_not_called()

    def test_caches_after_create(self):
        client = _make_client()
        mgr = SpectrumManager(client, "elem-1")

        result1 = mgr.get_spectrum_id(1500000000, 1600000000)
        result2 = mgr.get_spectrum_id(1900000000, 1995000000)

        assert result1 == result2 == "new-spectrum-id"
        client.create_spectrum.assert_called_once()  # only created once

    def test_different_bands_get_different_spectra(self):
        client = _make_client()
        mgr = SpectrumManager(client, "elem-1")

        mgr.get_spectrum_id(1500000000, 1600000000)  # L-band
        mgr.get_spectrum_id(3000000000, 3500000000)  # S-band

        assert client.create_spectrum.call_count == 2

    def test_returns_none_for_unknown_band(self):
        client = _make_client()
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(50000000000, 60000000000)  # 50-60 GHz

        assert result is None
        client.create_spectrum.assert_not_called()

    def test_ignores_deleted_spectrum(self):
        deleted = _make_existing_spectrum("L-band")
        deleted.deleted_at = "2026-01-01T00:00:00Z"
        client = _make_client(existing_spectra=[deleted])
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(1500000000, 1600000000)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

    def test_ignores_non_ra_ingest_spectrum(self):
        other = MagicMock()
        other.id = "other-spectrum"
        other.ext_id = "something-else"
        other.deleted_at = None
        client = _make_client(existing_spectra=[other])
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(1500000000, 1600000000)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

    def test_create_failure_returns_none(self):
        client = _make_client()
        fail_resp = MagicMock()
        fail_resp.is_success = False
        fail_resp.status_code = 500
        client.create_spectrum.return_value = fail_resp
        mgr = SpectrumManager(client, "elem-1")

        result = mgr.get_spectrum_id(1500000000, 1600000000)

        assert result is None
