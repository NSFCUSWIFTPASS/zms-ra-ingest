"""Tests for the spectrum bootstrap logic."""

from unittest.mock import MagicMock

from zmsclient.zmc.v1.models import Spectrum as RealSpectrum
from zmsclient.zmc.v1.models import SpectrumList as RealSpectrumList

from ra_ingest.bootstrap import SPECTRUM_EXT_ID_PREFIX, ensure_spectrum


def _make_settings(
    element_id="elem-1",
    spectrum_name="ATA L-band",
    min_freq=1000000000,
    max_freq=2000000000,
):
    settings = MagicMock()
    settings.element_id = element_id
    settings.spectrum_name = spectrum_name
    settings.spectrum_min_freq_hz = min_freq
    settings.spectrum_max_freq_hz = max_freq
    return settings


def _make_client(existing_spectrum=None):
    client = MagicMock()

    # list_spectrum response
    spectra = existing_spectrum or []
    spectrum_list = MagicMock(spec=RealSpectrumList)
    spectrum_list.spectrum = spectra

    list_resp = MagicMock()
    list_resp.is_success = True
    list_resp.parsed = spectrum_list
    client.list_spectrum.return_value = list_resp

    # create_spectrum response
    created = MagicMock(spec=RealSpectrum)
    created.id = "new-spectrum-id"
    create_resp = MagicMock()
    create_resp.is_success = True
    create_resp.parsed = created
    client.create_spectrum.return_value = create_resp

    return client


class TestEnsureSpectrum:
    def test_creates_when_none_exists(self):
        """No existing spectrum -> creates one."""
        client = _make_client(existing_spectrum=[])
        settings = _make_settings()

        result = ensure_spectrum(client, settings)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

        # Verify the spectrum object passed to create
        call_kwargs = client.create_spectrum.call_args
        spectrum = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert spectrum.element_id == "elem-1"
        assert spectrum.name == "ATA L-band"
        assert spectrum.starts_at is not None

    def test_finds_existing_by_ext_id(self):
        """Existing spectrum with matching ext_id -> reuses it."""
        existing = MagicMock()
        existing.id = "existing-spectrum-id"
        existing.name = "ATA L-band"
        existing.ext_id = f"{SPECTRUM_EXT_ID_PREFIX}:ATA L-band"
        existing.deleted_at = None

        client = _make_client(existing_spectrum=[existing])
        settings = _make_settings()

        result = ensure_spectrum(client, settings)

        assert result == "existing-spectrum-id"
        client.create_spectrum.assert_not_called()

    def test_ignores_deleted_spectrum(self):
        """Deleted spectrum with matching ext_id -> creates new one."""
        deleted = MagicMock()
        deleted.id = "deleted-spectrum-id"
        deleted.name = "ATA L-band"
        deleted.ext_id = f"{SPECTRUM_EXT_ID_PREFIX}:ATA L-band"
        deleted.deleted_at = "2026-01-01T00:00:00Z"

        client = _make_client(existing_spectrum=[deleted])
        settings = _make_settings()

        result = ensure_spectrum(client, settings)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

    def test_ignores_different_ext_id(self):
        """Existing spectrum with different ext_id -> creates new one."""
        other = MagicMock()
        other.id = "other-spectrum-id"
        other.name = "ISM-915"
        other.ext_id = "something-else"
        other.deleted_at = None

        client = _make_client(existing_spectrum=[other])
        settings = _make_settings()

        result = ensure_spectrum(client, settings)

        assert result == "new-spectrum-id"
        client.create_spectrum.assert_called_once()

    def test_raises_on_create_failure(self):
        """Create fails -> raises RuntimeError."""
        client = _make_client(existing_spectrum=[])
        fail_resp = MagicMock()
        fail_resp.is_success = False
        fail_resp.status_code = 500
        client.create_spectrum.return_value = fail_resp

        settings = _make_settings()

        import pytest

        with pytest.raises(RuntimeError, match="500"):
            ensure_spectrum(client, settings)

    def test_ext_id_includes_spectrum_name(self):
        """ext_id is derived from spectrum_name for uniqueness."""
        client = _make_client(existing_spectrum=[])
        settings = _make_settings(spectrum_name="Custom Band")

        ensure_spectrum(client, settings)

        call_kwargs = client.create_spectrum.call_args
        spectrum = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        assert spectrum.ext_id == f"{SPECTRUM_EXT_ID_PREFIX}:Custom Band"
