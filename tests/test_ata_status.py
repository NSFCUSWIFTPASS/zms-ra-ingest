"""Tests for the ATA antenna status check."""

import datetime
from unittest.mock import MagicMock, patch

from ra_ingest.ata_status import AntennaRow, AtaStatusReader, transmission_allowed

NOW = datetime.datetime(2026, 10, 13, 18, 0, 0)


def _row(ant, source, age_s=10):
    return AntennaRow(
        ant=ant, source=source, ts=NOW - datetime.timedelta(seconds=age_s)
    )


class TestTransmissionAllowed:
    def test_all_parked(self):
        rows = [_row("1a", "AzEl"), _row("1b", "Stay put"), _row("1c", None)]

        assert transmission_allowed(rows, NOW) is True

    def test_sky_source_is_in_use(self):
        rows = [_row("1a", "AzEl"), _row("1b", "3c286")]

        assert transmission_allowed(rows, NOW) is False

    def test_fixed_position_name_is_in_use(self):
        rows = [_row("1a", "AZEL0.00,18.00")]

        assert transmission_allowed(rows, NOW) is False

    def test_no_rows_is_in_use(self):
        assert transmission_allowed([], NOW) is False

    def test_all_stale_is_in_use(self):
        rows = [_row("1a", "AzEl", age_s=121), _row("1b", "AzEl", age_s=300)]

        assert transmission_allowed(rows, NOW) is False

    def test_stale_antenna_is_ignored(self):
        # 1b stopped reporting while observing: offline, so it doesn't block.
        rows = [_row("1a", "AzEl"), _row("1b", "3c286", age_s=600)]

        assert transmission_allowed(rows, NOW) is True

    def test_newest_row_wins_parked(self):
        rows = [_row("1a", "AzEl", age_s=5), _row("1a", "3c286", age_s=40)]

        assert transmission_allowed(rows, NOW) is True

    def test_newest_row_wins_in_use(self):
        rows = [_row("1a", "3c286", age_s=5), _row("1a", "AzEl", age_s=40)]

        assert transmission_allowed(rows, NOW) is False


class TestAtaStatusReader:
    def _reader(self):
        return AtaStatusReader("db", 3306, "user", "pw", "grafanadata")

    @patch("ra_ingest.ata_status.pymysql.connect")
    def test_reads_rows_and_db_time(self, mock_connect):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("1a", "AzEl", NOW - datetime.timedelta(seconds=10), NOW),
            ("1b", None, NOW - datetime.timedelta(seconds=10), NOW),
        ]
        mock_connect.return_value.cursor.return_value.__enter__.return_value = cursor

        assert self._reader().transmission_allowed() is True
        mock_connect.return_value.close.assert_called_once()

    @patch("ra_ingest.ata_status.pymysql.connect")
    def test_db_error_is_in_use(self, mock_connect):
        mock_connect.side_effect = OSError("unreachable")

        assert self._reader().transmission_allowed() is False

    @patch("ra_ingest.ata_status.pymysql.connect")
    def test_empty_table_is_in_use(self, mock_connect):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        mock_connect.return_value.cursor.return_value.__enter__.return_value = cursor

        assert self._reader().transmission_allowed() is False
