"""Decide whether the ATA is in use from its antenna status table.

Rule from HCRO operations (Oct 2026): an antenna is not in use when its
`source` is "AzEl", "Stay put", or NULL; any other source means it is
observing. An antenna missing from the table is offline and is ignored.
If no antenna has reported recently, or the table can't be read, the
array is treated as in use.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass

import pymysql

LOG = logging.getLogger(__name__)

PARKED_SOURCES = frozenset({"AzEl", "Stay put", None})

# antstatus gets a new row per antenna every ~32 s.
MAX_ROW_AGE = datetime.timedelta(minutes=2)

# Newest rows by primary key. 200 rows covers ~4 snapshots of 42 antennas,
# more than MAX_ROW_AGE, so a partly written snapshot doesn't hide an antenna.
QUERY = """
    SELECT ant, source, ts, UTC_TIMESTAMP() AS db_now
    FROM antstatus
    ORDER BY id DESC
    LIMIT 200
"""


@dataclass(frozen=True)
class AntennaRow:
    ant: str
    source: str | None
    ts: datetime.datetime


def transmission_allowed(rows: list[AntennaRow], now: datetime.datetime) -> bool:
    """True if every antenna that reported within MAX_ROW_AGE is parked.

    Uses each antenna's newest recent row. Antennas with no recent row are
    offline and ignored, unless none reported at all.
    """
    newest: dict[str, AntennaRow] = {}
    for row in rows:
        if now - row.ts > MAX_ROW_AGE:
            continue
        if row.ant not in newest or row.ts > newest[row.ant].ts:
            newest[row.ant] = row

    if not newest:
        LOG.warning(
            "No antenna status in the last %s; treating ATA as in use", MAX_ROW_AGE
        )
        return False

    in_use = sorted(a for a, r in newest.items() if r.source not in PARKED_SOURCES)
    if in_use:
        LOG.info("ATA in use: %s", ", ".join(in_use))
        return False
    return True


class AtaStatusReader:
    """Reads the antstatus table with a read-only MySQL account."""

    def __init__(
        self, host: str, port: int, user: str, password: str, database: str
    ) -> None:
        self._connect_args = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "connect_timeout": 10,
            "read_timeout": 10,
        }

    def read(self) -> tuple[list[AntennaRow], datetime.datetime | None]:
        """The newest antenna rows and the DB's current UTC time."""
        conn = pymysql.connect(**self._connect_args)
        try:
            with conn.cursor() as cur:
                cur.execute(QUERY)
                result = cur.fetchall()
        finally:
            conn.close()
        rows = [AntennaRow(ant=r[0], source=r[1], ts=r[2]) for r in result]
        db_now = result[0][3] if result else None
        return rows, db_now

    def transmission_allowed(self) -> bool:
        """transmission_allowed() on the live table; False if it can't be read."""
        try:
            rows, db_now = self.read()
        except Exception:
            LOG.exception("Failed to read ATA antenna status; treating ATA as in use")
            return False
        if db_now is None:
            LOG.warning("ATA antenna status table is empty; treating ATA as in use")
            return False
        return transmission_allowed(rows, db_now)
