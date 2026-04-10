"""Wrapper that runs the zmsclient gcal grant/claim sync using our config."""

from __future__ import annotations

import logging
import sys

from zmsclient.tools.grant.gcal import get_events, get_grants, synch
from zmsclient.zmc.client import ZmsZmcClient

from .config import Settings

LOG = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class _Args:
    """Mimics the argparse namespace that gcal.synch() expects."""

    def __init__(self, settings: Settings) -> None:
        self.element_id = settings.element_id
        self.spectrum_id = settings.gcal_spectrum_id
        self.ext_id_prefix = "gcal-"
        self.min_freq = settings.gcal_min_freq
        self.max_freq = settings.gcal_max_freq
        self.max_power = 0.0
        self.priority = 1023
        self.as_grants = False
        self.delete = True
        self.update = True
        self.no_create = False
        self.impotent = False
        self.start_time = None
        self.end_time = None


def main():
    settings = Settings()
    logging.basicConfig(format=LOG_FORMAT, level=settings.log_level, stream=sys.stderr)

    if not settings.gcal_calendar_id or not settings.gcal_calendar_token:
        LOG.error("GCAL_CALENDAR_ID and GCAL_CALENDAR_TOKEN are required")
        sys.exit(1)

    import datetime

    start_time = datetime.datetime.now(datetime.UTC).replace(microsecond=0)

    client = ZmsZmcClient(
        base_url=settings.zmc_url,
        token=settings.token,
        verify_ssl=settings.gcal_verify_ssl,
    )

    # Parse filters
    import re

    filter_exc = []
    filter_inc = []
    if settings.gcal_filter_exc:
        filter_exc = [
            re.compile(p.strip()) for p in settings.gcal_filter_exc.split(",")
        ]
    if settings.gcal_filter_inc:
        filter_inc = [
            re.compile(p.strip()) for p in settings.gcal_filter_inc.split(",")
        ]

    LOG.info("Fetching existing grants from ZMC...")
    args = _Args(settings)
    grants = get_grants(
        client,
        settings.gcal_spectrum_id,
        settings.element_id,
        start_time,
        None,
        "gcal-",
        claim=True,
    )
    LOG.info("Found %d existing grants", len(grants))

    LOG.info("Fetching events from Google Calendar...")
    events = get_events(
        settings.gcal_calendar_id,
        settings.gcal_calendar_token,
        start_time,
        None,
        filter_exc,
        filter_inc,
    )
    LOG.info("Fetched %d events", len(events))

    synch(client, grants, events, args)
    LOG.info("Gcal sync complete")


if __name__ == "__main__":
    main()
