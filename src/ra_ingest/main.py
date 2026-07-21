"""Entry point: poll loop that reconciles RA observations into zms-ra."""

from __future__ import annotations

import argparse
import json
import logging
import re
import signal
import sys
import time

from zmsclient.zmc.client import ZmsZmcClient

from .config import Settings
from .ra_client import ZmsRaClient
from .reconciler import reconcile
from .report import generate_report, send_report
from .sources.gcal import GcalSource
from .sources.ods import OdsSource
from .spectrum_picker import SpectrumPicker

SOURCE_REGISTRY: dict[str, type] = {
    "ods": OdsSource,
}

LOG = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    LOG.info("Received signal %s, shutting down", signum)
    _shutdown = True


def _build_gcal_source(settings: Settings) -> GcalSource:
    """Build a GcalSource from settings. Required; Pydantic enforces config presence."""
    filter_exc: list[re.Pattern] = []
    filter_inc: list[re.Pattern] = []
    if settings.gcal_filter_exc:
        filter_exc = [
            re.compile(p.strip()) for p in settings.gcal_filter_exc.split(",")
        ]
    if settings.gcal_filter_inc:
        filter_inc = [
            re.compile(p.strip()) for p in settings.gcal_filter_inc.split(",")
        ]

    return GcalSource(
        source_type="gcal",
        source_name="gcal",
        calendar_id=settings.gcal_calendar_id,
        calendar_token=settings.gcal_calendar_token,
        lookahead_days=settings.gcal_lookahead_days,
        filter_exc=filter_exc,
        filter_inc=filter_inc,
    )


def _load_sources(settings: Settings) -> list:
    """Load source configurations from JSON file."""
    try:
        with open(settings.sources_config) as f:
            raw = json.load(f)
    except FileNotFoundError:
        LOG.error("Sources config not found: %s", settings.sources_config)
        sys.exit(1)

    sources = []
    for entry in raw:
        kind = entry.pop("kind", None)
        source_type = entry.pop("type", None)
        source_name = entry.pop("source", None)
        if not kind or not source_type or not source_name:
            LOG.error("Source entry missing kind, type, or source: %r", entry)
            continue
        cls = SOURCE_REGISTRY.get(kind)
        if cls:
            sources.append(
                cls(source_type=source_type, source_name=source_name, **entry)
            )
        else:
            LOG.warning("Unknown source kind %r, skipping", kind)
    return sources


def main():
    parser = argparse.ArgumentParser(description="ZMS RA Facility Ingest Service")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Send a daily report email and exit (no poll loop)",
    )
    args = parser.parse_args()

    settings = Settings()

    logging.basicConfig(format=LOG_FORMAT, level=settings.log_level, stream=sys.stderr)
    zmc_client = ZmsZmcClient(
        base_url=settings.zmc_url,
        token=settings.token,
    )

    if args.report:
        body = generate_report(zmc_client, settings.element_id)
        send_report(settings, body)
        return

    # gcal and ODS are peers now: each source mints its own grants through the
    # one reconcile loop. No ordering dependency between them.
    sources = [_build_gcal_source(settings), *_load_sources(settings)]

    LOG.info(
        "Starting zms-ra-ingest: %d source(s), gcal lookahead %dd, polling every %ds",
        len(sources),
        settings.gcal_lookahead_days,
        settings.poll_interval_seconds,
    )

    ra_client = ZmsRaClient(
        base_url=settings.ra_url,
        token=settings.token,
        verify_ssl=settings.ra_verify_ssl,
    )

    spectrum_picker = SpectrumPicker(zmc_client, settings.element_id)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while not _shutdown:
        for source in sources:
            LOG.info(
                "Reconciling source: type=%s source=%s",
                source.source_type,
                source.source_name,
            )
            try:
                stats = reconcile(
                    zmc_client=zmc_client,
                    ra_client=ra_client,
                    source=source,
                    element_id=settings.element_id,
                    picker=spectrum_picker,
                )
                LOG.info(
                    "Reconcile done (%s): created=%d deleted=%d unchanged=%d "
                    "errors=%d ra_created=%d ra_deleted=%d",
                    source.source_type,
                    stats.created,
                    stats.deleted,
                    stats.unchanged,
                    stats.errors,
                    stats.ra_created,
                    stats.ra_deleted,
                )
            except Exception:
                LOG.exception("Error reconciling source type=%s", source.source_type)

        # Sleep in small increments so we can respond to signals
        for _ in range(settings.poll_interval_seconds):
            if _shutdown:
                break
            time.sleep(1)

    LOG.info("Shutdown complete")


if __name__ == "__main__":
    main()
