"""Entry point: poll loop that reconciles RA observations into zms-ra."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time

from zmsclient.zmc.client import ZmsZmcClient

from .config import Settings
from .ra_client import ZmsRaClient
from .reconciler import reconcile
from .report import generate_report, send_report
from .sources.ods import OdsSource

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

    sources = _load_sources(settings)
    if not sources:
        LOG.error("No sources configured, exiting")
        sys.exit(1)

    LOG.info(
        "Starting zms-ra-ingest: %d source(s), polling every %ds",
        len(sources),
        settings.poll_interval_seconds,
    )

    ra_client = ZmsRaClient(
        base_url=settings.ra_url,
        token=settings.token,
        verify_ssl=settings.ra_verify_ssl,
    )

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
                )
                LOG.info(
                    "Reconcile done: created=%d deleted=%d unchanged=%d "
                    "unmatched=%d errors=%d",
                    stats.created,
                    stats.deleted,
                    stats.unchanged,
                    stats.unmatched,
                    stats.errors,
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
