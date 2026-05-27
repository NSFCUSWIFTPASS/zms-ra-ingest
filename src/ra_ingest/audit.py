"""Audit logger for ra-ingest artifacts.

Distinct from the operational stderr logger. Writes one JSON line per
artifact-relevant event to a file specified by RA_INGEST_AUDIT_LOG_PATH.
Designed for durable artifact capture (e.g. PDR evidence collection).
Disabled if no path is configured.
"""

from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
from typing import Any

# Use a sibling logger name (not __name__ == "ra_ingest.audit") so our own
# diagnostic chatter doesn't write into the audit file.
LOG = logging.getLogger("ra_ingest.audit_setup")

_AUDIT_LOGGER_NAME = "ra_ingest.audit"
_audit_logger: logging.Logger | None = None


def configure(path: str | None) -> None:
    """Wire the audit logger to a file at `path`. No-op if path is empty/None."""
    global _audit_logger

    if not path:
        _audit_logger = None
        return

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    audit = logging.getLogger(_AUDIT_LOGGER_NAME)
    audit.setLevel(logging.INFO)
    audit.propagate = False
    for h in list(audit.handlers):
        audit.removeHandler(h)
    handler = logging.FileHandler(str(p))
    handler.setFormatter(logging.Formatter("%(message)s"))
    audit.addHandler(handler)
    _audit_logger = audit
    LOG.info("Audit logger writing to %s", p)


def audit(event: str, **fields: Any) -> None:
    """Emit one JSON line to the audit log if configured."""
    if _audit_logger is None:
        return
    record: dict[str, Any] = {
        "ts": datetime.datetime.now(datetime.UTC).isoformat(),
        "event": event,
        **fields,
    }
    _audit_logger.info(json.dumps(record, default=str))
