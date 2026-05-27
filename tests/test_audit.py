"""Tests for the audit logger."""

import datetime
import json

from ra_ingest.audit import audit, configure


def test_disabled_when_no_path(tmp_path):
    configure("")
    # should silently no-op; no file created
    audit("nothing", k=1)


def test_writes_jsonl(tmp_path):
    log = tmp_path / "audit.jsonl"
    configure(str(log))
    audit("claim_created", ext_id="gcal-abc", count=3)
    audit("claim_deleted", ext_id="gcal-xyz")

    lines = log.read_text().strip().splitlines()
    assert len(lines) == 2

    r1 = json.loads(lines[0])
    assert r1["event"] == "claim_created"
    assert r1["ext_id"] == "gcal-abc"
    assert r1["count"] == 3
    assert "ts" in r1

    r2 = json.loads(lines[1])
    assert r2["event"] == "claim_deleted"
    assert r2["ext_id"] == "gcal-xyz"


def test_serializes_datetimes(tmp_path):
    log = tmp_path / "audit.jsonl"
    configure(str(log))
    audit(
        "raobs_created",
        ext_id="ATA:ASP:2026-05-22T06:00:00:0",
        start=datetime.datetime(2026, 5, 22, 6, 0, 0, tzinfo=datetime.UTC),
    )
    r = json.loads(log.read_text().strip())
    assert "2026-05-22" in r["start"]


def test_reconfigure_does_not_duplicate_handlers(tmp_path):
    log = tmp_path / "audit.jsonl"
    configure(str(log))
    configure(str(log))  # call twice
    audit("once")
    assert len(log.read_text().strip().splitlines()) == 1
