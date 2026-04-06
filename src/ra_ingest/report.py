"""Daily report: queries claims from ZMS and sends an email summary."""

from __future__ import annotations

import datetime
import logging
import smtplib
from email.mime.text import MIMEText
from typing import cast

from zmsclient.zmc.client import ZmsZmcClient
from zmsclient.zmc.v1.models import Claim, ClaimList

from .config import Settings

LOG = logging.getLogger(__name__)


def generate_report(client: ZmsZmcClient, element_id: str) -> str:
    """Query ZMS for today's claims and format a text summary."""
    now = datetime.datetime.now(datetime.UTC)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + datetime.timedelta(days=1)

    # Fetch all claims for this element, include deleted so we see cancellations
    claims: list[Claim] = []
    page = 1
    while True:
        resp = client.list_claims(
            element_id=element_id,
            include_deleted=True,
            include_deleted_after=day_start,
            page=page,
            items_per_page=100,
            x_api_elaborate="True",
        )
        if not resp.is_success or not isinstance(resp.parsed, ClaimList):
            LOG.error("Failed to list claims (page %d): %s", page, resp.status_code)
            break
        claim_list = cast(ClaimList, resp.parsed)
        claims.extend(claim_list.claims)
        if page >= claim_list.pages:
            break
        page += 1

    # Categorize
    active = []
    created_today = []
    deleted_today = []

    for claim in claims:
        grant = claim.grant
        if grant is None:
            continue

        # Check if this claim's observation overlaps today
        starts = grant.starts_at
        expires = grant.expires_at
        overlaps_today = starts and expires and starts < day_end and expires > day_start

        if claim.deleted_at and claim.deleted_at >= day_start:
            deleted_today.append(claim)
        elif claim.created_at and claim.created_at >= day_start:
            created_today.append(claim)

        if overlaps_today and not claim.deleted_at:
            active.append(claim)

    # Format
    lines = [
        f"RA Facility Observations - {now.strftime('%Y-%m-%d')}",
        f"{'=' * 50}",
        "",
        f"Active claims today: {len(active)}",
    ]

    for claim in active:
        grant = claim.grant
        freq_str = ""
        if grant and grant.constraints and len(grant.constraints) > 0:
            c = grant.constraints[0].constraint
            if c:
                freq_str = f" ({c.min_freq / 1e6:.0f}-{c.max_freq / 1e6:.0f} MHz)"

        time_str = ""
        if grant:
            start = grant.starts_at.strftime("%H:%M") if grant.starts_at else "?"
            end = grant.expires_at.strftime("%H:%M") if grant.expires_at else "?"
            time_str = f" {start}-{end} UTC"

        lines.append(f"  - {claim.name}{freq_str}{time_str}")
        lines.append(
            f"    type={claim.type} source={claim.source} ext_id={claim.ext_id}"
        )

    lines.append("")
    lines.append(f"Created today: {len(created_today)}")
    lines.append(f"Deleted today: {len(deleted_today)}")

    return "\n".join(lines)


def send_report(settings: Settings, body: str) -> None:
    """Send the report via SMTP."""
    if not settings.smtp_host or not settings.report_to:
        LOG.warning("SMTP not configured, printing report to stdout")
        print(body)
        return

    recipients = [addr.strip() for addr in settings.report_to.split(",")]

    msg = MIMEText(body)
    msg["Subject"] = (
        f"RA Ingest Report - {datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')}"
    )
    msg["From"] = settings.report_from or settings.smtp_user
    msg["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.starttls()
            if settings.smtp_user and settings.smtp_password:
                server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(msg["From"], recipients, msg.as_string())
        LOG.info("Report sent to %s", settings.report_to)
    except Exception:
        LOG.exception("Failed to send report")
        print(body)
