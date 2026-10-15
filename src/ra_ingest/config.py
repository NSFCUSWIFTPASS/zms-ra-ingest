from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "RA_INGEST_"}

    # Logging
    log_level: str = Field(
        default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)"
    )

    # ZMS connection
    zmc_url: str = Field(description="ZMC base URL, e.g. http://localhost:8010/v1")
    ra_url: str = Field(description="zms-ra base URL, e.g. http://localhost:8050")
    ra_verify_ssl: bool = Field(
        default=True, description="Verify SSL on zms-ra connection"
    )
    token: str = Field(description="ZMS API token")

    # Claim identity
    element_id: str = Field(description="Element UUID for the RA facility")

    # Poll behavior
    poll_interval_seconds: int = Field(
        default=300, description="Seconds between poll cycles"
    )

    # Sources are configured via JSON file, not env vars
    sources_config: str = Field(
        default="sources.json",
        description="Path to JSON file defining the RA data sources",
    )

    # Google Calendar sync (required)
    gcal_calendar_id: str = Field(description="Google Calendar ID")
    gcal_calendar_token: str = Field(description="Google Calendar API key")
    gcal_lookahead_days: int = Field(
        default=28,
        description="How far ahead of now to pull calendar events (days)",
    )
    gcal_priority: int = Field(
        default=900,
        ge=-1023,
        le=1023,
        description=(
            "Grant priority for calendar events. Lower than ODS so an "
            "observation preempts an overlapping calendar block"
        ),
    )
    gcal_filter_exc: str = Field(
        default="", description="Comma-separated regexps to exclude calendar events"
    )
    gcal_filter_inc: str = Field(
        default="", description="Comma-separated regexps to include calendar events"
    )
    gcal_verify_ssl: bool = Field(
        default=True, description="Verify SSL for ZMC connection from gcal sync"
    )

    # ATA antenna-status guard (optional; blank host disables it)
    ata_db_host: str = Field(
        default="", description="MySQL host with the ATA antstatus table"
    )
    ata_db_port: int = Field(default=3306, description="MySQL port")
    ata_db_user: str = Field(default="", description="Read-only MySQL user")
    ata_db_password: str = Field(default="", description="MySQL password")
    ata_db_name: str = Field(
        default="grafanadata", description="Database holding antstatus"
    )
    ata_guard_spectrums: str = Field(
        default="ISM-915",
        description=(
            "Comma-separated spectrum names to block while the ATA is in use, "
            "or * for every spectrum on the element"
        ),
    )
    ata_guard_hold_minutes: int = Field(
        default=30,
        gt=0,
        description="How far ahead each busy claim reaches; extended while in use",
    )
    ata_guard_priority: int = Field(
        default=1023, ge=-1023, le=1023, description="Busy claim priority"
    )
    ata_guard_release_seconds: int = Field(
        default=60,
        gt=0,
        description=(
            "How long after the ATA parks its busy claim ends; leaves ZMC time "
            "to schedule the shortened replacement grant"
        ),
    )

    # Email reports (optional)
    report_enabled: bool = Field(
        default=False, description="Enable daily email reports"
    )
    report_cron: str = Field(
        default="0 8 * * *",
        description="Cron schedule for reports (default: 8am daily)",
    )
    smtp_host: str = Field(default="", description="SMTP server host")
    smtp_port: int = Field(default=587, description="SMTP server port")
    smtp_user: str = Field(default="", description="SMTP username")
    smtp_password: str = Field(default="", description="SMTP password")
    report_from: str = Field(default="", description="From address for reports")
    report_to: str = Field(
        default="", description="Comma-separated recipient addresses"
    )
