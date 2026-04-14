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

    # Google Calendar sync (optional)
    gcal_enabled: bool = Field(
        default=False, description="Enable Google Calendar grant/claim sync"
    )
    gcal_cron: str = Field(
        default="*/5 * * * *",
        description="Cron schedule for calendar sync (default: every 5 min)",
    )
    gcal_calendar_id: str = Field(default="", description="Google Calendar ID")
    gcal_calendar_token: str = Field(default="", description="Google Calendar API key")
    gcal_spectrum_id: str = Field(
        default="", description="Spectrum ID for calendar claims"
    )
    gcal_min_freq: float = Field(
        default=1000, description="Min frequency (MHz) for calendar claims"
    )
    gcal_max_freq: float = Field(
        default=2000, description="Max frequency (MHz) for calendar claims"
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
