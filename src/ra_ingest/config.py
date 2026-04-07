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
    token: str = Field(description="ZMS API token")

    # Claim identity
    element_id: str = Field(description="Element UUID for the RA facility")

    # Spectrum - auto-created on startup if it doesn't exist
    spectrum_name: str = Field(
        default="ATA L-band",
        description="Name for the auto-created spectrum",
    )
    spectrum_min_freq_hz: int = Field(
        default=1000000000, description="Spectrum lower bound in Hz"
    )
    spectrum_max_freq_hz: int = Field(
        default=2000000000, description="Spectrum upper bound in Hz"
    )

    # Poll behavior
    poll_interval_seconds: int = Field(
        default=300, description="Seconds between poll cycles"
    )

    # Sources are configured via JSON file, not env vars
    sources_config: str = Field(
        default="sources.json",
        description="Path to JSON file defining the RA data sources",
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
