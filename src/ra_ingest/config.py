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
    spectrum_id: str = Field(
        description="Pre-existing Spectrum UUID to reference in grants"
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
