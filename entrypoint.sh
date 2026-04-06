#!/bin/sh
set -e

# If reporting is enabled, set up cron
if [ "$RA_INGEST_REPORT_ENABLED" = "true" ] || [ "$RA_INGEST_REPORT_ENABLED" = "True" ] || [ "$RA_INGEST_REPORT_ENABLED" = "1" ]; then
    CRON_SCHEDULE="${RA_INGEST_REPORT_CRON:-0 8 * * *}"
    echo "Enabling daily report: ${CRON_SCHEDULE}"

    # Export current env so cron job inherits it
    env | grep '^RA_INGEST_' > /tmp/ra_ingest.env

    # Write crontab
    echo "${CRON_SCHEDULE} . /tmp/ra_ingest.env && /opt/venv/bin/zms-ra-ingest --report >> /proc/1/fd/1 2>> /proc/1/fd/2" > /tmp/crontab
    crontab /tmp/crontab
    crond
fi

# Run the main poll loop
exec zms-ra-ingest "$@"
