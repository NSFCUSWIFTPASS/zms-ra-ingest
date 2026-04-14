#!/bin/sh
set -e

CRON_JOBS=""

# Export current env so cron jobs inherit it
env | grep '^RA_INGEST_' > /tmp/ra_ingest.env

# Note: gcal sync is now integrated into the main poll loop (no separate cron).
# Set RA_INGEST_GCAL_ENABLED=true and the reconciler will pick it up.

# Daily email report
if [ "$RA_INGEST_REPORT_ENABLED" = "true" ] || [ "$RA_INGEST_REPORT_ENABLED" = "True" ] || [ "$RA_INGEST_REPORT_ENABLED" = "1" ]; then
    REPORT_CRON="${RA_INGEST_REPORT_CRON:-0 8 * * *}"
    echo "Enabling daily report: ${REPORT_CRON}"
    CRON_JOBS="${CRON_JOBS}${REPORT_CRON} . /tmp/ra_ingest.env && /opt/venv/bin/zms-ra-ingest --report >> /proc/1/fd/1 2>> /proc/1/fd/2\n"
fi

# Install cron jobs if any
if [ -n "$CRON_JOBS" ]; then
    printf "$CRON_JOBS" > /tmp/crontab
    crontab /tmp/crontab
    crond
fi

# Run the main poll loop
exec zms-ra-ingest "$@"
