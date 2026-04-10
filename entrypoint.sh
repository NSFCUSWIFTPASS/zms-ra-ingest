#!/bin/sh
set -e

CRON_JOBS=""

# Export current env so cron jobs inherit it
env | grep '^RA_INGEST_' > /tmp/ra_ingest.env

# Google Calendar sync
if [ "$RA_INGEST_GCAL_ENABLED" = "true" ] || [ "$RA_INGEST_GCAL_ENABLED" = "True" ] || [ "$RA_INGEST_GCAL_ENABLED" = "1" ]; then
    GCAL_CRON="${RA_INGEST_GCAL_CRON:-*/5 * * * *}"
    echo "Enabling gcal sync: ${GCAL_CRON}"

    # Build gcal.py command from env vars
    GCAL_CMD="/opt/venv/bin/python3 -m ra_ingest.gcal_sync"
    CRON_JOBS="${CRON_JOBS}${GCAL_CRON} . /tmp/ra_ingest.env && ${GCAL_CMD} >> /proc/1/fd/1 2>> /proc/1/fd/2\n"
fi

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
