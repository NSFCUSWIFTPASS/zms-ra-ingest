# builder
FROM python:3.11-slim as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir hatch

COPY pyproject.toml ./

RUN hatch dep show requirements --project-only > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
RUN pip install --no-cache-dir --no-deps .

# final
FROM python:3.11-slim as final

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends busybox-static \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /bin/busybox /usr/sbin/crond

COPY --from=builder /opt/venv /opt/venv
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/app/entrypoint.sh"]
