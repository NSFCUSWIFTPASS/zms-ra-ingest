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

RUN addgroup --system app && adduser --system --group --no-create-home --shell /bin/false --disabled-password app

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

USER app

CMD ["zms-ra-ingest"]
