# One image for the ingestion jobs and dbt: same toolchain on laptops, in CI and under Airflow.
# Pinned base image and dependencies, dependencies installed before the code (layer cache), non-root user.
FROM python:3.12.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ingestion/
COPY tools/ tools/
COPY dbt/ dbt/

RUN useradd --create-home pipeline && chown -R pipeline /app
USER pipeline

# No secret inside: the key is mounted at run time (/secrets), the rest comes from env_file
CMD ["python", "-c", "print('Usage: docker compose run --rm ingest python ingestion/<job>.py')"]
