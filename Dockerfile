FROM python:3.14-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shotgun_exporter.py .
COPY backfill_metrics.py .
COPY sales_cli.py .
EXPOSE 9090 9091

CMD ["python", "shotgun_exporter.py"]
