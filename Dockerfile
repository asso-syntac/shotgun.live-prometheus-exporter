FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shotgun_exporter.py .
COPY reimport_event.py .
EXPOSE 9090

CMD ["python", "shotgun_exporter.py"]
