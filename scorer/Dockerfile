FROM python:3.11-slim

WORKDIR /app

# Requerimos las dependencias de Kafka y Postgres
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage_scorer.py .
COPY .env.api .

CMD ["python", "storage_scorer.py"]