FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*
COPY db_writer_consumer.py /app/db_writer_consumer.py
COPY ./.env.api /app/.env.api
COPY 01schema.sql /app/01schema.sql


