FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app

# Instalar dependencias del sistema y herramientas de espera
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app 
CMD ["sh", "-c", "while ! nc -z kafka 9092; do echo 'Waiting for Kafka...'; sleep 1; done; python /app/llm_consumer_producer.py"]
