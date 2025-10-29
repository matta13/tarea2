FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
# Instalar librerías de Python (debes asegurar que 'requirements.txt' esté en la carpeta ./db_writer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*
CMD ["python", "db_writer_consumer.py"]
