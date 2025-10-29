# En ./llm_processor/Dockerfile
FROM python:3.11-slim
WORKDIR /app
# ⚠️ CLAVE: Agrega la raíz del proyecto al PYTHONPATH para que pueda ver 'api' si es necesario
ENV PYTHONPATH=/app

# Instalar dependencias del sistema y herramientas de espera
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . /app 

