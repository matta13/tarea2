# En ./api/Dockerfile

FROM python:3.11-slim

WORKDIR /app

# ⚠️ CLAVE: Instalar tools de DB y Netcat para la espera activa
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente (main.py y .env.api están en este contexto)
COPY . /app 

# 🚨 El CMD es sobrescrito por docker-compose.yml con la lógica de espera 🚨
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

