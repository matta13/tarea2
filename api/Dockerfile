FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY ./.env.api /app/.env.api
COPY . /app 
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
