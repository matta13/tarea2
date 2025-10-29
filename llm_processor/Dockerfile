FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY llm_consumer_producer.py .

CMD ["python", "llm_consumer_producer.py"]