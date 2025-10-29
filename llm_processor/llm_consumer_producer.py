import time
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# ⚠️ Importaciones para Confluent Kafka ⚠️
from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from confluent_kafka.error import KafkaError 

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Asegura la ruta de tu .env)
# Asumiendo que el archivo .env.api fue copiado aquí o que tienes uno .env.llm_processor
load_dotenv("/app/.env.llm_processor") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')

# --- LÓGICA DE CONEXIÓN DE KAFKA ---

_kafka_producer = None
def get_kafka_producer(max_retries=5, delay=2) -> Producer:
    """Inicializa y retorna el productor de Confluent Kafka bajo demanda."""
    global _kafka_producer
    if _kafka_producer is not None:
        return _kafka_producer
    
    if not KAFKA_BROKER:
        logger.error("KAFKA_BROKER no está definido.")
        raise ConnectionError("KAFKA_BROKER no está definido en el entorno.")

    for i in range(max_retries):
        try:
            producer_conf = {
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'llm-worker-producer',
                'api.version.request': True
            }
            producer = Producer(producer_conf)
            producer.poll(timeout=1.0) 
            _kafka_producer = producer
            logger.info("Kafka Producer (LLM) inicializado.")
            return producer
        except Exception as e:
            logger.warning(f"Intento {i+1}/{max_retries}: Fallo al inicializar Kafka Producer: {e}")
            if i == max_retries - 1:
                logger.error("Fallo definitivo: No se pudo conectar a Kafka para publicar.")
                raise ConnectionError(f"Fallo al inicializar Kafka Producer después de {max_retries} intentos.")
            time.sleep(delay)
            
    raise ConnectionError("No se pudo establecer la conexión al productor de Kafka.")

def kafka_worker():
    """Worker principal que procesa mensajes de Kafka (Confluent)."""
    try:
        # 1. Crear consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'llm-worker-group',
            'auto.offset.reset': 'earliest',
            'api.version.request': True
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_INPUT_TOPIC])
        logger.info(f"Consumer de Kafka (Confluent) suscrito a topic '{KAFKA_INPUT_TOPIC}'.")
        
        logger.info("Kafka Worker iniciado correctamente. Esperando mensajes...")
        
        while True:
            msg = consumer.poll(timeout=1.0) 
            
            if msg is None:
                continue
            if msg.error():
                # 🚨 CORRECCIÓN DEL ERROR DE IMPORTACIÓN 🚨
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue 
                else:
                    logger.error(f"Error al consumir: {msg.error()}")
                    continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                pregunta = data.get('question', 'N/A')
                logger.info(f"Mensaje recibido para procesamiento: {pregunta}")
                
                # --- Lógica de Procesamiento LLM Simulado ---
                response_text = f"Respuesta LLM: {pregunta} (Procesado a las {datetime.now().isoformat()})"
                
                response = {
                    "question_id": f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                    "title": pregunta,
                    "score": 9, 
                    "answer": response_text
                }
                
                # 3. Enviar respuesta usando el productor bajo demanda
                producer = get_kafka_producer()
                if producer:
                    producer.produce(KAFKA_LLM_OUTPUT_TOPIC, value=json.dumps(response).encode('utf-8'))
                    producer.flush(timeout=1)
                    logger.info(f"Respuesta enviada a '{KAFKA_LLM_OUTPUT_TOPIC}'")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Worker: {e}")
        return

if __name__ == "__main__":
    kafka_worker()
