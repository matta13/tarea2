import time
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# Importaciones de Kafka
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.error import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic 

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Usamos el archivo compartido)
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
# Consume del topic de reintento
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_RETRY_TOPIC', 'llm_retry_queue') 
# Produce de vuelta al topic de entrada original
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions') 

# --- PARÁMETROS DE REINTENTO ---
RETRY_DELAY_SECONDS = 300 # 5 minutos de espera antes de reintentar
MAX_RETRIES = 3 # Limitar los reintentos para no entrar en un bucle infinito


# --- LÓGICA DE GESTIÓN DE TOPICS ---

def ensure_topic_exists(topic_name: str, broker: str):
    """Crea el topic en Kafka si no existe, con reintentos."""
    max_retries = 10
    retry_delay = 5 
    
    for i in range(max_retries):
        try:
            admin_client = AdminClient({'bootstrap.servers': broker})
            topics_metadata = admin_client.list_topics(timeout=5).topics 
            if topic_name in topics_metadata:
                logger.info(f"Topic '{topic_name}' ya existe en Kafka.")
                return 
            
            logger.info(f"Creando topic '{topic_name}' en Kafka (Intento {i+1}/{max_retries})...")
            new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result() 
            logger.info(f"Topic '{topic_name}' creado correctamente.")
            return 
        except Exception as e:
            time.sleep(retry_delay)
            if i == max_retries - 1:
                 raise ConnectionError(f"No se pudo crear el topic {topic_name} después de {max_retries} intentos.")

# --- LÓGICA DE CONEXIÓN DE KAFKA PRODUCTOR ---

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
                'client.id': 'retry-producer',
            }
            producer = Producer(producer_conf)
            producer.poll(timeout=1.0) 
            _kafka_producer = producer
            logger.info("Kafka Producer (Retry) inicializado.")
            return producer
        except Exception as e:
            time.sleep(delay)
            
    raise ConnectionError("No se pudo establecer la conexión al productor de Kafka.")


def kafka_retry_worker():
    """Worker que maneja errores transitorios y programa reintentos."""
    try:
        # 1. Asegurar topics: Solo el de entrada 
        logger.info("Asegurando topic de reintento...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)
        # Asegurar el topic de salida
        ensure_topic_exists(KAFKA_OUTPUT_TOPIC, KAFKA_BROKER)
        
        # 2. Crear consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'llm-retry-group', 
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_INPUT_TOPIC])
        logger.info(f"Consumer de Reintento suscrito a topic '{KAFKA_INPUT_TOPIC}'.")
        
        logger.info(f"Kafka Retry Worker iniciado. Esperando mensajes. Espera de reintento: {RETRY_DELAY_SECONDS}s.")
        
        while True:
            msg = consumer.poll(timeout=1.0) 
            
            if msg is None or msg.error():
                continue

            # Procesamiento del Mensaje de Error
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Obtener contador de reintentos
                current_retries = data.get('retry_count', 0)
                original_question = data.get('original_question', 'N/A')
                
                if current_retries >= MAX_RETRIES:
                    logger.error(f" LÍMITE EXCEDIDO: Descartando pregunta '{original_question}'. Intentos: {current_retries}")
                    continue
                
                logger.warning(f" PROGRAMANDO REINTENTO (Intento {current_retries + 1}/{MAX_RETRIES}) para '{original_question}'. Esperando {RETRY_DELAY_SECONDS}s...")
                time.sleep(RETRY_DELAY_SECONDS) 

               
                republish_payload = {
                    "question": original_question, 
                    "title": original_question, 
                    "retry_count": current_retries + 1 
                }
                
                producer = get_kafka_producer()
                if producer:
                    producer.produce(KAFKA_OUTPUT_TOPIC, value=json.dumps(republish_payload).encode('utf-8'))
                    producer.flush(timeout=1)
                    logger.info(f" REINTENTADO: Pregunta republicada en topic '{KAFKA_OUTPUT_TOPIC}' (Intento {current_retries + 1})")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje de reintento: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Retry Worker: {e}")
        return

if __name__ == "__main__":

    kafka_retry_worker()
