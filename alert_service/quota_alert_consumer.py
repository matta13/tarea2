import time
import json
import logging
import os
from dotenv import load_dotenv

# Importaciones de Kafka
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.error import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic 

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_QUOTA_TOPIC', 'llm_quota_error') # Consume del topic de error de cuota

# --- LÓGICA DE GESTIÓN DE TOPICS ---
def ensure_topic_exists(topic_name: str, broker: str):
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
            

def kafka_alert_worker():
    """Worker que consume errores de cuota y simula la alerta."""
    try:
        # 1. Asegurar el topic de entrada
        logger.info("Asegurando topic de alerta...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)
        
        # 2. Crear consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'quota-alert-group', 
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_INPUT_TOPIC])
        logger.info(f"Consumer de Alerta suscrito a topic '{KAFKA_INPUT_TOPIC}'.")
        
        logger.info("Kafka Alert Worker iniciado. Esperando errores de cuota...")
        
        while True:
            msg = consumer.poll(timeout=1.0) 
            
            if msg is None or msg.error():
                continue

            # Procesamiento del Mensaje de Error
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                original_question = data.get('original_question', 'N/A')
                error_details = data.get('error_details', 'Desconocido')
                
                # logica de la alerta
                logger.critical(f"  ALERTA DE CUOTA EXCEDIDA  ")
                logger.critical(f"  Pregunta Original: {original_question}")
                logger.critical(f"  Detalle del Error: {error_details}")
                logger.critical(f"  ID de Mensaje: {data.get('question_id')}")
                logger.critical("  Acción Requerida: Intervención manual o aumento de límite de API.")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje de alerta: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Alert Worker: {e}")
        return

if __name__ == "__main__":

    kafka_alert_worker()
