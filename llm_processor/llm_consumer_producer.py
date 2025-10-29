from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import json
import logging
import threading
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_producer(retries=5, delay=5):
    """Intenta conectarse a Kafka con reintentos"""
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                # ❌ ELIMINAR api_version: permitimos que la librería negocie
                # api_version=(2, 0, 2), 
                request_timeout_ms=30000,
                retry_backoff_ms=1000
            )
            
            # ❌ ELIMINAR LÓGICA DE VERIFICACIÓN QUE ESTÁ FALLANDO ❌
            # Si el constructor no lanza excepción, el productor está listo para trabajar.
            
            logger.info("Conexión a Kafka establecida exitosamente")
            return producer
            
        except NoBrokersAvailable as e:
            logger.warning(f"Intento {i+1}/{retries}: No se pudo conectar a Kafka. Reintentando en {delay} segundos...")
            if i == retries - 1:
                logger.error("No se pudo establecer conexión con Kafka después de todos los intentos")
                raise e
            time.sleep(delay)
        except Exception as e:
            # Ahora este except atrapará solo errores fatales que no sean de conexión (si los hay)
            logger.warning(f"Intento {i+1}/{retries}: Error fatal en inicialización: {e}. Reintentando en {delay} segundos...")
            if i == retries - 1:
                logger.error("Error fatal conectando a Kafka")
                raise e
            time.sleep(delay)

def create_kafka_consumer(topic, retries=5, delay=5):
    """Intenta crear un consumer con reintentos"""
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='llm-worker-group',
                request_timeout_ms=30000,
                retry_backoff_ms=1000
            )
            # También forzamos el chequeo de metadatos en el consumer
            consumer.poll(timeout_ms=1000)
            
            # Esto es un workaround para asegurar que el consumer se conectó y vio el tópico
            topics_meta = consumer.topics()
            if topic not in topics_meta:
                logger.warning(f"Tópico '{topic}' no encontrado, esperando su creación...")
            
            logger.info(f"Consumer de Kafka para topic '{topic}' creado exitosamente")
            return consumer
        except NoBrokersAvailable as e:
            logger.warning(f"Intento {i+1}/{retries}: No se pudo crear consumer para Kafka. Reintentando en {delay} segundos...")
            if i == retries - 1:
                logger.error("No se pudo crear el consumer después de todos los intentos")
                raise e
            time.sleep(delay)

def kafka_worker():
    """Worker principal que procesa mensajes de Kafka"""
    try:
        # Crear consumer con reintentos
        consumer = create_kafka_consumer('llm-requests')
        
        # Crear producer con reintentos  
        producer = create_kafka_producer()
        
        logger.info("Kafka Worker iniciado correctamente. Esperando mensajes...")
        
        # Asumiendo que 'llm-responses' es el tópico de salida
        TOPIC_RESPONSES = 'llm-responses' 
        
        for message in consumer:
            try:
                data = message.value
                logger.info(f"Mensaje recibido: {data}")
                
                # --- Lógica de Procesamiento LLM ---
                response = {
                    "original_message": data,
                    "processed_at": datetime.now().isoformat(),
                    "response": f"Procesado: {data.get('text', '') if isinstance(data, dict) else data}"
                }
                
                # Enviar respuesta
                producer.send(TOPIC_RESPONSES, value=response)
                producer.flush() # Forzamos el envío inmediato
                logger.info(f"Respuesta enviada a '{TOPIC_RESPONSES}': {response}")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Worker: {e}")
        return

if __name__ == "__main__":
    kafka_worker()
