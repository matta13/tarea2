import os
import time
import json
import logging
from dotenv import load_dotenv

# ⚠️ Importar desde Confluent Kafka ⚠️
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.error import KafkaError

# ⚠️ Importar las funciones de DB desde el módulo 'api' ⚠️
# Esto requiere que el PYTHONPATH esté configurado o que la raíz esté montada en /app.
from api.main import obtener_conexion_db, escribir_a_db, Row 

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno
load_dotenv("/app/.env.api") # Usamos el mismo archivo de entorno que el API

# --- CONFIGURACIÓN ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')


def kafka_db_writer_worker():
    """Worker principal que consume y escribe a la base de datos."""
    try:
        # 1. Configuración del Consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'db-writer-group',
            'auto.offset.reset': 'earliest',
            'api.version.request': True
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_LLM_OUTPUT_TOPIC]) 
        logger.info(f"DB Writer Consumer (Confluent) suscrito a topic '{KAFKA_LLM_OUTPUT_TOPIC}'.")
        
        logger.info("Kafka DB Writer iniciado. Esperando resultados de LLM...")
        
        while True:
            msg = consumer.poll(timeout=1.0) 
            
            if msg is None:
                continue
            if msg.error():
                # Manejo de errores de consumo
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue 
                else:
                    logger.error(f"Error al consumir: {msg.error()}")
                    continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # 2. Convertir y Escribir a DB
                fila = Row(
                    score=data.get('score', 0),
                    title=data.get('title', 'N/A'),
                    body=data.get('question_id'), # Usamos el ID como body para ejemplo
                    answer=data.get('answer', 'Respuesta no generada')
                )
                
                escribir_a_db(fila) # Llama a la función de la API
                logger.info(f"Resultado LLM guardado en DB para pregunta: {fila.title}")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje y escribiendo a DB: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka DB Writer: {e}")
        return

if __name__ == "__main__":
    # Asegúrate de que la tabla 'querys' exista
    kafka_db_writer_worker()
