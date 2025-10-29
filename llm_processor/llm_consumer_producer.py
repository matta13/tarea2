import time
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# ⚠️ Importaciones para Confluent Kafka ⚠️
from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from confluent_kafka.error import KafkaError 
# 🆕 Importaciones para AdminClient
from confluent_kafka.admin import AdminClient, NewTopic 

from google import genai
from google.genai.errors import APIError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Asegura la ruta de tu .env)
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')

# --- LÓGICA DE GESTIÓN DE TOPICS ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY no está definida. La lógica LLM fallará.")
# Inicializa el cliente de Gemini (se usará bajo demanda en kafka_worker)
try:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error al inicializar el cliente de Gemini: {e}")
    gemini_client = None

def ensure_topic_exists(topic_name: str, broker: str):
    """
    Crea el topic en Kafka si no existe. 
    Aumentamos los reintentos para manejar mejor el arranque de Kafka.
    """
    max_retries = 10
    retry_delay = 5 # segundos
    
    for i in range(max_retries):
        try:
            admin_client = AdminClient({'bootstrap.servers': broker})
            
            # Intenta obtener metadatos de los topics, esto también prueba la conexión.
            topics_metadata = admin_client.list_topics(timeout=5).topics 

            if topic_name in topics_metadata:
                logger.info(f"Topic '{topic_name}' ya existe en Kafka.")
                return # Éxito: el topic existe

            logger.info(f"Creando topic '{topic_name}' en Kafka (Intento {i+1}/{max_retries})...")
            new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
            
            # create_topics es asíncrono, esperamos el resultado
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result() 
            logger.info(f"Topic '{topic_name}' creado correctamente.")
            return # Éxito: el topic fue creado

        except Exception as e:
            logger.warning(f"Fallo al contactar/crear topic '{topic_name}' (Intento {i+1}/{max_retries}). Reintentando en {retry_delay}s. Error: {e}")
            if i == max_retries - 1:
                 logger.error(f"Fallo definitivo: No se pudo crear el topic '{topic_name}'.")
                 raise ConnectionError(f"No se pudo crear el topic {topic_name} después de {max_retries} intentos.")
            time.sleep(retry_delay)


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
                # ❌ ELIMINADO: 'api.version.request': True
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

def generate_gemini_response(prompt: str) -> str:
    """Llama a la API de Gemini para obtener una respuesta."""
    if not gemini_client:
        return "ERROR: Cliente de Gemini no inicializado."
        
    logger.info(f"Llamando a Gemini API para el prompt: {prompt[:50]}...")
    
    try:
        # Usamos el modelo configurado
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt],
        )
        # Limpiamos el texto para asegurar el formato JSON
        return response.text.strip()
        
    except APIError as e:
        logger.error(f"Error de API al llamar a Gemini: {e}")
        return f"ERROR_API: No se pudo generar respuesta. {e}"
    except Exception as e:
        logger.error(f"Error desconocido al llamar a Gemini: {e}")
        return f"ERROR_UNKNOWN: {e}"

def kafka_worker():
    """Worker principal que procesa mensajes de Kafka (Confluent)."""
    try:
        # 1. Asegurar topics
        logger.info("Asegurando topics de Kafka...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)
        ensure_topic_exists(KAFKA_LLM_OUTPUT_TOPIC, KAFKA_BROKER)
        
        # 2. Crear consumer (Mantenido)
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'llm-worker-group',
            'auto.offset.reset': 'earliest',
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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue 
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Topic '{KAFKA_INPUT_TOPIC}' no disponible. Reintentando suscripción...")
                    consumer.subscribe([KAFKA_INPUT_TOPIC]) 
                    time.sleep(1)
                    continue
                else:
                    logger.error(f"Error al consumir: {msg.error()}")
                    continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Usamos la corrección de parsing para aceptar 'title'
                pregunta = data.get('question', data.get('title', 'N/A')) 
                
                if pregunta == 'N/A':
                    logger.warning(f"Mensaje descartado por falta de 'question' o 'title': {data}")
                    continue 

                logger.info(f"Mensaje recibido para procesamiento: {pregunta}")
                
                # --- Lógica de Procesamiento LLM REAL ---
                response_text = generate_gemini_response(pregunta)
                
                response = {
                    "question_id": f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                    "title": pregunta,
                    "score": 0, # Mantener un score fijo o ajustarlo si usas una lógica de calificación LLM real
                    "answer": response_text
                }
                
                # 3. Enviar respuesta
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
