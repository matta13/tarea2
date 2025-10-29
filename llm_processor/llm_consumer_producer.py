import time
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# ⚠️ Importaciones de Kafka
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.error import KafkaError 
from confluent_kafka.admin import AdminClient, NewTopic 

# 🚀 Importación para Google Gemini API
from google import genai
from google.genai.errors import APIError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Usando .env.api)
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')          # Topic 1 (Entrada)
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers') # Topic 2 (Salida LLM -> Entrada Scorer)
KAFKA_FINAL_OUTPUT_TOPIC = os.getenv('KAFKA_FINAL_OUTPUT_TOPIC', 'final_answer') # Topic 3 (Salida Scorer -> Entrada DB Writer)

# --- CONFIGURACIÓN DE GEMINI ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY no está definida. La lógica LLM fallará.")
try:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error al inicializar el cliente de Gemini: {e}")
    gemini_client = None


# --- LÓGICA DE GESTIÓN DE TOPICS (Mantenida) ---

def ensure_topic_exists(topic_name: str, broker: str):
    """Crea el topic en Kafka si no existe, con reintentos."""
    max_retries = 10
    retry_delay = 5 
    # ... (Mantenemos la implementación robusta de ensure_topic_exists) ...
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
            logger.warning(f"Fallo al contactar/crear topic '{topic_name}' (Intento {i+1}/{max_retries}). Reintentando en {retry_delay}s. Error: {e}")
            if i == max_retries - 1:
                 logger.error(f"Fallo definitivo: No se pudo crear el topic '{topic_name}'.")
                 raise ConnectionError(f"No se pudo crear el topic {topic_name} después de {max_retries} intentos.")
            time.sleep(retry_delay)


# --- LÓGICA DE CONEXIÓN DE KAFKA PRODUCTOR (Mantenida) ---

_kafka_producer = None
def get_kafka_producer(max_retries=5, delay=2) -> Producer:
    # ... (Mantener la función get_kafka_producer tal cual) ...
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

# Función de Generación de Respuesta con Gemini (Mantenida)
def generate_gemini_response(prompt: str) -> str:
    # ... (Mantener la lógica de llamada a Gemini) ...
    if not gemini_client:
        return "ERROR: Cliente de Gemini no inicializado."
        
    logger.info(f"Llamando a Gemini API para el prompt: {prompt[:50]}...")
    
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt],
        )
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
        # 1. 🟢 ASEGURAR LOS TRES TOPICS
        logger.info("Asegurando los tres topics de Kafka: questions, llm_answers, final_answer...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)        # questions
        ensure_topic_exists(KAFKA_LLM_OUTPUT_TOPIC, KAFKA_BROKER)   # llm_answers
        ensure_topic_exists(KAFKA_FINAL_OUTPUT_TOPIC, KAFKA_BROKER) # final_answer
        
        # 2. Crear consumer
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
                # ... (Manejo de otros errores)
                else:
                    logger.error(f"Error al consumir: {msg.error()}")
                    continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                pregunta = data.get('question', data.get('title', 'N/A')) 
                
                if pregunta == 'N/A':
                    logger.warning(f"Mensaje descartado por falta de 'question' o 'title': {data}")
                    continue 

                logger.info(f"Mensaje recibido para procesamiento: {pregunta}")
                
                # --- Lógica de Procesamiento LLM REAL ---
                response_text = generate_gemini_response(pregunta)
                
                # NOTA: Usamos un score fijo (ej. 9) aquí, ya que el 'scorer' lo actualizará después.
                response = {
                    "question_id": f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                    "title": pregunta,
                    "score": 9, 
                    "answer": response_text
                }
                
                # 3. 🟠 ENVIAR RESPUESTA AL TOPIC INTERMEDIO SOLAMENTE
                producer = get_kafka_producer()
                if producer:
                    producer.produce(KAFKA_LLM_OUTPUT_TOPIC, value=json.dumps(response).encode('utf-8'))
                    producer.flush(timeout=1)
                    logger.info(f"Respuesta enviada a tópico intermedio: '{KAFKA_LLM_OUTPUT_TOPIC}'")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Worker: {e}")
        return

if __name__ == "__main__":
    kafka_worker()
