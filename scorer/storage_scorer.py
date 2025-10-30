import time
import json
import logging
import os
import re 
from dotenv import load_dotenv

# Importaciones de Kafka
from confluent_kafka import Consumer, Producer
from confluent_kafka.error import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic 

# 🚀 Importación para Google Gemini API
from google import genai
from google.genai.errors import APIError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Usamos el archivo compartido, que DEBE estar en /app/)
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers') # Consume de llm_answers
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_FINAL_OUTPUT_TOPIC', 'final_answer') # Produce a final_answer

# --- CONFIGURACIÓN DE GEMINI ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY no está definida. La lógica Scorer fallará.")
try:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error al inicializar el cliente de Gemini: {e}")
    gemini_client = None


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
            logger.warning(f"Fallo al contactar/crear topic '{topic_name}' (Intento {i+1}/{max_retries}). Reintentando en {retry_delay}s. Error: {e}")
            if i == max_retries - 1:
                 logger.error(f"Fallo definitivo: No se pudo crear el topic '{topic_name}'.")
                 raise ConnectionError(f"No se pudo crear el topic {topic_name} después de {max_retries} intentos.")
            time.sleep(retry_delay)

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
                'client.id': 'scorer-producer',
            }
            producer = Producer(producer_conf)
            producer.poll(timeout=1.0) 
            _kafka_producer = producer
            logger.info("Kafka Producer (Scorer) inicializado.")
            return producer
        except Exception as e:
            logger.warning(f"Intento {i+1}/{max_retries}: Fallo al inicializar Kafka Producer: {e}")
            if i == max_retries - 1:
                logger.error("Fallo definitivo: No se pudo conectar a Kafka para publicar.")
                raise ConnectionError(f"Fallo al inicializar Kafka Producer después de {max_retries} intentos.")
            time.sleep(delay)
            
    raise ConnectionError("No se pudo establecer la conexión al productor de Kafka.")

# Lógica de Calificación con Gemini
def generate_gemini_score(question: str, answer: str) -> int:
    """Llama a Gemini para obtener un puntaje (1-10) y lo parsea."""
    if not gemini_client:
        return 0
        
    scoring_prompt = f"""
    Eres un evaluador experto. Analiza la siguiente pregunta y la respuesta.
    Asigna un puntaje de 1 a 10 basado en:
    1. Relevancia y precisión de la respuesta.
    2. Nivel de detalle y completitud.
    
    Responde ÚNICAMENTE con el número del puntaje final (1-10), sin texto adicional.
    
    ---
    PREGUNTA: "{question}"
    RESPUESTA: "{answer}"
    ---
    PUNTAJE (1-10):
    """
    
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[scoring_prompt],
        )
        
        # 🧪 Parsear el puntaje
        match = re.search(r'\b(10|[1-9])\b', response.text)
        if match:
            score = int(match.group(1))
            return max(1, min(10, score)) 
        
        logger.warning(f"No se pudo parsear el puntaje de la respuesta LLM: {response.text}")
        return 5 
        
    except APIError as e:
        logger.error(f"Error de API al calificar con Gemini: {e}")
        return 1 
    except Exception as e:
        logger.error(f"Error desconocido en el Scorer: {e}")
        return 1


def kafka_scorer_worker():
    """Worker principal que consume de llm_answers, califica, y produce a final_answer."""
    try:
        # 1. Asegurar topics: Entrada (llm_answers) y Salida (final_answer)
        logger.info("Asegurando topics de Kafka para Scorer...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)
        ensure_topic_exists(KAFKA_OUTPUT_TOPIC, KAFKA_BROKER)
        
        # 2. Crear consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'scorer-worker-group', 
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_INPUT_TOPIC])
        logger.info(f"Consumer de Kafka (Scorer) suscrito a topic '{KAFKA_INPUT_TOPIC}'.")
        
        logger.info("Kafka Scorer iniciado correctamente. Esperando respuestas LLM...")
        
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
                
                question = data.get('title', 'N/A') 
                answer = data.get('answer', 'N/A')
                
                if question == 'N/A' or answer == 'N/A':
                    logger.warning(f"Mensaje descartado en Scorer por falta de datos: {data}")
                    continue 

                logger.info(f"Calificando pregunta: {question}")
                
                # --- Lógica de Calificación LLM REAL ---
                new_score = generate_gemini_score(question, answer)
                logger.info(f"Puntaje generado para '{question[:20]}...': {new_score}")

                # 3. Preparar mensaje final
                data['score'] = new_score 
                
                # 4. Enviar respuesta final
                producer = get_kafka_producer()
                if producer:
                    producer.produce(KAFKA_OUTPUT_TOPIC, value=json.dumps(data).encode('utf-8'))
                    producer.flush(timeout=1)
                    logger.info(f"Resultado final enviado a '{KAFKA_OUTPUT_TOPIC}'")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje en Scorer: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Scorer Worker: {e}")
        return

if __name__ == "__main__":

    kafka_scorer_worker()
