import time
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional

# ⚠️ Importaciones de Kafka
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.error import KafkaError 
from confluent_kafka.admin import AdminClient, NewTopic 

# 🚀 Importación para Google Gemini API
from google import genai
from google.genai.errors import APIError
from google.api_core.exceptions import ResourceExhausted

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno (Usando .env.api)
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE CONEXIONES (5 TOPICS) ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')             
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')   # Éxito -> Scorer
KAFKA_FINAL_OUTPUT_TOPIC = os.getenv('KAFKA_FINAL_OUTPUT_TOPIC', 'final_answer') # Salida Scorer -> DB Writer

# 🆕 TOPICS DE ERROR PARA RESILIENCIA
KAFKA_RETRY_TOPIC = os.getenv('KAFKA_RETRY_TOPIC', 'llm_retry_queue')       # Errores transitorios (Reintento)
KAFKA_QUOTA_TOPIC = os.getenv('KAFKA_QUOTA_TOPIC', 'llm_quota_error')       # Errores permanentes (Cuota/Alerta)

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


# --- Definición de Tipos de Respuesta ---

class LLMResponse:
    """Clase simple para encapsular la respuesta o el tipo de error."""
    def __init__(self, text: Optional[str] = None, error_type: Optional[str] = None):
        self.text = text
        self.error_type = error_type

# --- LÓGICA DE GESTIÓN DE TOPICS ---

def ensure_topic_exists(topic_name: str, broker: str):
    """Crea el topic en Kafka si no existe, con reintentos."""
    max_retries = 10
    retry_delay = 5 
    num = 4 if topic_name == KAFKA_INPUT_TOPIC else 1

    for i in range(max_retries):
        try:
            admin_client = AdminClient({'bootstrap.servers': broker})
            topics_metadata = admin_client.list_topics(timeout=5).topics 

            if topic_name in topics_metadata:
                logger.info(f"Topic '{topic_name}' ya existe en Kafka.")
                return 
            logger.info(f"Creando topic '{topic_name}' en Kafka (Intento {i+1}/{max_retries})...")
            new_topic = NewTopic(topic=topic_name, num_partitions=num, replication_factor=1)
            
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

# Función de Generación de Respuesta con Gestión de Errores
def generate_gemini_response(prompt: str) -> LLMResponse:
    """Llama a Gemini, clasificando errores para ruteo en Kafka."""
    if not gemini_client:
        return LLMResponse(error_type="INTERNAL_CLIENT_ERROR")
        
    logger.info(f"Llamando a Gemini API para el prompt: {prompt[:50]}...")
    
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt],
        )
        return LLMResponse(text=response.text.strip())
        
    except ResourceExhaustedError as e:
        # 🔴 Error de CUOTA/Límite de Tarifa
        logger.error(f"Error de CUOTA/RATE-LIMIT de API (ResourceExhausted): {e}")
        return LLMResponse(error_type="QUOTA_EXCEEDED") 
        
    except APIError as e:
        # 🟡 Error transitorio (sobrecarga, timeout)
        logger.error(f"Error TRANSITORIO de API: {e}")
        return LLMResponse(error_type="API_TRANSIENT_ERROR") 
        
    except Exception as e:
        logger.error(f"Error desconocido al llamar a Gemini: {e}")
        return LLMResponse(error_type="UNKNOWN_ERROR")


def kafka_worker():
    """Worker principal que consume 'questions', procesa LLM y rutea el resultado/error."""
    try:
        # 1. 🟢 ASEGURAR LOS CINCO TOPICS
        logger.info("Asegurando los CINCO topics de Kafka...")
        ensure_topic_exists(KAFKA_INPUT_TOPIC, KAFKA_BROKER)        
        ensure_topic_exists(KAFKA_LLM_OUTPUT_TOPIC, KAFKA_BROKER)   
        ensure_topic_exists(KAFKA_FINAL_OUTPUT_TOPIC, KAFKA_BROKER) 
        ensure_topic_exists(KAFKA_RETRY_TOPIC, KAFKA_BROKER)       
        ensure_topic_exists(KAFKA_QUOTA_TOPIC, KAFKA_BROKER)       

        # 2. Crear consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'llm-worker-group',
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_INPUT_TOPIC])
        logger.info(f"Consumer suscrito a topic '{KAFKA_INPUT_TOPIC}'.")
        
        logger.info("Kafka Worker iniciado correctamente. Esperando mensajes...")
        
        while True:
            msg = consumer.poll(timeout=1.0) 
            
            if msg is None or msg.error():
                continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                pregunta = data.get('question', data.get('title', 'N/A'))
                
                # 🆕 OBTENER CONTADOR DE REINTENTOS SI EXISTE (para pasarlo al error_payload)
                current_retries = data.get('retry_count', 0) 
                
                if pregunta == 'N/A':
                    logger.warning(f"Mensaje descartado por falta de 'question': {data}")
                    continue 

                # --- Lógica de Procesamiento LLM REAL ---
                llm_result = generate_gemini_response(pregunta)
                producer = get_kafka_producer()
                
                # Payload base para el error
                error_payload = {
                    "question_id": f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                    "original_question": pregunta,
                    "timestamp": datetime.now().isoformat(),
                    "retry_count": current_retries # 👈 INCLUIR EL CONTADOR
                }

                if llm_result.error_type is None:
                    # 3A. 🟢 ÉXITO: Enviar a llm_answers (topic intermedio para Scorer)
                    response = {
                        "question_id": f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                        "title": pregunta,
                        "score": 9, 
                        "answer": llm_result.text,
                        "retry_count": current_retries # Registrar el contador
                    }
                    if producer:
                        producer.produce(KAFKA_LLM_OUTPUT_TOPIC, value=json.dumps(response).encode('utf-8'))
                        logger.info(f"ÉXITO: Enviado a tópico: '{KAFKA_LLM_OUTPUT_TOPIC}'")

                elif llm_result.error_type == "QUOTA_EXCEEDED":
                    # 3B. 🔴 ERROR PERMANENTE: Enviar a llm_quota_error (Alerta)
                    error_payload["error_details"] = "Cuota o límite de tarifa excedido (ResourceExhausted). Requiere intervención."
                    if producer:
                        producer.produce(KAFKA_QUOTA_TOPIC, value=json.dumps(error_payload).encode('utf-8'))
                        logger.warning(f"ERROR CUOTA: Enviado a tópico: '{KAFKA_QUOTA_TOPIC}'")

                elif llm_result.error_type == "API_TRANSIENT_ERROR":
                    # 3C. 🟡 ERROR TRANSITORIO: Enviar a llm_retry_queue (Reintento)
                    error_payload["error_details"] = "Error transitorio del servidor/sobrecarga. Reintento recomendado."
                    if producer:
                        producer.produce(KAFKA_RETRY_TOPIC, value=json.dumps(error_payload).encode('utf-8'))
                        logger.warning(f"ERROR REINTENTO: Enviado a tópico: '{KAFKA_RETRY_TOPIC}' (Intento {current_retries + 1})")

                else:
                    # 3D. ⚫ OTRO ERROR: Por defecto, enviar a la cola de reintento
                    error_payload["error_details"] = f"Error no mapeado: {llm_result.error_type}. Derivado a reintento."
                    if producer:
                        producer.produce(KAFKA_RETRY_TOPIC, value=json.dumps(error_payload).encode('utf-8'))
                        logger.error(f"Error no mapeado: {llm_result.error_type}. Enviado a '{KAFKA_RETRY_TOPIC}'")
                    
                if producer:
                    producer.flush(timeout=1)
                
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka Worker: {e}")
        return

if __name__ == "__main__":
    kafka_worker()