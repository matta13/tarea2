import os
import json
import time
import logging
from typing import Optional, Literal
from dotenv import load_dotenv

import psycopg2
from psycopg2 import OperationalError 
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 1. Configuración de Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("API-main")

# 2. Carga de Variables de Entorno (Usa la ruta que se copió en el Dockerfile)
# Esto asegura que las credenciales de POSTGRES y Kafka se carguen antes de usarlas.
load_dotenv("/app/.env.api") 

# --- 3. CONFIGURACIÓN Y CONEXIONES ---
# Leídas desde el entorno (asegúrate que .env.api coincida con docker-compose.yml)
DB_HOST = os.getenv("POSTGRES_HOST")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC')
KAFKA_TOPIC_RESPONSES = os.getenv('KAFKA_FINAL_RESULTS_TOPIC') # Asumiendo que usas esta variable para las respuestas

DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"


# --- 4. MODELOS PYDANTIC (DEBEN DEFINIRSE ANTES DE USARSE) ---
# Se define aquí para resolver el NameError.

class Row(BaseModel):
    score: int
    title: str
    body: Optional[str] = None
    answer: str

class AskRequest(BaseModel):
    question: str

class AskResponse(BaseModel):
    source: Literal["db", "llm_queue"] 
    row: Row
    message: str

def fila_a_mensaje(fila: Row) -> str:
    """Formatea la respuesta del LLM/DB para el usuario."""
    if fila.score == 0:
        return f"Mensaje del sistema: {fila.answer}"
    return f"Respuesta: {fila.answer}\n(Puntaje: {fila.score}/10 | Título: {fila.title})"


# --- 5. LÓGICA DE CONEXIÓN A DB (USANDO PSICOPG2) ---

_conexion_db = None
def obtener_conexion_db(max_retries: int = 5, delay: int = 2):
    """Retorna una conexión activa a PostgreSQL, intentando reconectar."""
    global _conexion_db
    if _conexion_db is not None and not _conexion_db.closed:
        return _conexion_db

    for i in range(max_retries):
        try:
            # Intentamos la conexión con las credenciales del entorno
            _conexion_db = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return _conexion_db
        except Exception as e:
            if i < max_retries - 1:
                # El log debe ser WARNING, no debe lanzar NameError
                logger.warning(f"Intento {i+1}/{max_retries}: Fallo al conectar a DB. Reintentando. Error: {e}")
                time.sleep(delay)
            else:
                # 🚨 Capturamos y relanzamos la excepción final (causa del 500)
                logger.exception(f"Fallo crítico al conectar con PostgreSQL después de {max_retries} intentos.")
                raise ConnectionError(f"Fallo al conectar con la base de datos: {e}")
    
    raise ConnectionError("No se pudo establecer la conexión a la base de datos.")

def leer_desde_db(pregunta: str) -> Optional[Row]:
    """Busca una respuesta en la DB por título (pregunta) en la tabla 'querys'."""
    conn = obtener_conexion_db()
    cursor = conn.cursor()
    
    try:
        # Usamos la tabla 'querys' como acordamos
        cursor.execute(
            "SELECT score, title, body, answer FROM querys WHERE title = %s",
            (pregunta,)
        )
        registro = cursor.fetchone()
        if registro:
            # Retorna el modelo Pydantic
            return Row(score=registro[0], title=registro[1], body=registro[2], answer=registro[3])
    except Exception as e:
        # Esto captura errores como 'tabla inexistente'
        logger.warning(f"Error al leer desde DB (posible tabla 'querys' inexistente): {e}")
    finally:
        cursor.close()
    return None

def escribir_a_db(fila: Row):
    """Guarda una nueva pregunta/respuesta en la DB (función que usaría el DB Writer)."""
    conn = obtener_conexion_db()
    cursor = conn.cursor()
    
    try:
        # Inserta en la tabla 'querys'
        cursor.execute(
            "INSERT INTO querys (score, title, body, answer) VALUES (%s, %s, %s, %s)",
            (fila.score, fila.title, fila.body, fila.answer)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error al escribir en DB: {e}")
        conn.rollback()
    finally:
        cursor.close()


# --- 6. LÓGICA DE KAFKA BAJO DEMANDA (CONFLUENT) ---

_kafka_producer = None # Inicializa globalmente como None

def get_kafka_producer(max_retries=5, delay=2) -> Producer:
    """Inicializa y retorna el productor de Confluent Kafka, bajo demanda."""
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
                'client.id': 'fastapi-orchestrator',
                'api.version.request': True
            }
            
            producer = Producer(producer_conf)
            producer.poll(timeout=1.0) # Forzar chequeo de metadatos
            
            _kafka_producer = producer
            logger.info("Kafka Producer inicializado con éxito bajo demanda.")
            return producer
            
        except Exception as e:
            logger.warning(f"Intento {i+1}/{max_retries}: Fallo al inicializar Kafka Producer: {e}")
            if i == max_retries - 1:
                logger.error("Fallo definitivo: No se pudo conectar a Kafka para publicar.")
                raise ConnectionError(f"Fallo al inicializar Kafka Producer después de {max_retries} intentos.")
            time.sleep(delay)
            
    raise ConnectionError("No se pudo establecer la conexión al productor de Kafka.")


def kafka_publish_question(question: str):
    """Publica la pregunta en el topic de Kafka 'questions'."""
    try:
        producer = get_kafka_producer() 
    except ConnectionError as e:
        raise Exception(f"No se pudo obtener el productor de Kafka: {e}")
        
    data = {"question": question}
    json_data = json.dumps(data).encode('utf-8')
    
    producer.produce(KAFKA_INPUT_TOPIC, value=json_data)
    producer.flush(timeout=5)
    
    logger.info(f"Pregunta enviada a Kafka topic '{KAFKA_INPUT_TOPIC}'")


# --- 7. FASTAPI ENDPOINTS ---
app = FastAPI(title="QA Orchestrator API - Confluent", version="1.0.7")

@app.get("/health")
def health():
    """Chequea la salud del servicio y la conexión a Postgres."""
    try:
        # Prueba si la conexión es funcional
        obtener_conexion_db().cursor().execute("SELECT 1") 
        estado_postgres = True
    except Exception:
        estado_postgres = False
        
    return {"status": "OK" if estado_postgres else "Error", "db_status": estado_postgres}

@app.post("/ask", response_model=AskResponse)
async def ask(solicitud: AskRequest):
    """Procesa una pregunta: busca en DB, si no existe, la publica en Kafka para procesamiento asíncrono."""
    
    pregunta = solicitud.question.strip()
    if not pregunta:
        raise HTTPException(status_code=400, detail="Pregunta vacía")

    # 1. Consultar base de datos
    try:
        fila = leer_desde_db(pregunta)
        if fila:
            # 🚨 Si se encuentra en DB, devuelve 200 OK 🚨
            return AskResponse(source="db", row=fila, message=fila_a_mensaje(fila))
            
    except Exception as e:
        # Si falla la lectura (ej: tabla inexistente), logueamos y continuamos a Kafka
        logger.warning(f"Fallo en la lectura de DB: {e}. Continuará con Kafka.")
        
    # 2. Publicar en Kafka (Si no se encontró en DB o si la lectura falló)
    try:
        kafka_publish_question(pregunta)
        
        # Respuesta inmediata al cliente (ACK 200 OK)
        fila_ack = Row(
            score=0, 
            title=pregunta, 
            body=None, 
            answer="Pregunta enviada a la cola de LLM para procesamiento y calificación asíncrona."
        )
        
        return AskResponse(source="llm_queue", row=fila_ack, message=fila_a_mensaje(fila_ack))
    
    except Exception as e:
        # 🚨 Este es el fallo 500 final: ni la DB ni Kafka funcionaron 🚨
        logger.exception("Error fatal: No se pudo publicar el mensaje en Kafka.")
        raise HTTPException(status_code=500, detail=f"Error al enviar la pregunta a Kafka: {e}")


if __name__ == "__main__":
    # La ejecución normal debe ser vía uvicorn en el CMD del Dockerfile
    # Solo para pruebas directas
    print("Iniciando main.py...")
