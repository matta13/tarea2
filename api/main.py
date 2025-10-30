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
load_dotenv("/app/.env.api") 

# --- 3. CONFIGURACIÓN Y CONEXIONES ---
DB_HOST = os.getenv("POSTGRES_HOST")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC')
KAFKA_TOPIC_RESPONSES = os.getenv('KAFKA_FINAL_RESULTS_TOPIC')

DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# rutas donde se copiarán los archivos SQL dentro del contenedor qa_api
SCHEMA_SQL_PATH = "./postgres/01schema.sql" 
DATA_SQL_PATH = "./postgres/02data.sql" 

def create_table_and_insert_data(db_conn_string: str):
    """
    Conecta a PostgreSQL, ejecuta 01schema.sql (creación de tabla)
    y luego ejecuta 02data.sql (carga de datos) solo si la tabla está vacía.
    """
    logger.info("Iniciando esquema y datos de 'querys' desde archivos SQL...")
    conn = None
    
    # Intenta la conexión y ejecución hasta 5 veces para esperar a que Postgres inicie
    for i in range(5): 
        try:
            conn = psycopg2.connect(db_conn_string)
            conn.autocommit = True
            
            # --- 1. Crear la Tabla (01schema.sql) ---
            if not os.path.exists(SCHEMA_SQL_PATH):
                logger.error(f"ERROR: Archivo de esquema no encontrado en {SCHEMA_SQL_PATH}. Fallo de inicialización.")
                return 

            with open(SCHEMA_SQL_PATH, 'r') as f:
                schema_sql = f.read()
            
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            logger.info("Esquema de 'querys' cargado con éxito desde 01schema.sql.")
            
            # --- 2. Insertar Datos (02data.sql) ---
            
            # Verificar si la tabla ya tiene datos
            with conn.cursor() as cur:
                # Usamos public.querys para asegurar que el esquema esté especificado
                cur.execute("SELECT COUNT(*) FROM public.querys;") 
                count = cur.fetchone()[0]
                
            if count == 0:
                logger.info("Tabla vacía. Cargando datos desde 02data.sql...")
                
                if not os.path.exists(DATA_SQL_PATH):
                    logger.error(f"ERROR: Archivo de datos no encontrado en {DATA_SQL_PATH}. Saltando inserción de datos.")
                    return
                
                with open(DATA_SQL_PATH, 'r') as f:
                    data_sql = f.read()
                
                with conn.cursor() as cur:
                    cur.execute(data_sql)
                logger.info("Datos de 02data.sql cargados con éxito.")
                
            else:
                logger.info(f"Tabla 'querys' ya contiene {count} filas. Saltando la carga de 02data.sql.")
                
            return 

        except OperationalError as e:
            logger.warning(f"Fallo de conexión a la DB (Intento {i+1}/5). Reintentando en 5s. Error: {e}")
            time.sleep(5) 
        except Exception as e:
            logger.error(f"Error desconocido al inicializar DB desde archivos: {e}")
            break 
        finally:
            if conn:
                conn.close()
    
    # Si el bucle termina sin éxito después de los reintentos
    logger.error("ERROR FATAL: No se pudo inicializar la base de datos.")
    
# Llamar a la función al inicio de la ejecución del módulo:
create_table_and_insert_data(DB_CONNECTION_STRING) 

# --- 4. FUNCIÓN UTILITY DB ---
class Row(BaseModel):
    score: int
    title: str
    body: Optional[str]
    answer: str

def fila_a_mensaje(fila: Row) -> str:
    return f"Pregunta: {fila.title}\nPuntaje: {fila.score}\nRespuesta: {fila.answer}"

def leer_desde_db(pregunta: str) -> Optional[Row]:
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        with conn.cursor() as cur:
            # Usamos public.querys para coincidir con la creación del esquema
            query = "SELECT score, title, body, answer FROM public.querys WHERE UPPER(title) = UPPER(%s)" 
            cur.execute(query, (pregunta,))
            fila = cur.fetchone()
            
            if fila:
                return Row(score=fila[0], title=fila[1], body=fila[2], answer=fila[3])
            return None
    finally:
        if conn:
            conn.close()

# --- 5. FUNCIÓN UTILITY KAFKA ---
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'fastapi-orchestrator',
    'api.version.request': 'true'
}
kafka_producer = None 

def get_kafka_producer():
    global kafka_producer
    if kafka_producer is None:
        try:
            kafka_producer = Producer(producer_config)
            logger.info("Kafka Producer inicializado con éxito bajo demanda.")
        except Exception as e:
            logger.exception("Fallo al inicializar Kafka Producer.")
            raise e
    return kafka_producer

def kafka_publish_question(pregunta: str):
    """Publica la pregunta en el topic de Kafka."""
    producer = get_kafka_producer()
    mensaje = json.dumps({"title": pregunta, "body": None})
    
    producer.produce(
        topic=KAFKA_INPUT_TOPIC, 
        value=mensaje.encode('utf-8'),
        key=str(time.time()).encode('utf-8'),
        callback=delivery_report
    )
    producer.flush(timeout=10)
    logger.info(f"Pregunta enviada a Kafka topic '{KAFKA_INPUT_TOPIC}'")

def delivery_report(err, msg):
    """Reporte de entrega de Kafka."""
    if err is not None:
        logger.error(f'Fallo al entregar el mensaje a Kafka: {err}')

# --- 6. MODELOS Pydantic ---
class AskRequest(BaseModel):
    pregunta: str

class AskResponse(BaseModel):
    source: Literal["db", "llm_queue"]
    row: Row
    message: str

# --- 7. FASTAPI APP ---
app = FastAPI()

@app.post("/ask", response_model=AskResponse)
async def ask_question(request: AskRequest):
    pregunta = request.pregunta.strip()
    
    if not pregunta:
        raise HTTPException(status_code=400, detail="Pregunta vacía")

    # 1. Consultar base de datos
    try:
        fila = leer_desde_db(pregunta)
        if fila:
            return AskResponse(source="db", row=fila, message=fila_a_mensaje(fila))
            
    except Exception as e:
        logger.warning(f"Fallo en la lectura de DB: {e}. Continuará con Kafka.")
        
    # 2. Publicar en Kafka, Si no se encontró en DB o si la lectura falló
    try:
        kafka_publish_question(pregunta)
        
        fila_ack = Row(
            score=0, 
            title=pregunta, 
            body=None, 
            answer="Pregunta enviada a la cola de LLM para procesamiento y calificación asíncrona."
        )
        
        return AskResponse(source="llm_queue", row=fila_ack, message=fila_a_mensaje(fila_ack))
    
    except Exception as e:
        logger.exception("Error fatal: No se pudo publicar el mensaje en Kafka.")
        raise HTTPException(status_code=500, detail="Error de infraestructura: No se pudo procesar la pregunta.")


