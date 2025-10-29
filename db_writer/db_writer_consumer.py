import os
import time
import json
import sys
import logging
from typing import Optional
from dotenv import load_dotenv

# Dependencias para DB y Modelos
import psycopg2
from psycopg2 import OperationalError 
from pydantic import BaseModel 

# Importaciones de Kafka
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.error import KafkaError

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de Variables de Entorno
load_dotenv("/app/.env.api") 

# --- CONFIGURACIÓN DE DB ---
DB_HOST = os.getenv("POSTGRES_HOST")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Ruta del archivo de esquema para la inicialización
SCHEMA_SQL_PATH = "/app/01schema.sql"
# --- CONFIGURACIÓN DE KAFKA ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_LLM_OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')


# ----------------------------------------------------
# 1. MODELO DE DATOS
# ----------------------------------------------------
class Row(BaseModel):
    score: int
    title: str
    body: Optional[str]
    answer: str

# ----------------------------------------------------
# 2. LÓGICA DE BASE DE DATOS
# ----------------------------------------------------

def create_table_if_not_exists(db_conn_string: str):
    """Asegura que la tabla 'querys' exista al iniciar el servicio."""
    logger.info("Asegurando que la tabla 'querys' exista...")
    conn = None
    
    # Reintentar la conexión y ejecución
    for i in range(3): 
        try:
            conn = psycopg2.connect(db_conn_string)
            conn.autocommit = True
            
            # Leer y ejecutar el script de esquema
            if not os.path.exists(SCHEMA_SQL_PATH):
                logger.error(f"ERROR: Archivo de esquema no encontrado: {SCHEMA_SQL_PATH}.")
                return 

            with open(SCHEMA_SQL_PATH, 'r') as f:
                schema_sql = f.read()
            
            with conn.cursor() as cur:
                cur.execute(schema_sql)
            logger.info("Esquema de 'querys' asegurado.")
            return # Éxito

        except OperationalError as e:
            logger.warning(f"Fallo de conexión a la DB (Intento {i+1}/3). Reintentando en 5s. Error: {e}")
            time.sleep(5) 
        except Exception as e:
            logger.error(f"Error al inicializar DB: {e}")
            break 
        finally:
            if conn:
                conn.close()
    
    logger.error("ERROR FATAL: No se pudo inicializar la tabla 'querys'.")
    sys.exit(1) # Salir si no se puede conectar

def escribir_a_db(row: Row):
    """Escribe la fila de respuesta del LLM a la base de datos."""
    conn = None
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        conn.autocommit = True
        
        insert_query = """
        INSERT INTO public.querys (score, title, body, answer) 
        VALUES (%s, %s, %s, %s)
        """
        
        with conn.cursor() as cur:
            cur.execute(insert_query, (row.score, row.title, row.body, row.answer))
        
    except Exception as e:
        logger.error(f"Error al escribir en DB: {e}")
        raise # Re-lanzar para que el error sea visible en el log
    finally:
        if conn:
            conn.close()

# ----------------------------------------------------
# 3. KAFKA WORKER
# ----------------------------------------------------

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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue 
                else:
                    logger.error(f"Error al consumir: {msg.error()}")
                    continue

            # Procesamiento del Mensaje
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # 2. Convertir a Modelo Row y Escribir a DB
                fila = Row(
                    score=data.get('score', 0),
                    title=data.get('title', 'N/A'),
                    body=data.get('question_id'), # Usamos el ID como body
                    answer=data.get('answer', 'Respuesta no generada')
                )
                
                escribir_a_db(fila) # Llama a la función local corregida
                logger.info(f"Resultado LLM guardado en DB para pregunta: {fila.title}")
                
            except Exception as e:
                logger.error(f"Error procesando mensaje y escribiendo a DB: {e}")
                
    except Exception as e:
        logger.error(f"Error CRÍTICO en Kafka DB Writer: {e}")
        return

# ----------------------------------------------------
# 4. EJECUCIÓN PRINCIPAL
# ----------------------------------------------------
if __name__ == "__main__":
    # 1. Inicializar DB de forma segura antes de empezar a consumir
    create_table_if_not_exists(DB_CONNECTION_STRING)
    
    # 2. Iniciar el worker principal de Kafka
    kafka_db_writer_worker()
