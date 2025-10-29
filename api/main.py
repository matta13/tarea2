import hashlib
import json
import os
import time
from typing import Optional, Literal

import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer # CAMBIO: Nuevo: Importación de kafka-python

# --- Configuración y Conexión ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')

# --- Modelos Pydantic ---

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

# --- Conexión a DB (Postgres) ---

_conexion_db = None
def obtener_conexion_db(max_retries: int = 5, delay: int = 2):
    """Retorna una conexión activa a PostgreSQL, intentando reconectar."""
    global _conexion_db
    if _conexion_db is not None and not _conexion_db.closed:
        return _conexion_db

    for i in range(max_retries):
        try:
            _conexion_db = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            # print("Conexión a PostgreSQL establecida con éxito.")
            return _conexion_db
        except Exception as e:
            if i < max_retries - 1:
                # print(f"Intento {i+1}/{max_retries}: Error al conectar a DB. Reintentando en {delay}s. Error: {e}")
                time.sleep(delay)
            else:
                raise ConnectionError(f"Fallo al conectar con la base de datos después de {max_retries} intentos: {e}")
    
    raise ConnectionError("No se pudo establecer la conexión a la base de datos.")

# --- Funciones de DB (Usan la tabla 'querys') ---

def leer_desde_db(pregunta: str) -> Optional[Row]:
    """Busca una respuesta en la DB por título (pregunta)."""
    conn = obtener_conexion_db()
    cursor = conn.cursor()
    
    try:
        # Busca directamente por 'title'
        cursor.execute(
            "SELECT score, title, body, answer FROM querys WHERE title = %s",
            (pregunta,)
        )
        registro = cursor.fetchone()
        if registro:
            return Row(score=registro[0], title=registro[1], body=registro[2], answer=registro[3])
    except Exception as e:
        # Este error es esperado si la tabla no existe (antes de la primera ejecución)
        print(f"Error al leer desde DB: {e}")
    finally:
        cursor.close()
    return None

def escribir_a_db(fila: Row):
    """Guarda una nueva pregunta/respuesta en la DB (usada por el DB Writer)."""
    conn = obtener_conexion_db()
    cursor = conn.cursor()
    
    try:
        # Inserta solo en las columnas existentes (score, title, body, answer)
        cursor.execute(
            "INSERT INTO querys (score, title, body, answer) VALUES (%s, %s, %s, %s)",
            (fila.score, fila.title, fila.body, fila.answer)
        )
        conn.commit()
    except Exception as e:
        # Esto puede ocurrir si se intenta insertar la misma pregunta (title duplicado)
        print(f"Error al escribir en DB: {e}")
        conn.rollback()
    finally:
        cursor.close()

# --- Kafka Producer Initialization ---

# Declaración Global Explicita (para evitar NameError)
producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
except Exception as e:
    print(f"Advertencia: No se pudo inicializar Kafka Producer: {e}")
    # Si falla, producer sigue siendo None

def kafka_publish_question(question: str):
    """Publica la pregunta en el topic de Kafka 'questions'."""
    if producer is None:
        raise Exception("El productor de Kafka no está inicializado.")
        
    data = {"question": question}
    
    future = producer.send(KAFKA_INPUT_TOPIC, data)
    future.get(timeout=1) # Bloqueo breve para confirmar el envío

# --- FastAPI Endpoints ---
app = FastAPI(title="QA Orchestrator API - Kafka-Python", version="1.0.7")

@app.get("/health")
def health():
    """Chequea la salud del servicio y la conexión a Postgres."""
    try:
        obtener_conexion_db().cursor().execute("SELECT 1")
        estado_postgres = True
    except Exception:
        estado_postgres = False
        
    # Asumimos que la API está sana si al menos Postgres funciona o si el servicio está arriba
    return {"ok": estado_postgres}

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
            return AskResponse(source="db", row=fila, message=fila_a_mensaje(fila))
    except ConnectionError as e:
        # Esto captura el error de reintento fallido de Postgres
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Maneja otros errores de lectura (como tabla inexistente en la primera ejecución)
        print(f"Error durante la lectura inicial de DB (continuando a Kafka): {e}")


    # 2. Publicar en Kafka (Procesamiento asíncrono)
    try:
        kafka_publish_question(pregunta)
        
        # Respuesta inmediata al cliente (ACK)
        fila_ack = Row(
            score=0, 
            title=pregunta, 
            body=None, 
            answer="Pregunta enviada a la cola de LLM para procesamiento y calificación asíncrona."
        )
        
        return AskResponse(source="llm_queue", row=fila_ack, message=fila_a_mensaje(fila_ack))
    
    except Exception as e:
        print(f"Error al publicar en Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Error al enviar la pregunta a Kafka: {e}. Revise los servicios de Kafka.")