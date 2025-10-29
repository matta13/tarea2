import os
import json
import time
import sys
# Añade el path temporal para importar las funciones de DB desde main.py
sys.path.append('/temp') 

from kafka import KafkaConsumer # CAMBIO: Nueva librería
from main import obtener_conexion_db, escribir_a_db, Row 

# --- Configuración ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = os.getenv('KAFKA_FINAL_RESULTS_TOPIC', 'final_results')

# --- Inicialización de Kafka (Variables Globales) ---
consumer = None

try:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='db_writer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"Error CRÍTICO al inicializar Kafka DB Writer: {e}")
    raise
# -------------------------------------------

print(f"DB Writer iniciado. Usando kafka-python. Escuchando en {INPUT_TOPIC}")

# --- Bucle Principal ---
while True:
    try:
        for msg in consumer:
            final_result = msg.value
            
            question = final_result.get("question")
            answer = final_result.get("answer")
            score = final_result.get("score")
            
            if question and answer and score is not None:
                print(f"Recibiendo resultado final (Score: {score}) para: '{question[:50]}...'")
                
                # Crear la fila para la base de datos
                fila = Row(score=score, title=question, body=None, answer=answer)
                
                # Guardar en la base de datos (usando la función importada)
                escribir_a_db(fila)
                print(f"Guardado exitoso en DB.")
                
    except Exception as e:
        print(f"Error al procesar mensaje de DB Writer: {e}")
        time.sleep(1)