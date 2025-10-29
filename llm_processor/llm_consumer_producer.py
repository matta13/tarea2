import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer # CAMBIO: Nueva librería
from google import genai
from google.genai.errors import APIError

# --- Configuración ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'questions')
OUTPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# --- Inicialización de Kafka (Variables Globales) ---
consumer = None
producer = None
gemini_client = None

try:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='llm_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    
    print(f"LLM Worker iniciado. Usando kafka-python. Publicando a {OUTPUT_TOPIC}")

except Exception as e:
    print(f"Error CRÍTICO al inicializar Kafka Worker: {e}")
    # Si la conexión falla, el worker lanzará la excepción y se reiniciará.
    raise


def get_gemini_answer(question: str) -> str:
    """Consulta a Gemini para obtener solo la respuesta (sin puntaje ni formato)."""
    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[question]
        )
        return response.text.strip()
    except APIError as e:
        print(f"Error de API de Gemini: {e}")
        return f"Error LLM: Falló la conexión con Gemini ({e})"
    except Exception as e:
        print(f"Error desconocido en LLM: {e}")
        return f"Error Desconocido en LLM: {e}"


# --- Bucle Principal ---
while True:
    try:
        # Itera sobre los mensajes del consumer
        for msg in consumer:
            question_data = msg.value
            question = question_data.get("question")
            
            if question:
                print(f"Procesando pregunta: '{question[:50]}...'")
                
                # 1. Obtener respuesta del LLM (Gemini)
                answer = get_gemini_answer(question)
                
                # 2. Publicar en el topic intermedio (llm_answers) para que el Scorer la tome
                response_data = {
                    "question": question,
                    "answer": answer
                }
                
                future = producer.send(OUTPUT_TOPIC, response_data)
                future.get(timeout=10) # Bloquea hasta que el mensaje se envíe
                
    except Exception as e:
        print(f"Error durante el consumo/producción de Kafka: {e}")
        time.sleep(1)