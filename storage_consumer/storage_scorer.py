import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer # CAMBIO: Nueva librería
from google import genai
from google.genai.errors import APIError

# --- Configuración ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = os.getenv('KAFKA_LLM_OUTPUT_TOPIC', 'llm_answers')
OUTPUT_TOPIC = os.getenv('KAFKA_FINAL_RESULTS_TOPIC', 'final_results')
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
        group_id='scorer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

    print(f"Storage Scorer iniciado. Usando kafka-python. Calificando {INPUT_TOPIC}")

except Exception as e:
    print(f"Error CRÍTICO al inicializar Kafka Scorer: {e}")
    raise

def score_answer_with_gemini(question: str, answer: str) -> int:
    """Usa Gemini para puntuar la respuesta según los criterios dados."""
    
    CRITERIA = (
        "- Exactitud (40%)\n"
        "- Integridad (25%)\n"
        "- Claridad (20%)\n"
        "- Concisión (10%)\n"
        "- Utilidad (5%)"
    )

    prompt = (
        f"Califica la siguiente respuesta a la pregunta del 1 al 10, basándote únicamente en estos criterios y sus pesos: \n{CRITERIA}\n\n"
        f"Pregunta: {question}\n"
        f"Respuesta a Calificar: {answer}\n\n"
        "Devuelve SOLO el número entero del puntaje (ej: 8)."
    )

    try:
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt]
        )
        score_text = response.text.strip()
        
        # Intenta parsear y limitar el puntaje a [1, 10]
        score = int(float(score_text))
        return max(1, min(10, score))
    except Exception as e:
        print(f"Error al calificar con Gemini (respuesta no numérica: {score_text}): {e}")
        return 1 # Puntaje por defecto muy bajo si falla la calificación

# --- Bucle Principal ---
while True:
    try:
        for msg in consumer:
            response_data = msg.value
            question = response_data.get("question")
            answer = response_data.get("answer")
            
            if question and answer:
                print(f"Calificando respuesta para: '{question[:50]}...'")
                
                # 1. Obtener puntaje de Gemini
                final_score = score_answer_with_gemini(question, answer)
                
                print(f"Puntaje asignado: {final_score}")

                # 2. Publicar en el topic FINAL_RESULTS
                final_result = {
                    "question": question,
                    "answer": answer,
                    "score": final_score
                }

                future = producer.send(OUTPUT_TOPIC, final_result)
                future.get(timeout=10) 

    except Exception as e:
        print(f"Error en Storage Scorer: {e}")
        time.sleep(1)