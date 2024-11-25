import logging
import sys
import time
from multiprocessing import Manager, Lock
from fastapi import FastAPI, HTTPException, Request
from google.cloud import pubsub_v1, bigquery
import uvicorn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import base64
import json

raw_data = [
    {
        "checksum": "22222",
        "transaction_date": "2024-11-20",
        "concept": "traspaso actinver - Receptor: BBVA MEXICO, Beneficiario: BANCO ACTINVER SA POR CTADEL FID 2342*, Cuenta Ref: 012180001107719376, Clave Rastreo: 202411204013300000000029933186, Ref: 0000001",
        "amount": -50000,
        "account_number": "133180000075522355",
        "currency": "MXN",
        "bank": "actinver"
    }
]
bigquery_data = [
    {
        "checksum": "673eb8b4cced4706752afd3e",
        "transaction_date": "2024-11-20",
        "concept": "traspaso actinver - Receptor: BBVA MEXICO, Beneficiario: BANCO ACTINVER SA POR CTADEL FID 2342*, Cuenta Ref: 012180001107719376, Clave Rastreo: 202411204013300000000029933186, Ref: 0000001",
        "amount": -500000,
        "account_number": "133180000075522355",
        "currency": "MXN",
        "bank": "actinver"
    }
]

manager = Manager()
checksums_in_process = manager.list()  # Estado compartido
lock = Lock()  # Lock para evitar condiciones de carrera

SIMILARITY_THRESHOLD = 0.9
FIELDS = {
    "concept": 0.8,        # Texto
    "amount": 0.1,         # Numérico
    "account_number": 0.0, # Exacto
    "bank": 0.0,           # Exacto
    "transaction_date": 0.1 # Exacto
}

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = FastAPI()

def calculate_field_similarity(field1, field2):
    """Calcula la similitud entre dos campos."""
    if isinstance(field1, str) and isinstance(field2, str):
        # Texto: Usar cosine similarity
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform([field1, field2])
        return cosine_similarity(tfidf_matrix)[0, 1]
    elif isinstance(field1, (int, float)) and isinstance(field2, (int, float)):
        # Números: Diferencia relativa
        return 1 - abs(field1 - field2) / max(abs(field1), abs(field2))
    else:
        # Exact match para otros tipos
        return 1 if field1 == field2 else 0

def calculate_combined_similarity(tx1, tx2):
    """Calcula la similitud combinada entre dos transacciones."""
    total_similarity = 0
    for field, weight in FIELDS.items():
        similarity = calculate_field_similarity(tx1.get(field), tx2.get(field))
        logging.debug(f"Campo: {field}, Similitud: {similarity:.3f}, Peso: {weight:.1f}")
        total_similarity += weight * similarity
    return total_similarity

def detect_anomalies(transactions1, transactions2):
    """Detecta anomalías entre dos conjuntos de transacciones."""
    logging.info(f"Detectando anomalías entre {len(transactions1)} y {len(transactions2)} transacciones.")
    anomalies = []
    for tx1 in transactions1:
        for tx2 in transactions2:
            similarity = calculate_combined_similarity(tx1, tx2)
            logging.info(f"Similitud combinada: {similarity:.3f}")

            if similarity >= SIMILARITY_THRESHOLD:
                anomalies.append({
                    "transaction_1": tx1,
                    "transaction_2": tx2,
                    "similarity_score": similarity
                })
                logging.info(f"Anomalía detectada: TX1: {tx1}\n TX2: {tx2}\n Similitud: {similarity:.3f}")

    logging.info(f"Anomalías detectadas: {len(anomalies)}")
    return anomalies

@app.post("/")
async def process_event(request: Request):
    """Endpoint para recibir y procesar eventos de Pub/Sub."""
    try:
        body = await request.json()
        logging.info(f"Evento recibido: {body}")

        # Extraer mensaje de Pub/Sub
        message = body.get("message", {})
        if not message:
            raise HTTPException(
                status_code=400, detail="El evento no contiene un mensaje válido."
            )

        # Decodificar datos en Base64
        data_encoded = message.get("data")
        if not data_encoded:
            raise HTTPException(
                status_code=400, detail="El evento no contiene datos codificados."
            )

        decoded_data = base64.b64decode(data_encoded).decode("utf-8")
        logging.info(f"Datos decodificados: {decoded_data}")

        # Convertir la cadena JSON decodificada en un diccionario
        event_data = json.loads(decoded_data)

        # Validar estructura de datos del evento
        bucket_name = event_data.get("bucket")
        file_path = event_data.get("name")
        if not bucket_name or not file_path:
            raise HTTPException(
                status_code=400,
                detail="El evento no contiene los campos requeridos (bucket, name).",
            )

        logging.info(f"Archivo detectado: gs://{bucket_name}/{file_path}")

        # Procesar transacciones
        start_time = time.time()
        anomalies = detect_anomalies(raw_data, bigquery_data)
        elapsed_time = time.time() - start_time
        logging.info(f"Detección completada en {elapsed_time:.3f} segundos.")

        if anomalies:
            for anomaly in anomalies:
                logging.info(f"Anomalía detectada: {json.dumps(anomaly, indent=2)}")

        return {"message": f"Procesadas {len(raw_data)} transacciones, {len(anomalies)} anomalías detectadas."}

    except Exception as e:
        logging.error(f"Error procesando el evento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error procesando el evento.")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
