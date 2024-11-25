import logging
import sys
import time
from multiprocessing import Manager, Lock
from fastapi import FastAPI, HTTPException, Request
from google.cloud import pubsub_v1, bigquery
import uvicorn
from src.bigquery import query_raw_transactions
from src.utils import parse_partitions
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import base64
import json


fixed_data = [
    {'checksum': '673eb8b4cced4706752afd3e', 'transaction_date': '2024-11-20', 
     'concept': 'traspaso actinver - Receptor: BBVA MEXICO, Beneficiario: BANCO ACTINVER SA POR CTADEL FID 2342*, Cuenta Ref: 012180001107719376, Clave Rastreo: 202411204013300000000029933186, Ref: 0000001', 
     'amount': -500000, 'account_number': '133180000075522355', 'currency': 'MXN', 'bank': 'actinver'}
]
# Inicializar estado compartido y lock
manager = Manager()
checksums_in_process = manager.list()  # Estado compartido
lock = Lock()  # Lock para evitar condiciones de carrera

COSINE_THRESHOLD = 0.9
AMOUNT_THRESHOLD = 1
DATE_WEIGHT = 0.3
TEXT_WEIGHT = 0.5
AMOUNT_WEIGHT = 0.2

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# App FastAPI
app = FastAPI()

def calculate_similarity(tx1, tx2):
    """Calcula la similitud entre dos transacciones."""
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform([tx1['concept'], tx2['concept']])
    text_similarity = cosine_similarity(tfidf_matrix)[0, 1]

    amount_diff = abs(tx1['amount'] - tx2['amount'])
    amount_similarity = max(0, 1 - amount_diff / AMOUNT_THRESHOLD)

    date_similarity = 1 if tx1['transaction_date'] == tx2['transaction_date'] else 0

    total_similarity = (
        TEXT_WEIGHT * text_similarity +
        AMOUNT_WEIGHT * amount_similarity +
        DATE_WEIGHT * date_similarity
    )
    return total_similarity

def detect_similar_transactions(transactions1, transactions2, similarity_threshold=0.8):
    """Detecta transacciones similares entre dos conjuntos."""
    similar_pairs = []
    for tx1 in transactions1:
        for tx2 in transactions2:
            similarity = calculate_similarity(tx1, tx2)
            if similarity >= similarity_threshold:
                similar_pairs.append({
                    'transaction_1': tx1,
                    'transaction_2': tx2,
                    'similarity_score': similarity
                })
    return similar_pairs

def process_transactions(transactions):
    """Procesa las transacciones únicas (transformación, inserción, etc.)."""
    logging.info(f"Procesando {len(transactions)} transacciones...")
    for transaction in transactions:
        logging.info(f"Procesando transacción con checksum: {transaction['checksum']}")
    logging.info("Procesamiento completado.")

@app.post("/")
async def process_event(request: Request):
    """Endpoint para recibir y procesar eventos de Pub/Sub."""
    try:
        body = await request.json()
        logging.info(f"Evento recibido: {body}")

        # Extraer mensaje de Pub/Sub
        message = body.get("message", {})
        if not message:
            raise HTTPException(status_code=400, detail="El evento no contiene un mensaje válido.")

        # Decodificar datos en Base64
        data_encoded = message.get("data")
        if not data_encoded:
            raise HTTPException(status_code=400, detail="El evento no contiene datos codificados.")

        decoded_data = b64decode(data_encoded).decode("utf-8")
        logging.info(f"Datos decodificados: {decoded_data}")

        # Convertir la cadena JSON decodificada en un diccionario
        event_data = json.loads(decoded_data)

        # Validar estructura de datos del evento
        bucket_name = event_data.get("bucket")
        file_path = event_data.get("name")
        if not bucket_name or not file_path:
            raise HTTPException(status_code=400, detail="El evento no contiene los campos requeridos (bucket, name).")

        logging.info(f"Archivo detectado: gs://{bucket_name}/{file_path}")

        # Parsear particiones del nombre del fichero
        partitions = parse_partitions(file_path)
        logging.info(f"Particiones detectadas: {partitions}")

        # Consultar BigQuery para obtener transacciones
        start_time = time.time()
        rows_to_process = query_raw_transactions(partitions)
        elapsed_time = time.time() - start_time
        logging.info(f"Query raw {elapsed_time:.3f} seconds.")
        logging.info(f"Número de transacciones recién ingestadas: {len(rows_to_process)}")

        # Similitud
        similar_transactions = detect_similar_transactions(rows_to_process, fixed_data)
        logging.info(f"Transacciones similares detectadas: {len(similar_transactions)}")

        for pair in similar_transactions:
            logging.info(f"\n---\nTransacción 1: {pair['transaction_1']}\n"
                         f"Transacción 2: {pair['transaction_2']}\n"
                         f"Similitud: {pair['similarity_score']:.2f}")

        # Identificar transacciones únicas usando lock
        with lock:
            unique_rows = [row for row in rows_to_process if row['checksum'] not in checksums_in_process]
            logging.info(f"Transacciones únicas a procesar tras filtro checksum local: {len(unique_rows)}")

            # Agregar checksums al estado compartido
            checksums_in_process.extend([row['checksum'] for row in unique_rows])

        # Procesar transacciones únicas
        for row in unique_rows:
            print(row)
        process_transactions(unique_rows)

        return {"message": f"Procesadas {len(unique_rows)} transacciones."}

    except Exception as e:
        logging.error(f"Error procesando el evento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error procesando el evento.")

if __name__ == "__main__":
    

    
    uvicorn.run(app, host="0.0.0.0", port=8081)
