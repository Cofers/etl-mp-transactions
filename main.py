import logging
import sys
import time
from multiprocessing import Manager, Lock
from fastapi import FastAPI, HTTPException, Request
from google.cloud import pubsub_v1, bigquery
from src.ai import detect_anomalies
from src.utils import process_transactions, parse_partitions
from src.bigquery import query_raw_transactions
import uvicorn
import base64
import json
import redis

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
LOCK_EXPIRY_SECONDS = 5


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

def check_redis_connection():
    """Verifica la conexión a Redis antes de iniciar la aplicación."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            redis_client.ping()
            logging.info("Conexión a Redis exitosa.")
            return
        except redis.ConnectionError:
            logging.warning(f"Intento {attempt + 1}/{max_retries}: Redis no disponible. Reintentando en 2 segundos...")
            time.sleep(2)
    logging.error("No se pudo conectar a Redis después de múltiples intentos. Cerrando la aplicación.")
    sys.exit(1)

def parse_event_body(body):
    """Parsea y valida el cuerpo del evento."""
    message = body.get("message", {})
    if not message:
        raise HTTPException(
            status_code=400, detail="El evento no contiene un mensaje válido."
        )

    data_encoded = message.get("data")
    if not data_encoded:
        raise HTTPException(
            status_code=400, detail="El evento no contiene datos codificados."
        )

    decoded_data = base64.b64decode(data_encoded).decode("utf-8")
    logging.info(f"Datos decodificados: {decoded_data}")

    return json.loads(decoded_data)

def validate_event_data(event_data):
    """Valida los campos necesarios en el evento."""
    bucket_name = event_data.get("bucket")
    file_path = event_data.get("name")
    if not bucket_name or not file_path:
        raise HTTPException(
            status_code=400,
            detail="El evento no contiene los campos requeridos (bucket, name).",
        )
    logging.info(f"Archivo detectado: gs://{bucket_name}/{file_path}")
    return bucket_name, file_path

def process_anomalies(raw_data, bigquery_data):
    """Detecta anomalías entre los conjuntos de datos."""
    start_time = time.time()
    anomalies = detect_anomalies(raw_data, bigquery_data)
    elapsed_time = time.time() - start_time
    logging.info(f"Detección completada en {elapsed_time:.3f} segundos.")

    if anomalies:
        for anomaly in anomalies:
            logging.info(f"Anomalía detectada: {json.dumps(anomaly, indent=2)}")

    return anomalies


def acquire_lock(key):
    """Intenta adquirir un lock en Redis."""
    lock_key = f"lock:{key}"
    is_locked = redis_client.set(lock_key, "1", nx=True, ex=LOCK_EXPIRY_SECONDS)
    return is_locked

def release_lock(key):
    """Libera el lock en Redis."""
    lock_key = f"lock:{key}"
    redis_client.delete(lock_key)

def store_checksum_atomic(checksum):
    """Guarda un checksum en Redis con exclusión mutua."""
    lock_key = f"checksum:{checksum}"
    if acquire_lock(lock_key):
        try:
            redis_client.sadd("processed_checksums", checksum)
            logging.info(f"Checksum almacenado: {checksum}")
        finally:
            release_lock(lock_key)
    else:
        logging.warning(f"No se pudo adquirir lock para checksum: {checksum}")

def is_checksum_processed_atomic(checksum):
    """Verifica si un checksum ya fue procesado."""
    return redis_client.sismember("processed_checksums", checksum)


def filter_unique_transactions(rows_to_process):
    """Filtra las transacciones únicas utilizando Redis."""
    unique_rows = []
    for row in rows_to_process:
        checksum = row['checksum']
        if not is_checksum_processed_atomic(checksum):
            unique_rows.append(row)
            store_checksum_atomic(checksum)
        else:
            logging.info(f"Checksum ya procesado: {checksum}")

    logging.info(f"Transacciones únicas a procesar: {len(unique_rows)}")
    return unique_rows



@app.post("/")
async def process_event(request: Request):
    """Endpoint para recibir y procesar eventos de Pub/Sub."""
    try:
        body = await request.json()
        logging.info(f"Evento recibido: {body}")

        # Parsear y validar el cuerpo del evento
        event_data = parse_event_body(body)
        bucket_name, file_path = validate_event_data(event_data)

        partitions = parse_partitions(file_path)

        #raw data
        rows_to_process = query_raw_transactions(partitions, file_path)
        logging.info(f"Transacciones ingestadas en raw: {len(rows_to_process)}\n")
        #for row in rows_to_process:
        #    logging.info(f"Transacción recuperada: {row}")

        
        # Detectar anomalías
        anomalies = process_anomalies(rows_to_process, bigquery_data)


        unique_rows = filter_unique_transactions(rows_to_process)

        # Procesar transacciones únicas
        process_transactions(unique_rows)

        return {"message": f"Procesadas {len(unique_rows)} transacciones, {len(anomalies)} anomalías detectadas."}

    except Exception as e:
        logging.error(f"Error procesando el evento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error procesando el evento.")

if __name__ == "__main__":
    check_redis_connection()
    uvicorn.run(app, host="0.0.0.0", port=8081)
