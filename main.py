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
from src import redis_tools
from theetl.etl import ETL
from src.redis_tools import filter_unique_transactions, acquire_lock, release_lock

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)





manager = Manager()
checksums_in_process = manager.list()  # Estado compartido
lock = Lock()  # Lock para evitar condiciones de carrera



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
        except redis_tools.ConnectionError:
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







@app.post("/")
async def process_event(request: Request):
    """Endpoint para recibir y procesar eventos de Pub/Sub."""
    try:
        etl = ETL('config/transactions.yaml',"transactions")
        
        body = await request.json()
        logging.info(f"Evento recibido: {body}")

        # Parsear y validar el cuerpo del evento
        event_data = parse_event_body(body)
        bucket_name, file_path = validate_event_data(event_data)

        partitions = parse_partitions(file_path)
        partitions['file_name'] = file_path 
        
        # Run ETL: get data from bigquery
        rows_to_process=etl.run_extraction(partitions)

        #raw data
        #rows_to_process = query_raw_transactions(partitions, file_path)
        logging.info(f"Transacciones ingestadas en raw: {len(rows_to_process)}\n")
        for row in rows_to_process:
            logging.info(f"Transacción recuperada: {row}")

        #transformations
        transactions=etl.run_transformations(rows_to_process)
        logging.info(f"Transacciones después de transformaciones: {len(transactions)}\n")
        for transaction in transactions:
            logging.info(f"Transacción transformada: {transaction}")

        # Detectar anomalías
        #anomalies = process_anomalies(rows_to_process, bigquery_data)
        #filtros antes de subir a redis y bigquery
        

        unique_rows = filter_unique_transactions(redis_client, rows_to_process)

        # Procesar transacciones únicas
        process_transactions(unique_rows)

        return {"message": f"Procesadas {len(unique_rows)} transacciones, {len(anomalies)} anomalías detectadas."}

    except Exception as e:
        logging.error(f"Error procesando el evento: {str(e)}")
        raise HTTPException(status_code=500, detail="Error procesando el evento.")

if __name__ == "__main__":
    check_redis_connection()
    uvicorn.run(app, host="0.0.0.0", port=8081)
