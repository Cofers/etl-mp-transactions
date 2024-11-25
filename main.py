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
# Inicializar estado compartido y lock
manager = Manager()
checksums_in_process = manager.list()  # Estado compartido
lock = Lock()  # Lock para evitar condiciones de carrera

COSINE_THRESHOLD = 0.9
AMOUNT_THRESHOLD = 1
DATE_WEIGHT = 0.3
TEXT_WEIGHT = 0.5
AMOUNT_WEIGHT = 0.2
REMAINING_WEIGHT = 0.3
WARNING_THRESHOLD = 0.8 

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# App FastAPI
app = FastAPI()


def calculate_similarity(tx1, tx2):
    """Calcula la similitud combinada entre dos transacciones."""
    logging.info(f"Comparando transacciones:\nTX1: {tx1}\nTX2: {tx2}")

    # Vectorización del texto (concept)
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform([tx1["concept"], tx2["concept"]])
    text_similarity = cosine_similarity(tfidf_matrix)[0, 1]
    logging.debug(f"Similitud de texto: {text_similarity:.3f}")

    # Diferencia relativa en amount
    if tx1["amount"] and tx2["amount"]:
        amount_diff = abs(tx1["amount"] - tx2["amount"]) / max(abs(tx1["amount"]), abs(tx2["amount"]))
        amount_similarity = 1 - amount_diff
    else:
        amount_similarity = 1 if tx1["amount"] == tx2["amount"] else 0
    logging.debug(f"Diferencia en amount: {amount_diff if 'amount_diff' in locals() else 'NA'}, Similitud en amount: {amount_similarity:.3f}")

    # Diferencia relativa en reported_remaining
    if "reported_remaining" in tx1 and "reported_remaining" in tx2:
        remaining_diff = abs(tx1["reported_remaining"] - tx2["reported_remaining"]) / max(tx1["reported_remaining"], tx2["reported_remaining"])
        remaining_similarity = 1 - remaining_diff
    else:
        remaining_similarity = 1 if "reported_remaining" not in tx1 and "reported_remaining" not in tx2 else 0
    logging.debug(f"Diferencia en reported_remaining: {remaining_diff if 'remaining_diff' in locals() else 'NA'}, Similitud en reported_remaining: {remaining_similarity:.3f}")

    # Validar pesos (asegurar que sumen 1)
    weight_sum = TEXT_WEIGHT + AMOUNT_WEIGHT + REMAINING_WEIGHT
    if weight_sum != 1:
        raise ValueError(f"Los pesos no suman 1. Suma actual: {weight_sum}")

    # Métrica combinada con pesos
    total_similarity = (
        TEXT_WEIGHT * text_similarity +
        AMOUNT_WEIGHT * amount_similarity +
        REMAINING_WEIGHT * remaining_similarity
    )
    logging.info(
        f"Similitud total calculada: {total_similarity:.3f} "
        f"(Texto: {text_similarity:.3f}, Monto: {amount_similarity:.3f}, Remaining: {remaining_similarity:.3f})"
    )
    return total_similarity, text_similarity, amount_similarity, remaining_similarity


def detect_anomalies(transactions1, transactions2):
    """Detecta anomalías entre dos conjuntos de transacciones."""
    logging.info(f"Detectando anomalías entre {len(transactions1)} y {len(transactions2)} transacciones.")
    anomalies = []
    warnings = []  # Para registrar advertencias de similitud moderada

    for tx1 in transactions1:
        for tx2 in transactions2:
            total_similarity, text_sim, amount_sim, remaining_sim = calculate_similarity(tx1, tx2)
            logging.info(f"Similitud total: {total_similarity:.3f}")

            if total_similarity >= COSINE_THRESHOLD:
                anomaly_reason = []

                # Diferencia en reported_remaining
                if "reported_remaining" in tx1 and "reported_remaining" in tx2:
                    if tx1["reported_remaining"] != tx2["reported_remaining"]:
                        anomaly_reason.append("Diferencia en reported_remaining")

                # Diferencia significativa en amount
                if abs(tx1["amount"] - tx2["amount"]) > AMOUNT_THRESHOLD:
                    anomaly_reason.append("Diferencia significativa en amount")

                if anomaly_reason:
                    anomalies.append({
                        "transaction_1": tx1,
                        "transaction_2": tx2,
                        "similarity_score": total_similarity,
                        "text_similarity": text_sim,
                        "amount_similarity": amount_sim,
                        "remaining_similarity": remaining_sim,
                        "anomaly_reason": ", ".join(anomaly_reason)
                    })
                    logging.info(
                        f"Anomalía detectada:\n"
                        f"TX1: {tx1}\nTX2: {tx2}\n"
                        f"Similitud total: {total_similarity:.3f}, Razón: {', '.join(anomaly_reason)}"
                    )
            elif WARNING_THRESHOLD <= total_similarity < COSINE_THRESHOLD:
                # Generar advertencia para similitudes moderadas
                warnings.append({
                    "transaction_1": tx1,
                    "transaction_2": tx2,
                    "similarity_score": total_similarity,
                    "text_similarity": text_sim,
                    "amount_similarity": amount_sim,
                    "remaining_similarity": remaining_sim,
                    "warning_reason": "Similitud moderada pero sin razones claras de anomalía"
                })
                logging.warning(
                    f"Advertencia de similitud moderada ({total_similarity:.3f}) entre:\n"
                    f"TX1: {tx1}\nTX2: {tx2}\n"
                )
            else:
                logging.debug(
                    f"Similitud baja ({total_similarity:.3f}). No se evaluó como posible anomalía."
                )

    logging.info(f"Anomalías detectadas: {len(anomalies)}")
    logging.info(f"Advertencias detectadas: {len(warnings)}")
    return anomalies, warnings



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

        # Parsear particiones del nombre del fichero
        partitions = parse_partitions(file_path)
        logging.info(f"Particiones detectadas: {partitions}")

        # Consultar BigQuery para obtener transacciones
        start_time = time.time()
        
        rows_to_process = raw_data #query_raw_transactions(partitions, 
                                                 #f"gs://{bucket_name}/{file_path}")
        elapsed_time = time.time() - start_time
        logging.info(f"Query raw {elapsed_time:.3f} seconds.")
        logging.info(
            f"Número de transacciones recién ingestadas: {len(rows_to_process)}"
        )

        # Similitud
        start_time = time.time()
        anomalies,warnings = detect_anomalies(rows_to_process, bigquery_data)
        logging.info(f"Transacciones similares detectadas: {len(anomalies)}")

        # Registrar advertencias
        if warnings:
            logging.warning(f"Advertencias encontradas: {len(warnings)}")
            for warning in warnings:
                logging.warning(f"Advertencia: {json.dumps(warning, indent=2)}")

        # Registrar anomalías
        if anomalies:
            logging.info(f"Anomalías encontradas: {len(anomalies)}")
            for anomaly in anomalies:
                logging.info(f"Anomalía detectada: {json.dumps(anomaly, indent=2)}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"Similarity {elapsed_time:.3f} seconds.")


        for anomaly in anomalies:
            logging.info(f"Anomalía detectada: {json.dumps(anomaly, indent=2)}")

        # Identificar transacciones únicas usando lock
        with lock:
            unique_rows = [
                row
                for row in rows_to_process
                if row["checksum"] not in checksums_in_process
            ]
            logging.info(
                f"Transacciones únicas a procesar tras filtro checksum local: {len(unique_rows)}"
            )

            # Agregar checksums al estado compartido
            checksums_in_process.extend([row["checksum"] for row in unique_rows])

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
