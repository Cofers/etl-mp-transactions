import logging
import sys


LOCK_EXPIRY_SECONDS = 5

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def acquire_lock(redis_client, key):
    """Intenta adquirir un lock en Redis."""
    lock_key = f"lock:{key}"
    is_locked = redis_client.set(lock_key, "1", nx=True, ex=LOCK_EXPIRY_SECONDS)
    return is_locked

def release_lock(redis_client, key):
    """Libera el lock en Redis."""
    lock_key = f"lock:{key}"
    redis_client.delete(lock_key)

def store_checksum_atomic(redis_client, checksum):
    """Guarda un checksum en Redis con exclusión mutua."""
    lock_key = f"checksum:{checksum}"
    if acquire_lock(redis_client, lock_key):
        try:
            redis_client.sadd("processed_checksums", checksum)
            logging.info(f"Checksum almacenado: {checksum}")
        finally:
            release_lock(redis_client, lock_key)
    else:
        logging.warning(f"No se pudo adquirir lock para checksum: {checksum}")

def is_checksum_processed_atomic(redis_client, checksum):
    """Verifica si un checksum ya fue procesado."""
    return redis_client.sismember("processed_checksums", checksum)

def filter_unique_transactions(redis_client, rows_to_process):
    """Filtra las transacciones únicas utilizando Redis."""
    unique_rows = []
    for row in rows_to_process:
        checksum = row['checksum']
        if not is_checksum_processed_atomic(redis_client, checksum):
            unique_rows.append(row)
            store_checksum_atomic(redis_client, checksum)
        else:
            logging.info(f"Checksum ya procesado: {checksum}")

    logging.info(f"Transacciones únicas a procesar: {len(unique_rows)}")
    return unique_rows