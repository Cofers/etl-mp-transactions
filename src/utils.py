import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def parse_partitions(file_path):
    """Parsea las particiones del nombre del fichero."""
    parts = file_path.split('/')
    result = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=')
            result[key] = value
    return result


def process_transactions(transactions):
    """Procesa las transacciones únicas (transformación, inserción, etc.)."""
    logging.info(f"Procesando {len(transactions)} transacciones...")
    for transaction in transactions:
        logging.info(f"Procesando transacción con checksum: {transaction['checksum']}")
    logging.info("Procesamiento completado.")