from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import logging





SIMILARITY_THRESHOLD = 0.9
FIELDS = {
    "concept": 0.8,        # Texto
    "amount": 0.1,         # Numérico
    "account_number": 0.0, # Exacto
    "bank": 0.0,           # Exacto
    "transaction_date": 0.1 # Exacto
}

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