from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np



COSINE_THRESHOLD = 0.9  # Similitud mínima para texto
AMOUNT_THRESHOLD = 1  # Diferencia máxima permitida en el monto
DATE_WEIGHT = 0.3  # Peso de la fecha
TEXT_WEIGHT = 0.5  # Peso del texto
AMOUNT_WEIGHT = 0.2  # Peso del monto

def calculate_similarity(tx1, tx2):
    """Calcula la similitud entre dos transacciones."""
    # Similitud en texto (concept)
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform([tx1['concept'], tx2['concept']])
    text_similarity = cosine_similarity(tfidf_matrix)[0, 1]

    # Diferencia en monto
    amount_diff = abs(tx1['amount'] - tx2['amount'])
    amount_similarity = max(0, 1 - amount_diff / AMOUNT_THRESHOLD)

    # Comparación de fecha (exacta o rango)
    date_similarity = 1 if tx1['transaction_date'] == tx2['transaction_date'] else 0

    # Puntaje ponderado
    total_similarity = (
        TEXT_WEIGHT * text_similarity +
        AMOUNT_WEIGHT * amount_similarity +
        DATE_WEIGHT * date_similarity
    )
    return total_similarity

def detect_similar_transactions(transactions, similarity_threshold=0.8):
    """Detecta transacciones similares en los datos."""
    similar_pairs = []
    n = len(transactions)

    for i in range(n):
        for j in range(i + 1, n):
            similarity = calculate_similarity(transactions[i], transactions[j])
            if similarity >= similarity_threshold:
                similar_pairs.append({
                    'transaction_1': transactions[i],
                    'transaction_2': transactions[j],
                    'similarity_score': similarity
                })

    return similar_pairs

