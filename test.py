from google.cloud import storage
import re

# Inicializa el cliente de GCS
client = storage.Client()

# Nombre del bucket
bucket_name = "data-raw-ingest-newbalances-cofers-us-east1-prod"

# Conéctate al bucket
bucket = client.bucket(bucket_name)

# Listar los blobs en el bucket
blobs = bucket.list_blobs()

# Expresión regular para extraer año, mes, día, hora e ID
pattern = re.compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/([a-f0-9\-]+)")

# Extraer información
results = []

for blob in blobs:
    match = pattern.search(blob.name)
    if match:
        year, month, day, hour, directory_id = match.groups()
        results.append({
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "id": directory_id
        })

# Mostrar los resultados
for result in results:
    print(result)

