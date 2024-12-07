
from google.cloud import storage
from io import BytesIO
import fastavro

def load_avro(data):
    bucket_name = data["bucket"]
    file_name = data["name"]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    blob_bytes = blob.download_as_bytes()
    bytes_io = BytesIO(blob_bytes)
    avro_reader = fastavro.reader(bytes_io)
    
    return avro_reader