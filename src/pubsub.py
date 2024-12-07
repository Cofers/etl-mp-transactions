from google.cloud import pubsub_v1
import logging
import json


logging.basicConfig(level=logging.INFO)

publisher_options = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024,  # Tamaño máximo por lote (1 MB)
    max_latency=0.1,  # Latencia máxima antes de enviar el lote
    max_messages=500  # Número máximo de mensajes por lote
)
publisher = pubsub_v1.PublisherClient(batch_settings=publisher_options)

def publish_response(message, topic):
    """Publishes processed message to Pub/Sub topic."""
    message_bytes = json.dumps(message).encode("utf-8")
    topic_path = publisher.topic_path("production-400914", topic)
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        logging.info(f"Published message with ID: {publish_future.result()}")
    except Exception as e:
        logging.error(f"Failed to publish message: {e}")