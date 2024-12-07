from src.pubsub import publish_response
import os
import logging

def push_fake(data):
    logging.info("Pushing data to pubsub")
    return data

def push(data):
    print("Pushing data to pubsub")
    for transaction in data:  # Asume que 'data' es una lista de transacciones
        pubsub_data = prepare_for_pubsub(transaction)
        if pubsub_data:  # Asegurarse de que la preparación fue exitosa
            logging.debug(f"Data to publish: {pubsub_data}")
            publish_response(pubsub_data, topic=os.environ.get("TOPIC_IN"))
        else:
            logging.error("Failed to prepare transaction data for publishing")

def prepare_for_pubsub(transaction):
    if not isinstance(transaction, dict):
        logging.error("Transaction data must be a dictionary.")
        return None

    transaction_data = transaction.copy()  # Make a copy to avoid modifying the original dictionary
    
    # Ensure that metadata is a dictionary, handle empty or improperly formatted metadata
    metadata = transaction_data.get('metadata', [])
    if isinstance(metadata, list) and metadata:
        try:
            metadata_dict = {item['key']: item['value'] for item in metadata}
            transaction_data['metadata'] = metadata_dict
        except (TypeError, KeyError) as e:
            logging.error(f"Error converting metadata: {e}")
            transaction_data['metadata'] = {}  # Set to empty dict if conversion fails
    else:
        transaction_data['metadata'] = {}  # Also set to empty dict if metadata is empty or not a list

    # Eliminate fields that are not needed in the Pub/Sub message
    transaction_data.pop('created_at', None)
    transaction_data.pop('etl_checksum', None)
    
    return transaction_data