import logging
from datetime import datetime





def adapt_metadata(metadata_dict):
    """
    Convert a metadata dictionary into a list of records as expected by BigQuery.
    """
    return [{'key': key, 'value': value} for key, value in metadata_dict.items()]

def prepare_metadata(metadata_dict):
    """
    Converts a dictionary of metadata into a list of key-value pairs.
    This aligns with the BigQuery STRUCT type expected in the schema.
    """
    return [{"key": k, "value": str(v)} for k, v in metadata_dict.items()]