from google.cloud import pubsub_v1, bigquery
import logging
import sys


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


BQ_TABLE = "production-400914.temp_data.bronze_transactions"

def query_raw_transactions(partitions):
    bq_client = bigquery.Client()
    """Consulta transacciones en BigQuery basadas en las particiones del archivo."""
    query = f"""
      SELECT
    lines.checksum AS checksum,
    lines.date AS transaction_date,
    lines.concept AS concept,
    lines.amount AS amount,
    lines.remaining AS reported_remaining,
    payload.header.account_number AS account_number,
    payload.header.account_alias AS account_alias,
    payload.header.currency AS currency,
    payload.header.timeframe AS report_type,
    payload.header.report_date AS created_at,
    payload.header.bank AS bank,
    payload.header.extraction_timestamp AS extraction_date,
    userId AS user_id,
    companyId AS company_id,
    metadata.key AS metadata_key,
    metadata.value AS metadata_value
FROM `production-400914.temp_data.bronze_transactions`,
UNNEST(payload) AS payload,
UNNEST(payload.lines) AS lines,
UNNEST(lines.metadata) AS metadata
    WHERE year = {partitions['year']} 
      AND month = {partitions['month']} 
      AND day = {partitions['day']} 
      AND company_id = '{partitions['company_id']}'
      and _FILE_NAME = 'gs://ingesta-pruebas-cofers-domingo/{partitions['file_name']}'
    """
    logging.info(f"Querying BigQuery: {query}")
    query_job = bq_client.query(query)
    return [dict(row) for row in query_job]