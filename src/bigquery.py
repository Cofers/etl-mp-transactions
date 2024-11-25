from google.cloud import pubsub_v1, bigquery


BQ_TABLE = "production-400914.temp_data.bronze_transactions"

def query_raw_transactions(partitions,file_name):
    bq_client = bigquery.Client()
    """Consulta transacciones en BigQuery basadas en las particiones del archivo."""
    query = f"""
    SELECT
        lines.checksum AS checksum,
        lines.date AS transaction_date,
        lines.concept AS concept,
        lines.amount AS amount,
        payload.header.account_number AS account_number,
        payload.header.currency AS currency,
        payload.header.bank AS bank
    FROM `{BQ_TABLE}`,
    UNNEST(payload) AS payload,
    UNNEST(payload.lines) AS lines
    WHERE year = {partitions['year']} 
      AND month = {partitions['month']} 
      AND day = {partitions['day']} 
      AND company_id = '{partitions['company_id']}'
      and _FILE_NAME = '{file_name}'
    """
    print(query)
    query_job = bq_client.query(query)
    return [dict(row) for row in query_job]