from google.cloud import bigquery
import logging
import os

# Inicializar cliente de BigQuery y variables de configuración una sola vez
client = bigquery.Client()
project_id = os.getenv("GCP_PROJECT")
dataset_name = os.getenv("DATASET_NAME")
table_name = os.getenv("TABLE_NAME")


def unique_ids_fake(rows):
    pass
    return(rows)

def unique_ids(rows):
    """
    Extracts unique IDs from a list of rows.
    """
    checksum_list_bq = get_checksums_from_bigquery(rows[0]['company_id'], 'checksum')
    final_data = filter_rows_by_checksums(rows, checksum_list_bq, 'checksum')
    logging.info(f"Transactions after checksums: {len(final_data)}")
    
    if final_data:
        #check size how many rows are delete from etl_checksum. 
        etl_checksum_list_bq = get_checksums_from_bigquery(rows[0]['company_id'], 'etl_checksum')
        final_data = filter_rows_by_checksums(final_data, etl_checksum_list_bq, 'etl_checksum')
        logging.info(f"Transactions after etl_checksums: {len(final_data)}")

    return final_data




def get_checksums_from_bigquery(company_id, checksum_type):
    """
    Generic function to fetch checksums from BigQuery.
    :param company_id: ID of the company to filter the checksums.
    :param checksum_type: Type of checksum to fetch ('checksum' or 'etl_checksum').
    :return: List of checksums fetched from BigQuery.
    """
    logging.info(f"Getting {checksum_type}s from BigQuery for company {company_id}")
    if not all([project_id, dataset_name, table_name]):
        logging.error("BigQuery configuration environment variables are not set correctly.")
        return []

    query = f"""
    SELECT {checksum_type}
    FROM `{project_id}.{dataset_name}.{table_name}`
    WHERE company_id = @company_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_id", "STRING", company_id)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        result_checksums = [row[checksum_type] for row in query_job.result()]
        logging.debug(f"Total {checksum_type}s from BigQuery: {len(result_checksums)}")
        return result_checksums
    except Exception as e:
        logging.error(f"Failed to query BigQuery: {e}")
        return []



def filter_rows_by_checksums(rows, ids_to_exclude, checksum_field):
    """
    Filters rows by excluding those that contain any of the specified checksums.
    """
    filtered_rows = [row for row in rows if row[checksum_field] not in ids_to_exclude]
    return filtered_rows