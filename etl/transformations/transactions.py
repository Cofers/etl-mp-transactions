from datetime import datetime
import logging
from src.transformations import prepare_metadata
import hashlib


def process_transactions(records):
    """Procesa las transacciones para prepararlas según el formato requerido."""
    rows = []
    
    metadata_dict = {}  # Diccionario para reconstruir metadatos
    
    for record in records:
        logging.debug(f"Processing record: {record}")
        try:
            # Reconstruir el diccionario de metadatos
            if record.get('metadata_key') and record.get('metadata_value'):
                metadata_dict[record['metadata_key']] = record['metadata_value']

            # Calcular el etl_checksum usando MD5
            checksum_string = f"{record['transaction_date']}{record['concept']}{record['amount']}{record['reported_remaining']}"
            etl_checksum = hashlib.md5(checksum_string.encode('utf-8')).hexdigest()

            # Manejar fechas
            transaction_date = fix_date_format(record['transaction_date']) if record.get('transaction_date') else None
            created_at = parse_date(record['created_at']).strftime('%Y-%m-%dT00:00:00') if record.get('created_at') else None

            rows.append({
                'checksum': record['checksum'],
                'etl_checksum': etl_checksum,
                'concept': record.get('concept', ''),
                'amount': record.get('amount', 0),
                'account_number': record.get('account_number', ''),
                'bank': record.get('bank', ''),
                'account_alias': record.get('account_alias', ''),
                'currency': record.get('currency', ''),
                'report_type': record.get('report_type', ''),
                'extraction_date': record.get('extraction_date'),
                'user_id': record.get('userId', ''),  # Ajustado a la consulta
                'company_id': record.get('companyId', ''),  # Ajustado a la consulta
                'transaction_date': transaction_date,
                'reported_remaining': record.get('reported_remaining', 0),
                'created_at': created_at,
                'metadata': metadata_dict  # Reconstrucción del diccionario
            })

        except Exception as e:
            logging.error(f"Unexpected error processing record: {e}")

    return rows




    

def fix_date_format(date_str):
    try:
        if '-' in date_str and date_str.count('-') == 2:
            try:
                datetime_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return datetime_obj.strftime('%Y-%m-%d')
            except ValueError:
                datetime_obj = datetime.strptime(date_str, '%d-%m-%Y')
                return datetime_obj.strftime('%Y-%m-%d')
        elif '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                if len(parts[0]) == 4:  
                    format = '%Y/%m/%d'
                elif len(parts[2]) == 4:  
                    format = '%d/%m/%Y'
                else:
                    raise ValueError("Date no found")
                datetime_obj = datetime.strptime(date_str, format)
                return datetime_obj.strftime('%Y-%m-%d')
            else:
                raise ValueError("Date no corrected")
    except ValueError as e:
        logging.info(f"Error processing date '{date_str}': {e}")
    return date_str

def parse_date(date_str):
    for date_format in ['%Y-%m-%d', '%d/%m/%Y']:  
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    raise ValueError(f"Date no valid: {date_str}")