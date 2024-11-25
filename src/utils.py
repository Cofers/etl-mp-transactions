def parse_partitions(file_path):
    """Parsea las particiones del nombre del fichero."""
    parts = file_path.split('/')
    result = {}
    for part in parts:
        if '=' in part:
            key, value = part.split('=')
            result[key] = value
    return result