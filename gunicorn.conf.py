bind = "0.0.0.0:8080"
workers = 2  # Número de procesos basado en núcleos de CPU
threads = 4  # Número de hebras por worker
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120  # Tiempo de espera máximo para una solicitud
loglevel = "info"
accesslog = "-"  # Registra accesos en la salida estándar
errorlog = "-"  # Registra errores en la salida estánd


def on_starting(server):
    """Hook to verify Redis connection before starting the server."""
    import logging
    from main import check_redis_connection
    logging.info("Verifying Redis connection...")
    check_redis_connection()
    logging.info("Connection with Redis successful")