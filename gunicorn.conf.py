bind = "0.0.0.0:8080"
workers = 1  # Número de procesos basado en núcleos de CPU
threads = 4  # Número de hebras por worker
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120  # Tiempo de espera máximo para una solicitud
loglevel = "info"
accesslog = "-"  # Registra accesos en la salida estándar
errorlog = "-"  # Registra errores en la salida estánd