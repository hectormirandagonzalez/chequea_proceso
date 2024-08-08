import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

from src.index import revisa_atraso
from src.index import estado_proceso
from src.index import realiza_chequeos

from src.config.db_config import conexion

logging.basicConfig(filename='main-log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scheduled_task():
    print("Ejecutando tarea periódica: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    realiza_chequeos()
    # Aquí va tu lógica de tarea


scheduler = BackgroundScheduler()

# Calculamos el próximo tiempo de ejecución, 10 segundos después del inicio
next_run_time = datetime.now() + timedelta(seconds=5)


# Agregamos la tarea al scheduler
def start_scheduler():
    if not scheduler.running:
        scheduler.add_job(func=scheduled_task, trigger="interval", seconds=300, next_run_time=next_run_time)
        scheduler.start()


if __name__ == '__main__':
    logging.info("HOST: " + conexion.host)
    start_scheduler()
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # Cerrar el scheduler de forma segura
        scheduler.shutdown()

