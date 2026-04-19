import shutil
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

origen = "origen/ventas.csv"
destino_dir = "data/raw/"

try:
    logging.info("Inicio del proceso de ingesta automatizada.")
    
    if os.path.exists(origen):
        os.makedirs(destino_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo_destino = f"ventas_{timestamp}.csv"
        destino = os.path.join(destino_dir, nombre_archivo_destino)

        shutil.copy(origen, destino)
        logging.info(f"Archivo historizado correctamente como: {nombre_archivo_destino}")

        with open(destino, 'r') as f:
            registros = sum(1 for line in f) - 1
            
        logging.info(f"Cantidad de registros procesados: {registros}")
        
    else:
        logging.error(f"Error: El archivo fuente no existe en {origen}")

except Exception as e:
    logging.error(f"Error en la ingesta: {e}")