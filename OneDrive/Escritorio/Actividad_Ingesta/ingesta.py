import shutil
import logging
import os

logging.basicConfig(level=logging.INFO)

origen = "origen/ventas.csv"
destino = "data/raw/ventas.csv"

try:
    if os.path.exists(origen):
        os.makedirs("data/raw", exist_ok=True)
        shutil.copy(origen, destino)
        logging.info("Archivo copiado correctamente")
        
        with open(destino, 'r') as f:
            registros = sum(1 for line in f) - 1
        logging.info(f"Cantidad de registros procesados: {registros}")
        
    else:
        logging.error("Error: El archivo de ventas.csv no se encuentra en la carpeta origen.")

except Exception as e:
    logging.error(f"Error en la ingesta: {e}")