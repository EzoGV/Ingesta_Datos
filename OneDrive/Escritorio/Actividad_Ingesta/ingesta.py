import shutil
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

origen = "origen/ventas.csv"
destino = "data/raw/ventas.csv"

try:
    shutil.copy(origen, destino)
    logging.info("Archivo copiado correctamente")
except Exception as e:
    logging.error(f"Error en la ingesta: {e}")