import csv
import logging
import os
from datetime import datetime

# CONFIGURACIÓN DE LOGS
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

origen = "origen/ventas.csv"
destino_raw = "data/raw/ventas_raw.csv"

def generar_respaldo_raw():
    try:
        logging.info("---INGESTA:CAPA RAW---")
        
        #Aseguramos que la carpeta data/raw exista
        os.makedirs("data/raw", exist_ok=True)
        
        if not os.path.exists(origen):
            logging.error(f"No se encontró el archivo original en {origen}")
            return

        #Leemos del origen y escribimos en la carpeta raw
        #Agregamos la columna fecha_ingesta para tener trazabilidad del archivo
        with open(origen, 'r', encoding='utf-8') as f_in, \
             open(destino_raw, 'w', newline='', encoding='utf-8') as f_out:
            
            lector = csv.reader(f_in)
            escritor = csv.writer(f_out)
            
            cabecera = next(lector)
            escritor.writerow(cabecera + ["fecha_ingesta"])
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            registros = 0
            
            for fila in lector:
                escritor.writerow(fila + [timestamp])
                registros += 1
                
        logging.info(f"ÉXITO: Se creó el archivo {destino_raw} con {registros} registros.")
  

    except Exception as e:
        logging.error(f"ERROR en la fase de ingesta: {e}")

if __name__ == "__main__":
    generar_respaldo_raw()