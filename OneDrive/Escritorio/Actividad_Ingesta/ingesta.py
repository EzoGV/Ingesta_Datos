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

# Rutas de archivos
origen = "origen/ventas.csv"
destino_raw = "data/raw/ventas_raw.csv"

def generar_respaldo_raw():
    
    try:
        logging.info("---INGESTA CAPA RAW ---")
        
        #Aseguramos que la carpeta data/raw exista
        os.makedirs("data/raw", exist_ok=True)
        
        if not os.path.exists(origen):
            logging.error(f"ERROR: No se encontro el archivo original en {origen}. Asegurate de haber generado los 1000 registros.")
            return

        #Procesamos el archivo de 1000 registros
        with open(origen, 'r', encoding='utf-8') as f_in, \
             open(destino_raw, 'w', newline='', encoding='utf-8') as f_out:
            
            lector = csv.reader(f_in)
            escritor = csv.writer(f_out)
            
            #Leemos la cabecera (id_venta, fecha_venta, producto, cantidad, precio_total, rut)
            try:
                cabecera = next(lector)
            except StopIteration:
                logging.error("El archivo de origen esta vacio.")
                return

            #Escribimos la cabecera + la columna de control interna
            escritor.writerow(cabecera + ["fecha_ingesta"])
            
            #Marcamos el momento exacto en que los datos entran al pipeline
            timestamp_ingesta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conteo_registros = 0
            
            for fila in lector:
                # erificamos que la fila no esté vacía
                if any(fila):
                    escritor.writerow(fila + [timestamp_ingesta])
                    conteo_registros += 1
                
        logging.info(f"EXITO: Se procesaron {conteo_registros} registros correctamente.")
        logging.info(f"Archivo Raw generado")

    except Exception as e:
        logging.error(f"ERROR CRITICO en la fase de ingesta: {e}")

if __name__ == "__main__":
    generar_respaldo_raw()