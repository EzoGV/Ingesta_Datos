import sqlite3
import csv
import logging
import os
from datetime import datetime

#CONFIGURACIÓN DE LOGS: Ahora se guardan en un archivo 'pipeline.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Guarda en archivo
        logging.StreamHandler()              # También muestra en consola
    ]
)

origen = "origen/ventas.csv"
ruta_db = "data/ventas.db"

try:
    logging.info("--- INICIANDO FASE DE INGESTA ---")
    
    if os.path.exists(origen):
        conexion = sqlite3.connect(ruta_db)
        cursor = conexion.cursor()
        
        #Tabla Raw (Cruda)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas_raw (
                id_venta TEXT, fecha TEXT, producto TEXT, cantidad TEXT, precio_total TEXT, fecha_ingesta TEXT
            )
        ''')
        
        registros_ingestados = 0
        timestamp_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(origen, 'r', encoding='utf-8') as f:
            lector = csv.reader(f)
            next(lector)
            for fila in lector:
                fila.append(timestamp_actual)
                cursor.execute('INSERT INTO ventas_raw VALUES (?,?,?,?,?,?)', fila)
                registros_ingestados += 1
        
        conexion.commit()
        conexion.close()
        logging.info(f"EXITO: Se ingestaron {registros_ingestados} productos a la tabla ventas_raw.")
    else:
        logging.error(f"ERROR: No se encontró el archivo en {origen}")

except Exception as e:
    logging.error(f"ERROR CRÍTICO en Ingesta: {e}")