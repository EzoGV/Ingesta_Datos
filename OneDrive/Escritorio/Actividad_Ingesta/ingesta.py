import sqlite3
import csv
import logging
import os
from datetime import datetime

# Configuración de trazabilidad
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

origen = "origen/ventas.csv"
carpeta_db = "data/"
ruta_db = "data/ventas.db"

try:
    logging.info("Iniciando proceso ETL (Extracción, Limpieza y Carga)...")
    
    if os.path.exists(origen):
        os.makedirs(carpeta_db, exist_ok=True)
        
        # --- PASO 1: EXTRACCIÓN Y LIMPIEZA EN MEMORIA ---
        ventas_limpias = {} # Usamos un diccionario para evitar id_venta duplicados automáticamente
        timestamp_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(origen, 'r', encoding='utf-8') as archivo_csv:
            lector = csv.reader(archivo_csv)
            next(lector)  # Saltamos los encabezados
            
            for fila in lector:
                id_venta = fila[0].strip()
                fecha = fila[1].strip()
                # LIMPIEZA: Quitamos espacios extra y pasamos todo a MAYÚSCULAS
                producto = fila[2].strip().upper() 
                # LIMPIEZA: Convertimos texto a número entero para poder hacer cálculos después
                cantidad = int(fila[3].strip())
                precio_total = int(fila[4].strip())
                
                # Al guardarlo así, si el CSV trae dos veces el ID '001', 
                # Python automáticamente se queda solo con el último (elimina duplicados)
                ventas_limpias[id_venta] = (id_venta, fecha, producto, cantidad, precio_total, timestamp_actual)

        logging.info(f"Limpieza exitosa. Registros únicos listos para inyectar: {len(ventas_limpias)}")

        # --- PASO 2: CARGA A LA BASE DE DATOS SQLITE ---
        conexion = sqlite3.connect(ruta_db)
        cursor = conexion.cursor()
        
        # Creamos la tabla definitiva
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas_procesadas (
                id_venta TEXT UNIQUE,
                fecha TEXT,
                producto TEXT,
                cantidad INTEGER,
                precio_total INTEGER,
                fecha_ingesta TEXT
            )
        ''')
        
        # Insertamos los datos ya limpios. 
        # Usamos REPLACE para que si el ID ya existe en la BD, lo actualice en lugar de dar error.
        registros_insertados = 0
        for datos_venta in ventas_limpias.values():
            cursor.execute('''
                INSERT OR REPLACE INTO ventas_procesadas (id_venta, fecha, producto, cantidad, precio_total, fecha_ingesta)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', datos_venta)
            registros_insertados += 1
            
        conexion.commit()
        conexion.close()
        
        logging.info(f"Carga exitosa. Registros guardados en SQLite: {registros_insertados}")
        
    else:
        logging.error(f"Error: No se encontró el archivo en {origen}")

except Exception as e:
    logging.error(f"Error crítico en el proceso ETL: {e}")