import sqlite3
import logging
import os
import csv

#Configuración de logs en archivo y consola
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

ruta_db = "data/ventas.db"
carpeta_procesados = "data/procesado/"

try:
    logging.info(" INICIANDO PROCESAMIENTO Y LIMPIEZA DE DATOS ")
    
    #Asegurar carpeta para exportación final
    os.makedirs(carpeta_procesados, exist_ok=True)
    
    conexion = sqlite3.connect(ruta_db)
    cursor = conexion.cursor()

    #Limpieza de estructura: Resetear la tabla procesada si ya existía
    cursor.execute('DROP TABLE IF EXISTS ventas_procesadas')

    #Crear la tabla con la nueva columna 'precio_unitario'
    cursor.execute('''
        CREATE TABLE ventas_procesadas (
            id_venta TEXT UNIQUE, 
            fecha TEXT, 
            producto TEXT, 
            cantidad INTEGER, 
            precio_total INTEGER, 
            precio_unitario INTEGER, 
            fecha_ingesta TEXT
        )
    ''')

    # Procesamiento y Transformación Directa (SQL)
    # Estandariza: UPPER y TRIM
    # Filtra: Nulos y cantidades menores o iguales a 0
    # Transforma: Genera precio_unitario (precio_total / cantidad)
    # Deduplica: GROUP BY id_venta
    cursor.execute('''
        INSERT INTO ventas_procesadas
        SELECT 
            id_venta, 
            fecha, 
            UPPER(TRIM(producto)), 
            CAST(cantidad AS INTEGER), 
            CAST(precio_total AS INTEGER), 
            (CAST(precio_total AS INTEGER) / CAST(cantidad AS INTEGER)),
            MAX(fecha_ingesta)
        FROM ventas_raw
        WHERE id_venta IS NOT NULL AND id_venta != '' 
          AND CAST(cantidad AS INTEGER) > 0
        GROUP BY id_venta
    ''')

    conexion.commit()
    
    # Guardar copia del dataset limpio en la carpeta /processed/
    ruta_csv_procesado = os.path.join(carpeta_procesados, "dataset_limpio.csv")
    cursor.execute('SELECT * FROM ventas_procesadas')
    filas = cursor.fetchall()
    
    with open(ruta_csv_procesado, 'w', newline='', encoding='utf-8') as f:
        escritor = csv.writer(f)
        escritor.writerow([d[0] for d in cursor.description])
        escritor.writerows(filas)

    conexion.close()
    
    logging.info("ÉXITO: Datos procesados y tabla ventas_procesadas creada con éxito.")
    logging.info(f"ÉXITO: Archivo CSV de respaldo generado en {ruta_csv_procesado}")

except Exception as e:
    logging.error(f"ERROR en el procesamiento de tablas: {e}")