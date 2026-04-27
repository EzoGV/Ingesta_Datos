import csv
import logging
import os
from datetime import datetime
from config_db import obtener_conexion

#Configuración de logs
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

ruta_raw = "data/raw/ventas_raw.csv"
carpeta_procesados = "data/procesado"
ruta_processed = f"{carpeta_procesados}/dataset_limpio.csv"

def limpiar_y_cargar():
    try:
        logging.info("--- ETAPA 2: PROCESAMIENTO PYTHON -> CSV -> POSTGRES ---")
        
        #Aseguramos que la carpeta exista
        os.makedirs(carpeta_procesados, exist_ok=True)
        
        datos_limpios = []
        ids_vistos = set() # Para eliminar duplicados

        #PROCESAMIENTO EN PYTHON (Limpiamos los datos en memoria)
        with open(ruta_raw, 'r', encoding='utf-8') as f:
            lector = csv.DictReader(f)
            for fila in lector:
                id_v = fila['id_venta'].strip()
                
                #Sin duplicados, sin vacíos, cantidad mayor a 0
                if id_v and id_v not in ids_vistos and int(fila['cantidad']) > 0:
                    producto = fila['producto'].strip().upper()
                    p_total = int(fila['precio_total'])
                    cant = int(fila['cantidad'])
                    p_unitario = p_total // cant # Creamos la nueva columna
                    
                    datos_limpios.append({
                        'id_venta': id_v,
                        'fecha': fila['fecha'],
                        'producto': producto,
                        'cantidad': cant,
                        'precio_total': p_total,
                        'precio_unitario': p_unitario,
                        'fecha_ingesta': fila['fecha_ingesta']
                    })
                    ids_vistos.add(id_v)

        #Generar csv de datos procesados
        with open(ruta_processed, 'w', newline='', encoding='utf-8') as f:
            campos = ['id_venta', 'fecha', 'producto', 'cantidad', 'precio_total', 'precio_unitario', 'fecha_ingesta']
            escritor = csv.DictWriter(f, fieldnames=campos)
            escritor.writeheader()
            escritor.writerows(datos_limpios)
        
        logging.info(f"ÉXITO: Archivo CSV limpio generado en {ruta_processed}")

        #CONECTAR Y CARGAR A LA BD POSTGRESQL
        conexion = obtener_conexion()
        cursor = conexion.cursor()
        
        #Reseteamos la tabla para que no se dupliquen datos si corres el script varias veces
        cursor.execute("DROP TABLE IF EXISTS ventas_procesadas")
        cursor.execute('''
            CREATE TABLE ventas_procesadas (
                id_venta TEXT PRIMARY KEY,
                fecha DATE,
                producto TEXT,
                cantidad INTEGER,
                precio_total INTEGER,
                precio_unitario INTEGER,
                fecha_ingesta TIMESTAMP
            )
        ''')

        # Insertamos los datos ya limpios en el motor
        for d in datos_limpios:
            cursor.execute('''
                INSERT INTO ventas_procesadas VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (d['id_venta'], d['fecha'], d['producto'], d['cantidad'], d['precio_total'], d['precio_unitario'], d['fecha_ingesta']))
        
        conexion.commit()
        cursor.close()
        conexion.close()
        
        logging.info("ÉXITO: Base de Datos PostgreSQL actualizada con datos procesados.")

    except Exception as e:
        logging.error(f"Error en limpieza: {e}")

if __name__ == "__main__":
    limpiar_y_cargar()