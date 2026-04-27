import csv
import logging
import os
from config_db import obtener_conexion

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

ruta_validos = "data/validados/registros_validos.csv"
ruta_errores = "data/errores/errores_validacion.csv"

def ejecutar_carga_final():
    conexion = None
    try:
        logging.info("--- ETAPA 4: CARGA DE DATOS A POSTGRESQL ---")
        
        # 1. Conexión a la BD
        conexion = obtener_conexion()
        if conexion is None:
            logging.error("No se pudo establecer conexion con PostgreSQL.")
            return
        cursor = conexion.cursor()

        # 2. TABLA DE VENTAS (Capa Gold - Datos Perfectos)
        cursor.execute("DROP TABLE IF EXISTS ventas_procesadas")
        cursor.execute('''
            CREATE TABLE ventas_procesadas (
                id_venta TEXT PRIMARY KEY,
                fecha_venta DATE,
                producto TEXT,
                cantidad INTEGER,
                precio_total INTEGER,
                precio_unitario INTEGER,
                rut_cliente TEXT,
                fecha_ingesta TIMESTAMP
            )
        ''')

        # 3. TABLA DE AUDITORÍA (Para los 50 errores)
        cursor.execute("DROP TABLE IF EXISTS log_errores_validacion")
        cursor.execute('''
            CREATE TABLE log_errores_validacion (
                id_error SERIAL PRIMARY KEY,
                id_venta_origen TEXT,
                motivo_rechazo TEXT,
                fecha_auditoria TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 4. CARGA DE REGISTROS VÁLIDOS
        cargados = 0
        if os.path.exists(ruta_validos):
            with open(ruta_validos, 'r', encoding='utf-8') as f:
                lector = csv.DictReader(f)
                for d in lector:
                    cursor.execute('''
                        INSERT INTO ventas_procesadas VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_venta) DO NOTHING
                    ''', (d['id_venta'], d['fecha_venta'], d['producto'], 
                          d['cantidad'], d['precio_total'], d['precio_unitario'], 
                          d['rut'], d['fecha_ingesta']))
                    cargados += 1

        # 5. CARGA DE LOG DE ERRORES
        rechazados = 0
        if os.path.exists(ruta_errores):
            with open(ruta_errores, 'r', encoding='utf-8') as f:
                lector = csv.DictReader(f)
                for d in lector:
                    cursor.execute('''
                        INSERT INTO log_errores_validacion (id_venta_origen, motivo_rechazo)
                        VALUES (%s, %s)
                    ''', (d['id_venta'], d['motivo_rechazo']))
                    rechazados += 1

        conexion.commit()
        cursor.close()
        logging.info(f"EXITO TOTAL: {cargados} ventas cargadas y {rechazados} errores auditados en la BD.")

    except Exception as e:
        logging.error(f"ERROR CRITICO en la carga: {e}")
        if conexion: conexion.rollback()
    finally:
        if conexion: conexion.close()

if __name__ == "__main__":
    ejecutar_carga_final()