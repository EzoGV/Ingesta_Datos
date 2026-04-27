import csv
import logging
import os
import re
from datetime import datetime

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

# Rutas
ruta_entrada = "data/procesado/dataset_limpio.csv"
ruta_validos = "data/validados/registros_validos.csv"
ruta_errores = "data/errores/errores_validacion.csv"

def validar_rut_chileno(rut):

    patron = r'^\d{7,8}[\dkK]$'
    return re.match(patron, str(rut)) is not None

def ejecutar_validacion():
    try:
        logging.info("--- ETAPA 3: VALIDACION Y SEPARACION DE REGISTROS ---")
        
        os.makedirs("data/validados", exist_ok=True)
        os.makedirs("data/errores", exist_ok=True)

        validos = []
        errores = []
        fecha_actual = datetime.now()

        if not os.path.exists(ruta_entrada):
            logging.error(f"No se encontró el archivo de entrada en {ruta_entrada}")
            return

        with open(ruta_entrada, 'r', encoding='utf-8') as f:
            lector = csv.DictReader(f)
            campos_originales = lector.fieldnames
            
            for fila in lector:
                fallas = []
                
                # Validación de RUT
                rut = fila.get('rut', '')
                if not validar_rut_chileno(rut):
                    fallas.append(f"RUT_INVALIDO({rut})")

                #Validación de Fecha de Venta (Semántica)
                try:
                    # Asumimos formato YYYY-MM-DD que viene de la limpieza
                    f_venta = datetime.strptime(fila['fecha_venta'], '%Y-%m-%d')
                    if f_venta > fecha_actual:
                        fallas.append("FECHA_FUTURA")
                except Exception:
                    fallas.append("FORMATO_FECHA_ERROR")

                # 3. Validación de Cantidad (Estructural)
                if int(fila['cantidad']) <= 0:
                    fallas.append("CANTIDAD_NO_POSITIVA")

                # CLASIFICACIÓN FINAL
                if not fallas:
                    validos.append(fila)
                else:
                    # Agregamos el motivo del porqué no entró
                    fila['motivo_rechazo'] = " | ".join(fallas)
                    errores.append(fila)

        # GUARDAR LOS BUENOS
        with open(ruta_validos, 'w', newline='', encoding='utf-8') as f:
            if validos:
                escritor = csv.DictWriter(f, fieldnames=validos[0].keys())
                escritor.writeheader()
                escritor.writerows(validos)
            else:
                logging.warning("No hay registros validos para guardar.")

        # GUARDAR LOS MALOS (Para la tabla de auditoría en la Etapa 4)
        with open(ruta_errores, 'w', newline='', encoding='utf-8') as f:
            # Definimos cabeceras para errores (campos originales + el motivo)
            cabecera_error = campos_originales + ['motivo_rechazo']
            escritor = csv.DictWriter(f, fieldnames=cabecera_error)
            escritor.writeheader()
            if errores:
                escritor.writerows(errores)

        logging.info(f"PROCESO COMPLETADO: {len(validos)} válidos y {len(errores)} errores detectados.")
        
    except Exception as e:
        logging.error(f"ERROR CRITICO en Validacion: {e}")

if __name__ == "__main__":
    ejecutar_validacion()