import csv
import logging
import os
from datetime import datetime

# Configuración de logs
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

ruta_raw = "data/raw/ventas_raw.csv"
carpeta_procesados = "data/procesado"
ruta_processed = f"{carpeta_procesados}/dataset_limpio.csv"

def limpiar_rut(rut_sucio):
    """Limpia el RUT quitando puntos, guiones y espacios."""
    if not rut_sucio:
        return ""
    return str(rut_sucio).replace(".", "").replace("-", "").strip().upper()

def limpiar_datos():
    try:
        logging.info("--- ETAPA 2: LIMPIEZA Y NORMALIZACIÓN (PYTHON) ---")
        
        # Aseguramos que la carpeta exista
        os.makedirs(carpeta_procesados, exist_ok=True)
        
        datos_limpios = []
        ids_vistos = set() 

        if not os.path.exists(ruta_raw):
            logging.error(f"No se encontró el archivo raw en {ruta_raw}")
            return

        with open(ruta_raw, 'r', encoding='utf-8') as f:
            lector = csv.DictReader(f)
            for fila in lector:
                id_v = fila['id_venta'].strip()
                
                #REGLAS DE LIMPIEZA:
                #Evitar duplicados por ID
                #Asegurar que cantidad sea mayor a 0
                #Normalizar producto (Mayúsculas)
                #Normalizar RUT (Sin puntos ni guiones)
                if id_v and id_v not in ids_vistos and int(fila['cantidad']) > 0:
                    
                    producto = fila['producto'].strip().upper()
                    rut_normalizado = limpiar_rut(fila.get('rut', ''))
                    
                    p_total = int(fila['precio_total'])
                    cant = int(fila['cantidad'])
                    # Calculamos precio unitario redondeado
                    p_unitario = p_total // cant 
                    
                    datos_limpios.append({
                        'id_venta': id_v,
                        'fecha_venta': fila['fecha_venta'], # Columna nueva
                        'producto': producto,
                        'cantidad': cant,
                        'precio_total': p_total,
                        'precio_unitario': p_unitario,
                        'rut': rut_normalizado, # RUT limpio
                        'fecha_ingesta': fila['fecha_ingesta']
                    })
                    ids_vistos.add(id_v)

        # GENERAR CSV DE DATOS PROCESADOS (CAPA SILVER)
        if datos_limpios:
            with open(ruta_processed, 'w', newline='', encoding='utf-8') as f:
                campos = ['id_venta', 'fecha_venta', 'producto', 'cantidad', 'precio_total', 'precio_unitario', 'rut', 'fecha_ingesta']
                escritor = csv.DictWriter(f, fieldnames=campos)
                escritor.writeheader()
                escritor.writerows(datos_limpios)
            
            logging.info(f"ÉXITO: Se procesaron {len(datos_limpios)} registros.")
            logging.info(f"Archivo Dataset creado")
        else:
            logging.warning("No se generaron datos limpios para exportar.")

    except Exception as e:
        logging.error(f"Error en fase de limpieza: {e}")

if __name__ == "__main__":
    limpiar_datos()