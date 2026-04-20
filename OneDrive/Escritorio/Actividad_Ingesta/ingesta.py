import shutil
import logging
import os
import csv
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ── RUTAS ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # carpeta de ingesta.py
ORIGEN = os.path.join(BASE_DIR, "origen", "ventas.csv")
DESTINO_DIR = os.path.join(BASE_DIR, "data", "raw")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "data", "checkpoint.json")

def cargar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
            return data.get("ultimo_id", 0)
    return 0

def guardar_checkpoint(ultimo_id):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({"ultimo_id": ultimo_id, "actualizado": datetime.now().isoformat()}, f, indent=2)

def leer_registros_nuevos(origen, ultimo_id_procesado):
    nuevos = []
    with open(origen, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if int(row["id_venta"]) > ultimo_id_procesado:
                nuevos.append(row)
    return nuevos

def guardar_registros(registros, destino_dir):
    os.makedirs(destino_dir, exist_ok=True)
    destino = os.path.join(destino_dir, "ventas_incremental.csv")
    archivo_nuevo = not os.path.exists(destino)
    with open(destino, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=registros[0].keys())
        if archivo_nuevo:
            writer.writeheader()
        writer.writerows(registros)
    return destino

# ── PROCESO PRINCIPAL ──────────────────────────────────────────────────────────
try:
    logging.info("=" * 50)
    logging.info("Inicio del proceso de ingesta INCREMENTAL.")

    if not os.path.exists(ORIGEN):
        logging.error(f"Archivo fuente no encontrado: {ORIGEN}")
        exit(1)

    ultimo_id = cargar_checkpoint()
    logging.info(f"Último ID procesado (checkpoint): {ultimo_id}")

    nuevos = leer_registros_nuevos(ORIGEN, ultimo_id)

    if not nuevos:
        logging.info("Sin registros nuevos. No hay nada que ingestar.")
    else:
        logging.info(f"Registros nuevos encontrados: {len(nuevos)}")
        archivo_destino = guardar_registros(nuevos, DESTINO_DIR)
        logging.info(f"Registros escritos en: {archivo_destino}")
        nuevo_ultimo_id = max(int(r["id_venta"]) for r in nuevos)
        guardar_checkpoint(nuevo_ultimo_id)
        logging.info(f"Checkpoint actualizado → último ID: {nuevo_ultimo_id}")

    logging.info("Proceso finalizado correctamente.")
    logging.info("=" * 50)

except Exception as e:
    logging.error(f"Error en la ingesta: {e}")
