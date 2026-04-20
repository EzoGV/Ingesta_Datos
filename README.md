# Pipeline de Ingesta Automatizada - Actividad 2.1

Script automatizado en Python para la ingesta de datos. El proceso mueve archivos desde una zona de origen a una zona historizada (raw), aplicando marcas de tiempo para evitar la pérdida de datos.

## ¿Qué hace el script?
El script `ingesta.py` automatiza la copia y el respaldo de datos. Traslada los archivos a una nueva carpeta y les añade la fecha y hora en el nombre (ingesta incremental) para mantener un historial ordenado sin borrar datos anteriores. Además, muestra un reporte de éxito y filas procesadas en la consola.

## Estructura del Proyecto

Actividad_Ingesta/
 ├── origen/
 |   |
 │   └── ventas.csv          # Archivo fuente original
 ├── data/
 |   |
 │   └── raw/                # Carpeta de destino (historizada)
 |
 ├── ingesta.py              # Script de automatización
 |
 └── README.md               # Documentación


## Archivos Utilizados
Entrada: origen/ventas.csv

Salida: data/raw/ventas_[timestamp].csv

## Instrucciones de Ejecución
Preparación: Colocar el archivo ventas.csv en la carpeta origen/.

Ejecución: Ejecutar el script desde la terminal:

Bash
python ingesta.py

### Verificación:
 - Revisar los logs en consola y la carpeta data/raw/ para confirmar la ingesta.

### Funciones Automatizadas
- Copia de archivos:
  -  Uso de shutil para traslado programático.

### Historización:
- Generación de nombres únicos con datetime (Nivel Avanzado).

### Trazabilidad: 
- Registro de inicio, éxito/error y conteo de filas mediante logging.

