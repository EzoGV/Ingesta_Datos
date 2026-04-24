# Pipeline de Datos: Ingesta, Limpieza y Visualización (Actividad 2.1)

Este proyecto implementa un flujo de datos automatizado (ETL) para la gestión de ventas de una tienda de ropa urbana. El sistema cumple con el ciclo completo de procesamiento: desde la extracción de fuentes crudas hasta la visualización en un dashboard.

##  Flujo del Sistema
Siguiendo el esquema de arquitectura solicitado, el pipeline opera de la siguiente manera:

1. **Fuente (Source):** Lectura de datos desde `origen/ventas.csv`.
2. **Ingesta y Limpieza:** El script `ingesta.py` extrae los datos y realiza una transformación en memoria (Mayúsculas, limpieza de espacios, tipado de datos y eliminación de duplicados).
3. **Almacenamiento (Base de Datos):** Carga de datos optimizados en una base de datos **SQLite** (`data/ventas.db`) en la tabla `ventas_procesadas`.
4. **Dashboard:** Generación de métricas visuales mediante el script `dashboard.py`.

##  Estructura del Proyecto
* `origen/ventas.csv`: Archivo con los datos de ventas originales.
* `data/ventas.db`: Base de datos relacional donde se almacena la información limpia.
* `ingesta.py`: Script encargado de la extracción, limpieza y carga (ETL).
* `dashboard.py`: Script encargado de la visualización de datos.

##  Requisitos
Es necesario tener instalado Python 3.x y la librería de visualización:
```bash
pip install matplotlib


 Instrucciones de Ejecución
Para poner en marcha el pipeline de datos, asegúrate de estar parado en la carpeta raíz del proyecto (Actividad_Ingesta) y sigue este orden:

1. Preparación de la Fuente
Verifica que el archivo con los datos crudos esté en la ruta correcta:

origen/ventas.csv

2. Ingesta y Limpieza (Proceso ETL)
Ejecuta el script principal para procesar los datos. Este paso leerá el CSV, aplicará las reglas de limpieza en memoria y cargará la información en la base de datos SQLite:

Bash
python ingesta.py
Al finalizar, verás un mensaje en la terminal confirmando cuántos registros fueron inyectados en data/ventas.db.

3. Visualización (Dashboard)
Una vez que los datos estén procesados y guardados, genera el reporte gráfico con el siguiente comando:

Bash
python dashboard.py
Se abrirá una ventana emergente con un gráfico de barras que detalla el rendimiento de ventas por producto.