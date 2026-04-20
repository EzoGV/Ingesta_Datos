# Pipeline de Ingesta de Datos – Ventas STK

Proyecto de automatización para la primera etapa de un pipeline de datos:
ingesta incremental desde un archivo CSV de ventas.

## ¿Qué hace este script?

El script `ingesta.py` lee el archivo `origen/ventas.csv` y procesa
**solo los registros nuevos** que no hayan sido ingestados anteriormente,
usando un archivo de control (`checkpoint.json`) para recordar hasta
dónde llegó en la ejecución anterior.

### Características
-  Ingesta incremental (no reprocesa registros ya cargados)
-  Logging con timestamp en cada paso
-  Validación de existencia del archivo fuente
-  Conteo de registros procesados
-  Historial acumulativo en `data/raw/ventas_incremental.csv`

## Archivo fuente

| Campo        | Descripción                  |
|--------------|------------------------------|
| id_venta     | Identificador único de venta |
| fecha        | Fecha de la transacción      |
| producto     | Nombre del producto          |
| cantidad     | Unidades vendidas            |
| precio_total | Monto total de la venta      |

Ubicación: `origen/ventas.csv`

## Cómo ejecutarlo

### Requisitos
- Python 3.8 o superior
- No requiere librerías externas (solo módulos estándar)

### Ejecución

```bash
python ingesta.py
```
