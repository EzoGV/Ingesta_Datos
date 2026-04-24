import sqlite3
import matplotlib.pyplot as plt

# 1. Conexión a tu Base de Datos ya limpia
ruta_db = "data/ventas.db"
conexion = sqlite3.connect(ruta_db)
cursor = conexion.cursor()

# 2. Consultamos a la BD: ¿Cuánto dinero generó cada producto?
# Usamos SUM para sumar los precios totales agrupados por el nombre del producto
cursor.execute('''
    SELECT producto, SUM(precio_total) as total_generado
    FROM ventas_procesadas
    GROUP BY producto
    ORDER BY total_generado DESC
''')

datos = cursor.fetchall()
conexion.close()

# 3. Separamos los datos para el gráfico
productos = [fila[0] for fila in datos]
ingresos = [fila[1] for fila in datos]

# 4. Diseñamos el Gráfico (Dashboard)
plt.figure(figsize=(10, 6))
# Creamos un gráfico de barras con colores llamativos
barras = plt.bar(productos, ingresos, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])

# 5. Le ponemos estilo para que se vea profesional
plt.title('Dashboard de Ventas - Ropa Urbana', fontsize=16, fontweight='bold')
plt.xlabel('Prenda', fontsize=12)
plt.ylabel('Ingresos (CLP)', fontsize=12)
plt.xticks(rotation=15) # Inclinamos los nombres para que se lean bien
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Le ponemos el signo de peso y el valor exacto arriba de cada barra
for i, v in enumerate(ingresos):
    plt.text(i, v + 500, f'${v}', ha='center', fontweight='bold')

# 6. Mostramos Dashboard
plt.tight_layout()
plt.show()