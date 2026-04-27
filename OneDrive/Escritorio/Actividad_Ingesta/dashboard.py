import sqlite3
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Estilo base minimalista
plt.style.use('seaborn-v0_8-whitegrid')

#Conexión a la BD
ruta_db = "data/ventas.db"
conexion = sqlite3.connect(ruta_db)
cursor = conexion.cursor()

# Consultamos (Usamos ASC para que el más vendido quede arriba en el gráfico)
cursor.execute('''
    SELECT producto, SUM(precio_total) as total_generado
    FROM ventas_procesadas
    GROUP BY producto
    ORDER BY total_generado ASC
''')

datos = cursor.fetchall()
conexion.close()

# Separamos los datos para el gráfico
productos = [fila[0] for fila in datos]
ingresos = [fila[1] for fila in datos]

#Diseñamos el Gráfico Horizontal (Mucho más elegante para nombres largos)
fig, ax = plt.subplots(figsize=(12, 8))

#Lógica de color: Celeste para todos, Azul oscuro para el producto top ventas
colores = ['#A9C2D9'] * (len(productos) - 1) + ['#1D3557']
barras = ax.barh(productos, ingresos, color=colores, edgecolor='none')

#Estilo Profesional
ax.set_title('Dashboard de Ventas: Top Ingresos por Prenda', fontsize=18, fontweight='bold', pad=20, color='#1D3557')
ax.set_xlabel('Ingresos Totales (CLP)', fontsize=12, labelpad=10, fontweight='bold')

#Limpiamos los bordes cuadrados feos para un look más moderno
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_color('#CCCCCC')

#Formateamos el eje X para que los números tengan el punto de los miles (ej: 150.000)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'${int(x):,}'.replace(',', '.')))

#Le ponemos el valor exacto al lado derecho de cada barra
for i, barra in enumerate(barras):
    valor = ingresos[i]
    # Formateamos el texto con el punto de mil
    texto_valor = f'${valor:,}'.replace(',', '.')
    
    # Destacamos en negrita solo el texto del producto ganador
    peso = 'bold' if i == len(productos) - 1 else 'normal'
    color_texto = '#1D3557' if i == len(productos) - 1 else '#555555'
    
    ax.text(valor + (max(ingresos)*0.01), barra.get_y() + barra.get_height()/2, 
            texto_valor, va='center', ha='left', fontsize=11, fontweight=peso, color=color_texto)

#Mostramos Dashboard
plt.tight_layout()
plt.show()