# Databricks notebook source

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "#f8f9fa",
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "font.size":        11,
})

print("DASHBOARD - ETL Medallion Pipeline")
print("=" * 50)

# COMMAND ----------

# GRAFICO 1 - Top 10 Directores por Ganancia
print("[1] Generando: Top Directores por Ganancia...")

df_dir = (
    spark.read.table("gold.genre_analytics")
    .filter("director != 'Unknown'")
    .orderBy("ganancia_promedio_usd", ascending=False)
    .limit(10)
    .toPandas()
)

colores = [
    "#2ecc71" if c == "Exitoso" else "#f39c12" if c == "Rentable" else "#e74c3c"
    for c in df_dir["categoria_rendimiento"]
]

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(df_dir["director"], df_dir["ganancia_promedio_usd"] / 1_000_000, color=colores)
ax.set_xlabel("Ganancia Promedio (millones USD)")
ax.set_title("Top 10 Directores por Ganancia Promedio", fontsize=14, fontweight="bold")
ax.invert_yaxis()

for bar, val in zip(bars, df_dir["ganancia_promedio_usd"] / 1_000_000):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
            f"${val:,.1f}M", va="center", fontsize=9)

legend = [
    Patch(color="#2ecc71", label="Exitoso"),
    Patch(color="#f39c12", label="Rentable"),
    Patch(color="#e74c3c", label="Deficitario"),
]
ax.legend(handles=legend, loc="lower right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# GRAFICO 2 - Top 10 Marcas de Autos por Precio
print("[2] Generando: Top Marcas de Autos...")

df_autos = (
    spark.read.table("gold.auto_brand_analytics")
    .orderBy("precio_promedio", ascending=False)
    .limit(10)
    .toPandas()
)

colores_fuel = {"gas": "#3498db", "diesel": "#e67e22"}
colores2 = [colores_fuel.get(str(f).lower(), "#95a5a6") for f in df_autos["fuel_type"]]

fig, ax = plt.subplots(figsize=(12, 6))
bars2 = ax.bar(df_autos["make"], df_autos["precio_promedio"], color=colores2, edgecolor="white")
ax.set_ylabel("Precio Promedio (USD)")
ax.set_title("Top 10 Marcas de Autos por Precio Promedio", fontsize=14, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.xticks(rotation=30, ha="right")

for bar, eff in zip(bars2, df_autos["eficiencia_promedio_mpg"]):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
            f"{eff:.0f} mpg", ha="center", fontsize=8, color="#555")

legend2 = [Patch(color="#3498db", label="Gasolina"), Patch(color="#e67e22", label="Diesel")]
ax.legend(handles=legend2)
plt.tight_layout()
plt.show()

# COMMAND ----------

# GRAFICO 3 - Distribucion del Mercado Combinado
print("[3] Generando: Resumen de Mercado Combinado...")

df_comb = spark.read.table("gold.market_summary").toPandas()

df_autos_g  = df_comb[df_comb["categoria"] == "Automovil"].sort_values("total_items", ascending=False)
df_movies_g = df_comb[df_comb["categoria"] == "Pelicula"].sort_values("total_items", ascending=False)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

wedge_props = {"width": 0.5, "edgecolor": "white", "linewidth": 2}
colors_autos = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12"]

if len(df_autos_g) > 0:
    ax1.pie(df_autos_g["total_items"], labels=df_autos_g["segmento"],
            autopct="%1.1f%%", colors=colors_autos[:len(df_autos_g)],
            wedgeprops=wedge_props, startangle=90)
ax1.set_title("Autos por Segmento de Precio", fontsize=13, fontweight="bold")

colors_movies = ["#9b59b6", "#1abc9c", "#e74c3c"]
if len(df_movies_g) > 0:
    ax2.bar(df_movies_g["segmento"], df_movies_g["total_items"],
            color=colors_movies[:len(df_movies_g)], edgecolor="white")
    for i, (seg, total) in enumerate(zip(df_movies_g["segmento"], df_movies_g["total_items"])):
        ax2.text(i, total + 0.5, str(total), ha="center", fontweight="bold")
ax2.set_ylabel("Total de Peliculas")
ax2.set_title("Peliculas por Categoria de Rendimiento", fontsize=13, fontweight="bold")

plt.suptitle("Resumen de Mercado - Autos y Peliculas", fontsize=15, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

print("Dashboard completado. Tablas consultadas:")
print("  - gold.genre_analytics")
print("  - gold.auto_brand_analytics")
print("  - gold.market_summary")
