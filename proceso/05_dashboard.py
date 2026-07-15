# Databricks notebook source
# ═══════════════════════════════════════════════════════
# DASHBOARD — Visualización de tablas Gold
# Genera gráficos desde las 3 tablas de la capa Gold
# ═══════════════════════════════════════════════════════

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from config.settings import TBL_GOLD_GENRE, TBL_GOLD_AUTOS_BRAND, TBL_GOLD_COMBINED

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "#f8f9fa",
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "font.size":        11,
})

print("=" * 60)
print("DASHBOARD — ETL Medallion Pipeline")
print("=" * 60)

# ──────────────────────────────────────────
# GRÁFICO 1 — Top 10 Directores por Ganancia
# ──────────────────────────────────────────
print("\n[1] Generando: Top Directores por Ganancia...")

df_dir = (
    spark.read.table(TBL_GOLD_GENRE)
    .filter("director != 'Unknown'")
    .orderBy("ganancia_promedio_usd", ascending=False)
    .limit(10)
    .toPandas()
)

colores = ["#2ecc71" if c == "Exitoso" else "#f39c12" if c == "Rentable" else "#e74c3c"
           for c in df_dir["categoria_rendimiento"]]

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(df_dir["director"], df_dir["ganancia_promedio_usd"] / 1_000_000, color=colores)
ax.set_xlabel("Ganancia Promedio (millones USD)")
ax.set_title("Top 10 Directores por Ganancia Promedio", fontsize=14, fontweight="bold", pad=15)
ax.invert_yaxis()

for bar, val in zip(bars, df_dir["ganancia_promedio_usd"] / 1_000_000):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
            f"${val:,.1f}M", va="center", fontsize=9)

from matplotlib.patches import Patch
legend = [Patch(color="#2ecc71", label="Exitoso"),
          Patch(color="#f39c12", label="Rentable"),
          Patch(color="#e74c3c", label="Deficitario")]
ax.legend(handles=legend, loc="lower right")

plt.tight_layout()
plt.show()
print("[OK] Gráfico 1 mostrado.")

# ──────────────────────────────────────────
# GRÁFICO 2 — Top 10 Marcas de Autos por Precio Promedio
# ──────────────────────────────────────────
print("\n[2] Generando: Top Marcas de Autos...")

df_autos = (
    spark.read.table(TBL_GOLD_AUTOS_BRAND)
    .orderBy("precio_promedio", ascending=False)
    .limit(10)
    .toPandas()
)

colores_fuel = {"gas": "#3498db", "diesel": "#e67e22"}
colores2 = [colores_fuel.get(f.lower(), "#95a5a6") for f in df_autos["fuel_type"]]

fig, ax = plt.subplots(figsize=(12, 6))
bars2 = ax.bar(df_autos["make"], df_autos["precio_promedio"], color=colores2, edgecolor="white", linewidth=0.8)
ax.set_ylabel("Precio Promedio (USD)")
ax.set_title("Top 10 Marcas de Autos por Precio Promedio", fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.xticks(rotation=30, ha="right")

for bar, eff in zip(bars2, df_autos["eficiencia_promedio_mpg"]):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
            f"{eff:.0f} mpg", ha="center", fontsize=8, color="#555")

legend2 = [Patch(color="#3498db", label="Gasolina"), Patch(color="#e67e22", label="Diesel")]
ax.legend(handles=legend2)

plt.tight_layout()
plt.show()
print("[OK] Gráfico 2 mostrado.")

# ──────────────────────────────────────────
# GRÁFICO 3 — Distribución del Mercado Combinado
# ──────────────────────────────────────────
print("\n[3] Generando: Resumen de Mercado Combinado...")

df_comb = spark.read.table(TBL_GOLD_COMBINED).toPandas()

df_autos_g  = df_comb[df_comb["categoria"] == "Automóvil"].sort_values("total_items", ascending=False)
df_movies_g = df_comb[df_comb["categoria"] == "Película"].sort_values("total_items", ascending=False)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Autos — dona
wedge_props = {"width": 0.5, "edgecolor": "white", "linewidth": 2}
colors_autos = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12"]
ax1.pie(df_autos_g["total_items"], labels=df_autos_g["segmento"],
        autopct="%1.1f%%", colors=colors_autos[:len(df_autos_g)],
        wedgeprops=wedge_props, startangle=90)
ax1.set_title("Distribución de Autos\npor Segmento de Precio", fontsize=13, fontweight="bold")

# Películas — barras
colors_movies = ["#9b59b6", "#1abc9c", "#e74c3c"]
ax2.bar(df_movies_g["segmento"], df_movies_g["total_items"],
        color=colors_movies[:len(df_movies_g)], edgecolor="white", linewidth=0.8)
ax2.set_ylabel("Total de Películas")
ax2.set_title("Películas por\nCategoría de Rendimiento", fontsize=13, fontweight="bold")
for i, (seg, total) in enumerate(zip(df_movies_g["segmento"], df_movies_g["total_items"])):
    ax2.text(i, total + 0.5, str(total), ha="center", fontweight="bold")

plt.suptitle("Resumen de Mercado — Autos y Películas", fontsize=15, fontweight="bold", y=1.02)
plt.tight_layout()
plt.show()
print("[OK] Gráfico 3 mostrado.")

print("\n[OK] Dashboard completado.")
print("Tablas consultadas:")
print(f"  - {TBL_GOLD_GENRE}")
print(f"  - {TBL_GOLD_AUTOS_BRAND}")
print(f"  - {TBL_GOLD_COMBINED}")
