#datos/ → Clase GestorDatos: encargada de cargar, transformar y exportar
#archivos CSV, Excel, etc.

import pandas as pd

# 1. Cargar archivo

df = pd.read_csv("C:\\Users\\victo\OneDrive\\Escritorio\\New folder\\Proyecto_final_grupo-3\\data\\raw\Datos_Abiertos_ARESEP_Flujo_vehicular_CONAVI_.csv")

# 2. Copia de seguridad para trabajar
df_clean = df.copy()

#  3. Limpieza básica
# Quitar espacios extra en nombres de meses
df_clean["Mes"] = df_clean["Mes"].str.strip()

# Normalizar texto en columnas de tipo object (en este caso Puesto de Peaje)
df_clean["Puesto de Peaje"] = df_clean["Puesto de Peaje"].str.strip().str.title()

# 4. Eliminar filas donde el puesto de peaje sea "Naranjo"
df_clean = df_clean[df_clean["Puesto de Peaje"] != "Naranjo"]

# 5. Manejo de valores nulos
# Eliminar columna con demasiados nulos (Cuatro Ejes)
if "Cuatro Ejes" in df_clean.columns:
    df_clean = df_clean.drop(columns=["Cuatro Ejes"])

# Rellenar nulos restantes con 0 (porque son conteos de vehículos)
df_clean = df_clean.fillna(0)

# 6. Guardar dataset limpio (solo columnas originales)
output_path = "peajes_clean.csv"
df_clean.to_csv(output_path, index=False)

print(" Limpieza completada (sin Naranjo, sin columnas extra).")
print(f"Archivo guardado en: {output_path}")