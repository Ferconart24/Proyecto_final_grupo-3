import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.eda.ProcesadorEDA import ProcesadorEDA


def main():
    # --- Ruta base de datos ---
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

    if not os.path.exists(DATA_DIR):
        print(f"No se encontró la carpeta: {DATA_DIR}")
        return

    # --- Detectar todos los CSV ---
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    if not csv_files:
        print(f"No se encontraron archivos CSV en {DATA_DIR}")
        return

    dfs = {}
    for archivo in csv_files:
        ruta = os.path.join(DATA_DIR, archivo)
        try:
            df = pd.read_csv(ruta)
            nombre = os.path.splitext(archivo)[0]  # Nombre sin extensión
            dfs[nombre] = df
            print(f"Cargado: {archivo}")
        except Exception as e:
            print(f"No se pudo cargar {archivo}: {e}")

    if not dfs:
        print("No hay archivos válidos para procesar. Salida del programa.")
        return

    # --- Crear instancia de ProcesadorEDA ---
    eda = ProcesadorEDA(dfs)

    # --- Información general y estadísticas ---
    eda.info_general()
    eda.estadisticas()

    # --- Mostrar gráficos en pantalla ---
    print("\nMostrando gráficos...")
    for nombre, df in dfs.items():
        numeric_cols = df.select_dtypes(include="number").columns
        if len(numeric_cols) == 0:
            continue

        # Histogramas
        df[numeric_cols].hist(bins=30, figsize=(12, 8))
        plt.suptitle(f"Histogramas - {nombre}", fontsize=14)
        plt.show()

        # Boxplots
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df[numeric_cols])
        plt.title(f"Boxplots - {nombre}")
        plt.xticks(rotation=45)
        plt.show()

        # Matriz de correlación
        plt.figure(figsize=(10, 8))
        sns.heatmap(df[numeric_cols].corr(), annot=True, cmap="coolwarm", fmt=".2f")
        plt.title(f"Matriz de Correlación - {nombre}")
        plt.show()

    print("\nEDA completado. Todos los gráficos mostrados en pantalla.")


if __name__ == "__main__":
    main()
