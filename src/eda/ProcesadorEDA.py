import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

class ProcesadorEDA:
    def __init__(self, dfs: dict):
        """
        dfs: diccionario con nombre -> DataFrame
        """
        self.dfs = dfs

    def info_general(self):
        """Muestra shape, tipos, nulos y duplicados en consola"""
        for nombre, df in self.dfs.items():
            print(f"\n📄 Información general - {nombre}")
            print("Shape:", df.shape)
            print("Tipos de datos:\n", df.dtypes)
            print("Valores nulos:\n", df.isnull().sum())
            print("Duplicados:", df.duplicated().sum())
            print("Primeras filas:\n", df.head(3))

    def estadisticas(self):
        """Estadísticas descriptivas"""
        for nombre, df in self.dfs.items():
            print(f"\n📊 Estadísticas descriptivas - {nombre}")
            print(df.describe(include="all"))

    def histogramas(self, output_dir=None):
        """Genera histogramas y opcionalmente los guarda"""
        for nombre, df in self.dfs.items():
            numeric_cols = df.select_dtypes(include="number").columns
            if len(numeric_cols) == 0:
                continue
            fig, ax = plt.subplots(figsize=(12,8))
            df[numeric_cols].hist(bins=30, ax=ax)
            plt.suptitle(f"Histogramas - {nombre}", fontsize=14)
            if output_dir:
                path = os.path.join(output_dir, f"{nombre}_histogramas.png")
                fig.savefig(path)
            plt.close(fig)

    def boxplots(self, output_dir=None):
        """Genera boxplots y opcionalmente los guarda"""
        for nombre, df in self.dfs.items():
            numeric_cols = df.select_dtypes(include="number").columns
            if len(numeric_cols) == 0:
                continue
            fig, ax = plt.subplots(figsize=(12,6))
            sns.boxplot(data=df[numeric_cols], ax=ax)
            ax.set_title(f"Boxplots - {nombre}")
            plt.xticks(rotation=45)
            if output_dir:
                path = os.path.join(output_dir, f"{nombre}_boxplots.png")
                fig.savefig(path)
            plt.close(fig)

    def correlaciones(self, output_dir=None):
        """Matriz de correlación con heatmap y opcionalmente guardada"""
        for nombre, df in self.dfs.items():
            numeric_cols = df.select_dtypes(include="number").columns
            if len(numeric_cols) == 0:
                continue
            fig, ax = plt.subplots(figsize=(10,8))
            sns.heatmap(df[numeric_cols].corr(), annot=True, cmap="coolwarm", fmt=".2f", ax=ax)
            ax.set_title(f"Matriz de Correlación - {nombre}")
            if output_dir:
                path = os.path.join(output_dir, f"{nombre}_correlacion.png")
                fig.savefig(path)
            plt.close(fig)

