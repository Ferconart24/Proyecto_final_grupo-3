import pandas as pd
import os

class GestorDatos:
    def __init__(self, path_csv):
        self.path_csv = path_csv
        self.df = None

    def cargar(self):
        """Carga el CSV original"""
        if not os.path.exists(self.path_csv):
            raise FileNotFoundError(f"No se encuentra el archivo: {self.path_csv}")
        self.df = pd.read_csv(self.path_csv)
        return self.df

    def limpiar(self):
        """Aplica limpieza básica"""
        df_clean = self.df.copy()

        # Quitar espacios extra en nombres de columnas
        if "Mes" in df_clean.columns:
            df_clean["Mes"] = df_clean["Mes"].str.strip()

        if "Puesto de Peaje" in df_clean.columns:
            df_clean["Puesto de Peaje"] = df_clean["Puesto de Peaje"].str.strip().str.title()

        # Eliminar filas donde el puesto de peaje sea "Naranjo"
        if "Puesto de Peaje" in df_clean.columns:
            df_clean = df_clean[df_clean["Puesto de Peaje"] != "Naranjo"]

        # Eliminar columna con demasiados nulos
        if "Cuatro Ejes" in df_clean.columns:
            df_clean = df_clean.drop(columns=["Cuatro Ejes"])

        # Rellenar nulos restantes con 0
        df_clean = df_clean.fillna(0)

        self.df = df_clean
        return self.df

    def exportar_csv(self, output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.df.to_csv(output_path, index=False)
        print(f"Archivo guardado en: {output_path}")
