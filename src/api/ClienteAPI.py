import os
import requests
import pandas as pd

class ClienteAPI:
    def __init__(self):
        self.latitude = 9.9281
        self.longitude = -84.0907
        self.variables = "temperature_2m_max,temperature_2m_min,precipitation_sum"
        self.years = [2020, 2021, 2022, 2023]
        self.df = None

        # Carpeta donde se guardará el CSV
        base_dir = os.path.dirname(os.path.abspath(__file__))
        carpeta_processed = os.path.join(base_dir, "..", "data", "processed")
        os.makedirs(carpeta_processed, exist_ok=True)
        self.csv_path = os.path.join(carpeta_processed, "clima_mensual_2020_2023.csv")

    def obtener_datos(self):
        all_data = []
        for year in self.years:
            url = (
                f"https://archive-api.open-meteo.com/v1/era5?"
                f"latitude={self.latitude}&longitude={self.longitude}"
                f"&daily={self.variables}"
                f"&start_date={year}-01-01&end_date={year}-12-31"
                f"&timezone=auto"
            )
            response = requests.get(url)
            data = response.json()
            df_year = pd.DataFrame(data["daily"])
            all_data.append(df_year)

        df = pd.concat(all_data, ignore_index=True)
        df["time"] = pd.to_datetime(df["time"])
        df["Anio"] = df["time"].dt.year
        df["Mes"] = df["time"].dt.month

        df_mensual = df.groupby(["Anio", "Mes"], as_index=False).agg({
            "temperature_2m_max": "mean",
            "temperature_2m_min": "mean",
            "precipitation_sum": "mean"
        })

        # Renombrar columnas para SQL Server
        df_mensual.rename(columns={
            "temperature_2m_max": "TempMax",
            "temperature_2m_min": "TempMin",
            "precipitation_sum": "Precipitacion"
        }, inplace=True)

        self.df = df_mensual
        return self.df

    def exportar_csv(self):
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        self.df.to_csv(self.csv_path, index=False)
        print(f"CSV de clima guardado en: {self.csv_path}")
