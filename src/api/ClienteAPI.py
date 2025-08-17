#api/ → Clase ClienteAPI: realiza peticiones a APIs públicas y transforma los
#resultados en DataFrames.
import requests
import pandas as pd

latitude = 9.9281
longitude = -84.0907
variables = "temperature_2m_max,temperature_2m_min,precipitation_sum"
years = [2020, 2021, 2022, 2023]

all_data = []

for year in years:
    url = (
        f"https://archive-api.open-meteo.com/v1/era5?"
        f"latitude={latitude}&longitude={longitude}"
        f"&daily={variables}"
        f"&start_date={year}-01-01&end_date={year}-12-31"
        f"&timezone=auto"
    )

    response = requests.get(url)
    data = response.json()
    print(f"Datos {year}: ", data.keys())

    df_year = pd.DataFrame(data["daily"])
    all_data.append(df_year)

#  Concatenar todos los años
df = pd.concat(all_data, ignore_index=True)

# Convertir fechas
df["time"] = pd.to_datetime(df["time"])
df["Año"] = df["time"].dt.year
df["Mes"] = df["time"].dt.month

#  Promedios mensuales
df_mensual = (
    df.groupby(["Año", "Mes"], as_index=False)
    .agg({
        "temperature_2m_max": "mean",
        "temperature_2m_min": "mean",
        "precipitation_sum": "mean"
    })
)

# Guardar a CSV
df_mensual.to_csv("clima_mensual_2020_2023.csv", index=False)

print("Listo: se generaron promedios mensuales en clima_mensual_2020_2023.csv")

