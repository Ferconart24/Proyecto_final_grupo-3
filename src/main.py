import os
import json
import pandas as pd
from datos.GestorDatos import GestorDatos
from basedatos.GestorBaseDatos import GestorBaseDatos
from api.ClienteAPI import ClienteAPI

def main():
    # --- Directorios ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    carpeta_processed = os.path.join(BASE_DIR, "..", "data", "processed")
    os.makedirs(carpeta_processed, exist_ok=True)

    # ---  Datos de peajes ---
    ruta_peajes = os.path.join(BASE_DIR, "..", "data", "raw", "Datos_Abiertos_ARESEP_Flujo_vehicular_CONAVI_.csv")
    gestor_peajes = GestorDatos(ruta_peajes)
    df_peajes = gestor_peajes.cargar()
    df_peajes = gestor_peajes.limpiar()
    ruta_peajes_clean = os.path.join(carpeta_processed, "peajes_clean.csv")
    gestor_peajes.exportar_csv(ruta_peajes_clean)

    # ---  Datos de clima desde API ---
    cliente_api = ClienteAPI()
    df_clima = cliente_api.obtener_datos()
    cliente_api.exportar_csv()  # Guarda en data/processed

    # ---  Cargar CSV clima con GestorDatos ---
    gestor_clima = GestorDatos(cliente_api.csv_path)
    df_clima = gestor_clima.cargar()

    # ---  Conexión a SQL Server ---
    gestor_db = GestorBaseDatos(
        server=r"DESKTOP-GQ1EGAS\JOHEL",
        database="ProyectoClimaContaminacion"
    )
    gestor_db.conectar()

    # ---  Crear tablas si no existen ---
    gestor_db.crear_tabla_desde_dataframe(df_peajes, "FlujoVehicular")
    gestor_db.crear_tabla_desde_dataframe(df_clima, "ClimaMensual")

    # ---  Insertar datos ---
    gestor_db.insertar_dataframe(df_peajes, "FlujoVehicular")
    gestor_db.insertar_dataframe(df_clima, "ClimaMensual")

    # ---  Datos de contaminación mensual desde JSON (code.json) ---
    ruta_contaminacion = os.path.join(BASE_DIR, "..", "data", "raw", "code.json")
    with open(ruta_contaminacion, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Convertir el JSON a DataFrame
    df_contaminacion = pd.DataFrame(data)

    # Exportar copia limpia a processed
    ruta_contaminacion_csv = os.path.join(carpeta_processed, "contaminacion_mensual.csv")
    df_contaminacion.to_csv(ruta_contaminacion_csv, index=False, encoding="utf-8")

    # Crear tabla e insertar en SQL
    gestor_db.crear_tabla_desde_dataframe(df_contaminacion, "ContaminacionMensual")
    gestor_db.insertar_dataframe(df_contaminacion, "ContaminacionMensual")

    # ---  Integración de ClimaMensual y ContaminacionMensual ---
    df_clima_csv = pd.read_csv(os.path.join(carpeta_processed, "clima_mensual_2020_2023.csv"))
    df_cont_csv = pd.read_csv(os.path.join(carpeta_processed, "contaminacion_mensual.csv"))

    # Merge por Anio y Mes
    df_clima_contaminacion = pd.merge(df_clima_csv, df_cont_csv, on=["Anio", "Mes"], how="inner")

    # Exportar CSV final
    ruta_clima_cont_csv = os.path.join(carpeta_processed, "clima_contaminacion.csv")
    df_clima_contaminacion.to_csv(ruta_clima_cont_csv, index=False, encoding="utf-8")

    # Crear tabla ClimaContaminacion e insertar datos
    gestor_db.crear_tabla_desde_dataframe(df_clima_contaminacion, "ClimaContaminacion")
    gestor_db.insertar_dataframe(df_clima_contaminacion, "ClimaContaminacion")

    # ---  Cerrar conexión ---
    gestor_db.cerrar()

if __name__ == "__main__":
    main()
