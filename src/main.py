import os
import json
import pandas as pd
# Añadir rutas al sys.path para que funcionen los imports al ejecutar como script
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from datos.GestorDatos import GestorDatos
from basedatos.GestorBaseDatos import GestorBaseDatos
from api.ClienteAPI import ClienteAPI
# Intentar ambas variantes de nombre de archivo para el modelo
try:
    from modelos.ModeloML import ModeloML  # archivo: modelos/modelo_ml.py
except ModuleNotFoundError:
    from modelos.ModeloML import ModeloML  # archivo alternativo: modelos/ModeloML.py

def main():
    # --- Directorios ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    carpeta_processed = os.path.join(BASE_DIR, "..", "data", "processed")
    os.makedirs(carpeta_processed, exist_ok=True)
    # Asegurar carpeta para modelos
    carpeta_modelos = os.path.join(BASE_DIR, "modelos")
    os.makedirs(carpeta_modelos, exist_ok=True)

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
    try:
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
    except Exception as e:
        print(f"Error al integrar datos de clima y contaminación: {e}")
        gestor_db.cerrar()
        return

    # --- Implementación del Modelo de Machine Learning ---
    print("\n--- Iniciando el proceso de Machine Learning ---")
    features_regresion = ['pm10', 'CO', 'NO2', 'O3', 'TempMax', 'TempMin', 'Precipitacion', 'Mes']

    modelo_regresion = ModeloML(df=df_clima_contaminacion, tipo_modelo="regresion", target_column="pm2_5")
    if modelo_regresion.prepare_data(features_list=features_regresion):
        modelo_regresion.train_model(algorithm="RandomForest")
        modelo_regresion.evaluate_model()
        modelo_path = os.path.join(carpeta_modelos, "modelo_pm25_regresion.joblib")
        modelo_regresion.save_model(modelo_path)

        if not modelo_regresion.X_test.empty:
            sample_new_data = modelo_regresion.X_test.head(1)
            prediction = modelo_regresion.predict(sample_new_data)
            print(f"\nPredicción de PM2.5 para una muestra: {prediction[0]:.2f} μg/m³")

    print("\n--- Fin del proceso de Machine Learning ---")

    # --- Cerrar conexión ---
    gestor_db.cerrar()

if __name__ == "__main__":
    main()
