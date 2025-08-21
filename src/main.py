import os  # Para manejo de rutas de archivos y carpetas
import sys  # Para modificar sys.path y poder importar módulos locales
import json  # Para leer archivos JSON
import pandas as pd  # Para manejo de dataframes
import matplotlib.pyplot as plt  # Para gráficos
import seaborn as sns  # Para gráficos estadísticos

# --- Añadir rutas para imports locales ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Carpeta actual del script
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # Carpeta raíz del proyecto
if BASE_DIR not in sys.path:  # Verificar si BASE_DIR está en sys.path
    sys.path.append(BASE_DIR)  # Agregar BASE_DIR a sys.path
if PROJECT_ROOT not in sys.path:  # Verificar si PROJECT_ROOT está en sys.path
    sys.path.append(PROJECT_ROOT)  # Agregar PROJECT_ROOT a sys.path

# --- Imports de tu proyecto ---
from datos.GestorDatos import GestorDatos  # Clase para cargar y limpiar datos
from basedatos.GestorBaseDatos import GestorBaseDatos  # Clase para SQL Server
from api.ClienteAPI import ClienteAPI  # Clase para obtener datos de clima vía API
from src.eda.ProcesadorEDA import ProcesadorEDA  # Clase para análisis exploratorio de datos
try:
    from modelos.ModeloML import ModeloML  # Modelo de ML
except ModuleNotFoundError:
    from modelos.ModeloML import ModeloML  # Alternativa si hay diferencia de nombre

def main():
    # --- Directorios ---
    carpeta_raw = os.path.join(PROJECT_ROOT, "data", "raw")  # Carpeta de datos crudos
    carpeta_processed = os.path.join(PROJECT_ROOT, "data", "processed")  # Carpeta de datos procesados
    os.makedirs(carpeta_processed, exist_ok=True)  # Crear carpeta processed si no existe
    carpeta_modelos = os.path.join(PROJECT_ROOT, "modelos")  # Carpeta para guardar modelos
    os.makedirs(carpeta_modelos, exist_ok=True)  # Crear carpeta modelos si no existe

    # ------------------- CARGA Y LIMPIEZA DE DATOS -------------------
    # Peajes
    ruta_peajes = os.path.join(carpeta_raw, "Datos_Abiertos_ARESEP_Flujo_vehicular_CONAVI_.csv")  # Ruta CSV peajes
    gestor_peajes = GestorDatos(ruta_peajes)  # Crear instancia de GestorDatos
    df_peajes = gestor_peajes.cargar()  # Cargar CSV
    df_peajes = gestor_peajes.limpiar()  # Limpiar datos
    ruta_peajes_clean = os.path.join(carpeta_processed, "peajes_clean.csv")  # Ruta para guardar CSV limpio
    gestor_peajes.exportar_csv(ruta_peajes_clean)  # Exportar CSV limpio

    # Clima desde API
    cliente_api = ClienteAPI()  # Crear cliente API
    df_clima = cliente_api.obtener_datos()  # Obtener datos de clima
    cliente_api.exportar_csv()  # Guardar CSV de clima en processed
    gestor_clima = GestorDatos(cliente_api.csv_path)  # Cargar CSV con GestorDatos
    df_clima_csv = gestor_clima.cargar()  # Leer CSV en dataframe

    # Contaminación desde JSON
    ruta_contaminacion = os.path.join(carpeta_raw, "code.json")  # Ruta archivo JSON
    with open(ruta_contaminacion, "r", encoding="utf-8") as f:  # Abrir JSON
        data = json.load(f)  # Leer JSON
    df_contaminacion = pd.DataFrame(data)  # Convertir JSON a dataframe
    ruta_contaminacion_csv = os.path.join(carpeta_processed, "contaminacion_mensual.csv")  # Ruta CSV procesado
    df_contaminacion.to_csv(ruta_contaminacion_csv, index=False, encoding="utf-8")  # Guardar CSV

    # ------------------- INTEGRACIÓN DE DATOS -------------------
    try:
        df_clima_contaminacion = pd.merge(
            df_clima_csv, df_contaminacion, on=["Anio", "Mes"], how="inner"  # Merge por Año y Mes
        )
        ruta_clima_cont_csv = os.path.join(carpeta_processed, "clima_contaminacion.csv")  # Ruta CSV final
        df_clima_contaminacion.to_csv(ruta_clima_cont_csv, index=False, encoding="utf-8")  # Guardar CSV final
    except Exception as e:  # Si hay error en la integración
        print(f"Error integrando Clima y Contaminación: {e}")  # Mostrar mensaje
        return  # Salir del programa

    # ------------------- VISUALIZACIÓN DE DATOS (EDA) -------------------
    dfs_eda = {
        "Peajes": df_peajes,
        "ClimaMensual": df_clima_csv,
        "ContaminacionMensual": df_contaminacion,
        "ClimaContaminacion": df_clima_contaminacion
    }  # Diccionario de dataframes para EDA
    eda = ProcesadorEDA(dfs_eda)  # Crear instancia de ProcesadorEDA
    eda.info_general()  # Mostrar info general de los dataframes
    eda.estadisticas()  # Mostrar estadísticas descriptivas

    print("\nMostrando gráficos...")  # Mensaje
    for nombre, df in dfs_eda.items():  # Iterar sobre los dataframes
        numeric_cols = df.select_dtypes(include="number").columns  # Columnas numéricas
        if len(numeric_cols) == 0:  # Si no hay columnas numéricas
            continue  # Saltar

        df[numeric_cols].hist(bins=30, figsize=(12,8))  # Histogramas
        plt.suptitle(f"Histogramas - {nombre}", fontsize=14)
        plt.show()  # Mostrar gráfico


        plt.figure(figsize=(10,8))  # Figura correlación
        sns.heatmap(df[numeric_cols].corr(), annot=True, cmap="coolwarm", fmt=".2f")  # Heatmap correlación
        plt.title(f"Matriz de Correlación - {nombre}")
        plt.show()  # Mostrar gráfico

    # ------------------- CONEXIÓN A SQL SERVER -------------------
    gestor_db = GestorBaseDatos(
        server=r"DESKTOP-GQ1EGAS\JOHEL",  # Servidor SQL
        database="ProyectoClimaContaminacion"  # Base de datos
    )
    gestor_db.conectar()  # Conectar a SQL Server
    gestor_db.crear_tabla_desde_dataframe(df_peajes, "FlujoVehicular")  # Crear tabla peajes
    gestor_db.crear_tabla_desde_dataframe(df_clima_csv, "ClimaMensual")  # Crear tabla clima
    gestor_db.crear_tabla_desde_dataframe(df_contaminacion, "ContaminacionMensual")  # Crear tabla contaminación
    gestor_db.crear_tabla_desde_dataframe(df_clima_contaminacion, "ClimaContaminacion")  # Crear tabla merge
    gestor_db.insertar_dataframe(df_peajes, "FlujoVehicular")  # Insertar datos peajes
    gestor_db.insertar_dataframe(df_clima_csv, "ClimaMensual")  # Insertar datos clima
    gestor_db.insertar_dataframe(df_contaminacion, "ContaminacionMensual")  # Insertar datos contaminación
    gestor_db.insertar_dataframe(df_clima_contaminacion, "ClimaContaminacion")  # Insertar datos merge

    # ------------------- MACHINE LEARNING -------------------
    print("\n--- Iniciando Machine Learning ---")
    features = ['pm10', 'CO', 'NO2', 'O3', 'TempMax', 'TempMin', 'Precipitacion', 'Mes']  # Features para ML
    modelo = ModeloML(df=df_clima_contaminacion, tipo_modelo="regresion", target_column="pm2_5")  # Crear modelo
    if modelo.prepare_data(features_list=features):  # Preparar datos
        modelo.train_model(algorithm="RandomForest")  # Entrenar modelo
        modelo.evaluate_model()  # Evaluar modelo
        modelo_path = os.path.join(carpeta_modelos, "modelo_pm25_regresion.joblib")  # Ruta para guardar modelo
        modelo.save_model(modelo_path)  # Guardar modelo

        if not modelo.X_test.empty:  # Si hay datos de test
            sample_new = modelo.X_test.head(1)  # Tomar primera fila
            pred = modelo.predict(sample_new)  # Predecir PM2.5
            print(f"Predicción PM2.5 para muestra: {pred[0]:.2f} μg/m³")  # Mostrar resultado

    print("\n--- Fin del proceso ---")  # Mensaje final
    gestor_db.cerrar()  # Cerrar conexión SQL


if __name__ == "__main__":  # Ejecutar main solo si se ejecuta el script directamente
    main()
