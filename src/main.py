import os
from datos.GestorDatos import GestorDatos
from basedatos.GestorBaseDatos import GestorBaseDatos
from api.ClienteAPI import ClienteAPI

# --- Directorios ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
carpeta_processed = os.path.join(BASE_DIR, "..", "data", "processed")
os.makedirs(carpeta_processed, exist_ok=True)

# --- 1️⃣ Datos de peajes ---
ruta_peajes = os.path.join(BASE_DIR, "..", "data", "raw", "Datos_Abiertos_ARESEP_Flujo_vehicular_CONAVI_.csv")
gestor_peajes = GestorDatos(ruta_peajes)
df_peajes = gestor_peajes.cargar()
df_peajes = gestor_peajes.limpiar()
ruta_peajes_clean = os.path.join(carpeta_processed, "peajes_clean.csv")
gestor_peajes.exportar_csv(ruta_peajes_clean)

# --- 2️⃣ Datos de clima desde API ---
cliente_api = ClienteAPI()
df_clima = cliente_api.obtener_datos()
cliente_api.exportar_csv()  # Guarda en data/processed

# --- 3️⃣ Cargar CSV clima con GestorDatos ---
gestor_clima = GestorDatos(cliente_api.csv_path)
df_clima = gestor_clima.cargar()

# --- 4️⃣ Conexión a SQL Server con Windows Authentication ---
gestor_db = GestorBaseDatos(
    server=r"DESKTOP-GQ1EGAS\JOHEL",
    database="ProyectoClimaContaminacion"
)
gestor_db.conectar()

# --- 5️⃣ Crear tablas si no existen ---
gestor_db.crear_tabla_desde_dataframe(df_peajes, "FlujoVehicular")
gestor_db.crear_tabla_desde_dataframe(df_clima, "ClimaMensual")

# --- 6️⃣ Insertar datos ---
gestor_db.insertar_dataframe(df_peajes, "FlujoVehicular")
gestor_db.insertar_dataframe(df_clima, "ClimaMensual")

# --- 7️⃣ Cerrar conexión ---
gestor_db.cerrar()
