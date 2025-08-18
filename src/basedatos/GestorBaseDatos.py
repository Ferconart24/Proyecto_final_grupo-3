import pyodbc
import pandas as pd

class GestorBaseDatos:
    def __init__(self, server, database):
        """
        Inicializa la conexión usando Windows Authentication.
        """
        self.conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
        self.conn = None

    def conectar(self):
        """Conecta a SQL Server usando Windows Authentication."""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            print("✅ Conexión establecida con SQL Server (Windows Authentication)")
        except Exception as e:
            self.conn = None
            print("❌ Error en la conexión:", e)

    def crear_tabla_desde_dataframe(self, df, tabla):
        """Crea una tabla automáticamente según el DataFrame"""
        if not self.conn:
            print("❌ No hay conexión activa.")
            return

        cursor = self.conn.cursor()

        tipo_sql = {
            "int64": "INT",
            "float64": "FLOAT",
            "object": "NVARCHAR(255)",
            "bool": "BIT",
            "datetime64[ns]": "DATETIME"
        }

        columnas_sql = [f"[{col}] {tipo_sql.get(str(dtype), 'NVARCHAR(255)')}"
                        for col, dtype in df.dtypes.items()]
        columnas_sql_str = ", ".join(columnas_sql)

        query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{tabla}' AND xtype='U')
        CREATE TABLE {tabla} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            {columnas_sql_str}
        )
        """
        cursor.execute(query)
        self.conn.commit()
        print(f"✅ Tabla '{tabla}' creada/verificada en SQL Server")

    def insertar_dataframe(self, df, tabla):
        """Inserta un DataFrame completo en la tabla"""
        if not self.conn:
            print("❌ No hay conexión activa.")
            return

        cursor = self.conn.cursor()
        columnas = ",".join([f"[{c}]" for c in df.columns])
        placeholders = ",".join("?" * len(df.columns))
        query = f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})"

        for _, row in df.iterrows():
            cursor.execute(query, tuple(row))

        self.conn.commit()
        print(f"✅ {len(df)} registros insertados en '{tabla}'")

    def consultar(self, query):
        """Ejecuta una consulta y devuelve un DataFrame"""
        if not self.conn:
            print("❌ No hay conexión activa.")
            return pd.DataFrame()
        return pd.read_sql(query, self.conn)

    def cerrar(self):
        """Cierra la conexión si existe"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("🔒 Conexión cerrada")
        else:
            print("❌ No había conexión activa para cerrar")
