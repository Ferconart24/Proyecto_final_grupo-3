#basedatos/ → Clase GestorBaseDatos: conecta con SQLite, MySQL, PostgreSQL
#o SQL Server y permite ejecutar consultas.

import pyodbc
import pandas as pd

class GestorBaseDatos:
    def _init_(self, server, database, username, password):
        self.conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        self.conn = None

    def conectar(self):
        try:
            self.conn = pyodbc.connect(self.conn_str)
            print("✅ Conexión establecida con SQL Server")
        except Exception as e:
            print("❌ Error en la conexión:", e)

    def insertar_dataframe(self, df, tabla):
        cursor = self.conn.cursor()
        for _, row in df.iterrows():
            placeholders = ",".join("?" * len(row))
            query = f"INSERT INTO {tabla} VALUES ({placeholders})"
            cursor.execute(query, tuple(row))
        self.conn.commit()
        print(f"✅ {len(df)} registros insertados en {tabla}")

    def consultar(self, query):
        return pd.read_sql(query, self.conn)