#modelos/ → Clase ModeloML: entrena y evalúa modelos supervisados (regresión
#o clasificación).

# src/modelos/modelo_ml.py
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.neighbors import KNeighborsRegressor, KNeighborsClassifier
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report
import joblib  # Para guardar y cargar modelos


class ModeloML:
    def __init__(self, df, tipo_modelo="regresion", target_column="pm2_5"):
        """
        Inicializa la clase ModeloML.

        Args:
            df (pd.DataFrame): DataFrame con los datos a utilizar para el modelo.
            tipo_modelo (str): 'regresion' para predecir valores continuos (e.g., PM2.5),
                               'clasificacion' para categorizar (e.g., calidad del aire).
            target_column (str): Nombre de la columna objetivo.
        """
        self.df = df.copy()
        self.tipo_modelo = tipo_modelo
        self.target_column = target_column
        self.model = None
        self.X_train, self.X_test, self.y_train, self.y_test = None, None, None, None
        self.features = []  # Se definirá en prepare_data

    def prepare_data(self, features_list=None):
        """
        Prepara los datos para el entrenamiento del modelo.
        Realiza la selección de características y la división en conjuntos de entrenamiento y prueba.

        Args:
            features_list (list, optional): Lista de nombres de columnas a usar como características.
                                            Si es None, se intentarán usar características predefinidas.
        """
        if self.df is None or self.df.empty:
            print("Error: DataFrame vacío o no cargado.")
            return False

        if self.target_column not in self.df.columns:
            print(f"Error: La columna objetivo '{self.target_column}' no se encuentra en el DataFrame.")
            return False

        # Definir características por defecto si no se proporcionan
        if features_list:
            self.features = features_list
        else:
            # Ejemplo de características basadas en los datos disponibles
            # Asegúrate de que estas columnas existan en tu df_clima_contaminacion
            # y que hayas integrado los datos de tráfico si los necesitas.
            if self.tipo_modelo == "regresion":
                self.features = [col for col in
                                 ['pm10', 'CO', 'NO2', 'O3', 'TempMax', 'TempMin', 'Precipitacion', 'Anio', 'Mes'] if
                                 col in self.df.columns and col != self.target_column]
                # Si tienes datos de tráfico integrados, podrías añadir 'Total' o 'Liviano' etc.
                # self.features.extend([col for col in ['Total', 'Liviano'] if col in self.df.columns])
            elif self.tipo_modelo == "clasificacion":
                self.features = [col for col in
                                 ['pm10', 'CO', 'NO2', 'O3', 'TempMax', 'TempMin', 'Precipitacion', 'Anio', 'Mes'] if
                                 col in self.df.columns and col != self.target_column]
                # Para clasificación, la columna objetivo 'target_column' debe ser categórica.
                # Necesitarás una función para convertir pm2_5 a categorías ICA si ese es tu objetivo.

        # Filtrar solo las características que realmente existen en el DataFrame
        self.features = [f for f in self.features if f in self.df.columns]

        if not self.features:
            print("Error: No se encontraron características válidas para el entrenamiento.")
            return False

        X = self.df[self.features]
        y = self.df[self.target_column]

        # Manejar valores infinitos o muy grandes que puedan causar problemas
        X = X.replace([float('inf'), -float('inf')], pd.NA).fillna(X.mean())
        y = y.replace([float('inf'), -float('inf')], pd.NA).fillna(
            y.mean() if pd.api.types.is_numeric_dtype(y) else y.mode()[0])

        # Eliminar filas con NaN si aún quedan después del fillna (por si alguna columna es completamente NaN)
        combined_df = pd.concat([X, y], axis=1).dropna()
        X = combined_df[self.features]
        y = combined_df[self.target_column]

        if X.empty or y.empty:
            print("Error: Datos insuficientes después de la limpieza para la división de entrenamiento/prueba.")
            return False

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        print(f"Datos preparados. X_train shape: {self.X_train.shape}, y_train shape: {self.y_train.shape}")
        return True

    def train_model(self, algorithm="LinearRegression"):
        """
        Entrena el modelo de Machine Learning.

        Args:
            algorithm (str): Algoritmo a utilizar ('LinearRegression', 'KNN', 'RandomForest', 'DecisionTree', 'LogisticRegression').
                             'LogisticRegression' solo para clasificación.
        """
        if self.X_train is None:
            print("Error: Los datos no han sido preparados. Ejecuta prepare_data() primero.")
            return

        print(f"Entrenando modelo de {self.tipo_modelo} con algoritmo: {algorithm}...")

        if self.tipo_modelo == "regresion":
            if algorithm == "LinearRegression":
                self.model = LinearRegression()
            elif algorithm == "KNN":
                self.model = KNeighborsRegressor()
            elif algorithm == "RandomForest":
                self.model = RandomForestRegressor(random_state=42)
            elif algorithm == "DecisionTree":
                self.model = DecisionTreeRegressor(random_state=42)
            else:
                print("Algoritmo de regresión no soportado o incorrecto.")
                return
        elif self.tipo_modelo == "clasificacion":
            if algorithm == "LogisticRegression":
                self.model = LogisticRegression(random_state=42, max_iter=1000)
            elif algorithm == "KNN":
                self.model = KNeighborsClassifier()
            elif algorithm == "RandomForest":
                self.model = RandomForestClassifier(random_state=42)
            elif algorithm == "DecisionTree":
                self.model = DecisionTreeClassifier(random_state=42)
            else:
                print("Algoritmo de clasificación no soportado o incorrecto.")
                return
        else:
            print("Tipo de modelo no soportado. Debe ser 'regresion' o 'clasificacion'.")
            return

        self.model.fit(self.X_train, self.y_train)
        print("Modelo entrenado exitosamente.")

    def evaluate_model(self):
        """Evalúa el rendimiento del modelo entrenado."""
        if self.model is None:
            print("Error: El modelo no ha sido entrenado. Ejecuta train_model() primero.")
            return

        if self.X_test is None:
            print("Error: Los datos de prueba no están disponibles.")
            return

        y_pred = self.model.predict(self.X_test)

        print("\n--- Evaluación del Modelo ---")
        if self.tipo_modelo == "regresion":
            mse = mean_squared_error(self.y_test, y_pred)
            r2 = r2_score(self.y_test, y_pred)
            print(f"Error Cuadrático Medio (MSE): {mse:.2f}")
            print(f"Coeficiente de Determinación (R2): {r2:.2f}")
        elif self.tipo_modelo == "clasificacion":
            accuracy = accuracy_score(self.y_test, y_pred)
            print(f"Precisión (Accuracy): {accuracy:.2f}")
            print("\nReporte de Clasificación:")
            print(classification_report(self.y_test, y_pred))

    def predict(self, new_data):
        """
        Realiza predicciones con el modelo entrenado.

        Args:
            new_data (pd.DataFrame): DataFrame con nuevas observaciones para predecir.
                                     Debe tener las mismas columnas que las características de entrenamiento.

        Returns:
            np.array: Predicciones del modelo.
        """
        if self.model is None:
            print("Error: El modelo no ha sido entrenado. Ejecuta train_model() primero.")
            return None
        if self.features is None or not self.features:
            print("Error: Las características del modelo no están definidas.")
            return None

        # Asegurarse de que new_data tenga las mismas columnas y en el mismo orden
        new_data_processed = new_data[self.features]

        # Manejar valores infinitos o muy grandes en new_data
        new_data_processed = new_data_processed.replace([float('inf'), -float('inf')], pd.NA).fillna(
            new_data_processed.mean())

        return self.model.predict(new_data_processed)

    def save_model(self, path="model.joblib"):
        """Guarda el modelo entrenado en un archivo."""
        if self.model:
            joblib.dump(self.model, path)
            print(f"Modelo guardado en: {path}")
        else:
            print("No hay modelo para guardar.")

    def load_model(self, path="model.joblib"):
        """Carga un modelo desde un archivo."""
        try:
            self.model = joblib.load(path)
            print(f"Modelo cargado desde: {path}")
        except FileNotFoundError:
            print(f"Error: Archivo de modelo no encontrado en {path}")
        except Exception as e:
            print(f"Error al cargar el modelo: {e}")

    def _categorize_air_quality(self, pm2_5_values):
        """
        Función auxiliar para categorizar la calidad del aire basada en PM2.5.
        Puedes ajustar los umbrales según las normativas de Costa Rica o estándares internacionales.
        Ejemplo de umbrales (referencia: EPA US AQI para PM2.5):
        Buena: 0-12.0
        Moderada: 12.1-35.4
        Mala para grupos sensibles: 35.5-55.4
        Muy Mala: 55.5-150.4
        Peligrosa: >150.4
        """
        bins = [0, 12.0, 35.4, 55.4, 150.4, float('inf')]
        labels = ['Buena', 'Moderada', 'Mala', 'Muy Mala', 'Peligrosa']
        return pd.cut(pm2_5_values, bins=bins, labels=labels, right=True, include_lowest=True)

    def convert_to_classification_target(self, pm2_5_column='pm2_5'):
        """
        Convierte la columna de PM2.5 a categorías de calidad del aire para clasificación.
        Debe llamarse antes de prepare_data si el tipo_modelo es 'clasificacion'.
        """
        if pm2_5_column not in self.df.columns:
            print(f"Error: La columna '{pm2_5_column}' no se encuentra en el DataFrame.")
            return False

        print(f"Convirtiendo '{pm2_5_column}' a categorías de calidad del aire...")
        self.df['Calidad_Aire_Categoria'] = self._categorize_air_quality(self.df[pm2_5_column])
        self.target_column = 'Calidad_Aire_Categoria'
        print(f"Columna objetivo cambiada a '{self.target_column}'.")
        return True

