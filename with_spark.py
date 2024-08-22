from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean

# Iniciar una sesión Spark
spark = SparkSession.builder \
    .appName("Google Ads Analysis") \
    .config("spark.jars", "sqlite-jdbc-3.46.0.1.jar") \
    .getOrCreate()

# Cargar los datos desde SQLite usando JDBC
db_url = "jdbc:sqlite:fake_google_ads.db"
df = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "campaigns") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# Eliminar duplicados
df = df.dropDuplicates()

# Rellenar valores nulos
df = df.fillna(0)

# Calcular Costo por Clic (CPC) y Tasa de Conversión
df = df.withColumn("CPC", col("cost") / col("clicks"))
df = df.withColumn("conversion_rate", col("conversions") / col("clicks"))

# Lidiar con divisiones por cero
df = df.withColumn("CPC", when(col("CPC").isNull() | (col("CPC") == float("inf")), 0).otherwise(col("CPC")))
df = df.withColumn("conversion_rate", when(col("conversion_rate").isNull() | (col("conversion_rate") == float("inf")), 0).otherwise(col("conversion_rate")))

# Detectar anomalías en las métricas calculadas
cpc_mean = df.select(mean(col("CPC"))).collect()[0][0]
anomalous_data = df.filter((col("CPC") > 3 * cpc_mean) | (col("conversion_rate") > 1))

if anomalous_data.count() > 0:
    print("Anomalías detectadas en los datos:")
    anomalous_data.show()

# Guardar los resultados en una nueva base de datos SQLite
db_url_out = "jdbc:sqlite:analytics.db"
df.write \
    .format("jdbc") \
    .option("url", db_url_out) \
    .option("dbtable", "analytics") \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

# Cerrar la sesión de Spark
spark.stop()
