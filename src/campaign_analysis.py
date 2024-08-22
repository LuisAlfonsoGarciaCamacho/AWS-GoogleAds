from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, sum, round, current_date, date_sub
import pyspark.sql.functions as F
import os

# Configurar variables de entorno para Redshift (solo para pruebas)
def set_redshift_env_vars():
    os.environ["REDSHIFT_USER"] = "admin"
    os.environ["REDSHIFT_PASSWORD"] = "Fany1711"

# Llamar a la función para configurar las variables de entorno antes de iniciar Spark
set_redshift_env_vars()
# Configuración de Redshift
redshift_url = "jdbc:redshift://default-workgroup.637423291933.us-west-1.redshift-serverless.amazonaws.com:5439/dev"
redshift_properties = {
    "user": os.environ.get("REDSHIFT_USER"),
    "password": os.environ.get("REDSHIFT_PASSWORD"),
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Iniciar una sesión Spark
spark = SparkSession.builder \
    .appName("Google Ads Analysis") \
    .config("spark.jars", "../redshift-jdbc.jar") \
    .getOrCreate()

def get_google_ads_data():
    # Cargar los datos desde Redshift
    df = spark.read \
        .jdbc(url=redshift_url, table="campaigns", properties=redshift_properties)

    # Filtrar los últimos 30 días
    df = df.filter(col("date") >= date_sub(current_date(), 30))

    # Eliminar duplicados y rellenar valores nulos
    df = df.dropDuplicates().fillna(0)

    # Agrupar y calcular métricas
    df_aggregated = df.groupBy("id", "name", "country", "region", "city", "age_group", "gender", "device_type", "campaign_type", "ad_group", "keyword") \
        .agg(
            sum("impressions").alias("impressions"),
            sum("clicks").alias("clicks"),
            sum("conversions").alias("conversions"),
            sum("cost").alias("cost")
        )

    return df_aggregated

def analyze_campaign_performance(df):
    # Calcular métricas adicionales
    df = df.withColumn("CTR", (col("clicks") / col("impressions")) * 100)
    df = df.withColumn("conversion_rate", (col("conversions") / col("clicks")) * 100)
    df = df.withColumn("CPC", col("cost") / col("clicks"))

    # Lidiar con divisiones por cero
    df = df.withColumn("CTR", when(col("CTR").isNull() | (col("CTR") == float("inf")), 0).otherwise(col("CTR")))
    df = df.withColumn("conversion_rate", when(col("conversion_rate").isNull() | (col("conversion_rate") == float("inf")), 0).otherwise(col("conversion_rate")))
    df = df.withColumn("CPC", when(col("CPC").isNull() | (col("CPC") == float("inf")), 0).otherwise(col("CPC")))

    # Redondear los valores para mejor legibilidad
    df = df.select(
        "id", "name", "country", "region", "city", "age_group", "gender", "device_type", "campaign_type", "ad_group", "keyword",
        col("impressions").cast("int"),
        col("clicks").cast("int"),
        round("conversions", 2).alias("conversions"),
        round("cost", 2).alias("cost"),
        round("CTR", 2).alias("CTR"),
        round("conversion_rate", 2).alias("conversion_rate"),
        round("CPC", 2).alias("CPC")
    )

    return df

def detect_anomalies(df):
    # Calcular medias para detectar anomalías
    cpc_mean = df.select(mean(col("CPC"))).collect()[0][0]
    ctr_mean = df.select(mean(col("CTR"))).collect()[0][0]
    
    # Detectar anomalías
    anomalous_data = df.filter(
        (col("CPC") > 3 * cpc_mean) | 
        (col("CTR") > 3 * ctr_mean) | 
        (col("conversion_rate") > 100)  # Tasa de conversión mayor al 100% es sospechosa
    )

    return anomalous_data

def save_results(df, anomalous_data):
    # Guardar los resultados en Redshift
    df.write \
        .jdbc(url=redshift_url, table="campaign_analytics", mode="overwrite", properties=redshift_properties)

    if anomalous_data.count() > 0:
        anomalous_data.write \
            .jdbc(url=redshift_url, table="anomalous_campaigns", mode="overwrite", properties=redshift_properties)

def run_campaign_analysis():
    try:
        # Obtener y analizar los datos
        df = get_google_ads_data()
        df_analyzed = analyze_campaign_performance(df)
        
        # Detectar anomalías
        anomalous_data = detect_anomalies(df_analyzed)
        
        # Guardar resultados
        save_results(df_analyzed, anomalous_data)
        
        # Mostrar algunos resultados
        print("Resumen de campañas:")
        df_analyzed.show(5, truncate=False)
        
        if anomalous_data.count() > 0:
            print("\nAnomalías detectadas:")
            anomalous_data.show(5, truncate=False)
        else:
            print("\nNo se detectaron anomalías.")

    except Exception as e:
        print(f"Error en el análisis de campañas: {str(e)}")
    finally:
        # Cerrar la sesión de Spark
        spark.stop()

if __name__ == "__main__":
    run_campaign_analysis()