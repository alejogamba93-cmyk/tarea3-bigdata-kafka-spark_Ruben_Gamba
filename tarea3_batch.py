from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, when

spark = SparkSession.builder \
    .appName("Analisis_Infracciones_Transito_Batch") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("infracciones.csv", header=True, inferSchema=True)

df_clean = df.filter(col("valor_multa").isNotNull()) \
    .withColumn("valor_multa", col("valor_multa").cast("double")) \
    .withColumn("categoria_gravedad", 
                when(col("valor_multa") >= 1000000, "Alta")
                .when(col("valor_multa") >= 400000, "Media")
                .otherwise("Baja"))

df_clean.createOrReplaceTempView("vista_infracciones")
query_sql = """
    SELECT tipo_vehiculo, 
           COUNT(infraccion_id) as cantidad_multas, 
           SUM(valor_multa) as recaudo_total,
           AVG(valor_multa) as promedio_multa
    FROM vista_infracciones
    GROUP BY tipo_vehiculo
    ORDER BY recaudo_total DESC
"""
spark.sql(query_sql).show(truncate=False)

rdd_vehiculos = df_clean.rdd.map(lambda row: (row["tipo_vehiculo"], 1)) \
    .reduceByKey(lambda a, b: a + b)

for vehiculo, total in rdd_vehiculos.collect():
    print(f"Tipo de Vehículo: {vehiculo:<15} | Total Infracciones: {total}")

spark.stop()