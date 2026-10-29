from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Monitoreo_Transacciones_Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaccion_id", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("medio_pago", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones_topic") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

df_metrics = df_parsed.groupBy("ciudad").agg(
    count("transaccion_id").alias("total_transacciones"),
    _sum("monto").alias("monto_acumulado"),
    avg("monto").alias("monto_promedio")
)

query = df_metrics.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()