from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count
from pyspark.sql.types import StructType, StringType, IntegerType

# Initialisation de Spark Session
spark = SparkSession.builder \
    .appName("Streaming Log Analysis") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
    .getOrCreate()

# Définition du schéma des logs
schema = StructType() \
    .add("ip", StringType()) \
    .add("timestamp", StringType()) \
    .add("method", StringType()) \
    .add("url", StringType()) \
    .add("status", IntegerType())

# Lecture des logs depuis Kafka
logs = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weblogs") \
    .load()

# Conversion en DataFrame structuré
logs_df = logs.selectExpr("CAST(value AS STRING)") \
    .selectExpr("split(value, ' ')[0] as ip",
               "split(value, ' ')[3] as timestamp",
               "split(value, ' ')[5] as method",
               "split(value, ' ')[6] as url",
               "CAST(split(value, ' ')[8] AS INT) as status")

# Détection des pics d'erreurs (404 et 500)
error_logs = logs_df.filter(col("status").isin([404, 500]))
error_counts = error_logs \
    .groupBy(window(col("timestamp"), "5 minutes"), "status") \
    .count()

# Détection des produits en tendance
popular_products = logs_df.filter(col("url").contains("/products/"))
popular_counts = popular_products \
    .groupBy(window(col("timestamp"), "1 minute"), col("url")) \
    .count().filter(col("count") > 20)

# Surveillance des adresses IP suspectes
ip_activity = logs_df.groupBy(window(col("timestamp"), "1 minute"), "ip").count()
suspicious_ips = ip_activity.filter(col("count") > 100)  # Seuil suspect

# Écriture des résultats en console (peut être remplacé par MongoDB)
query1 = error_counts.writeStream.outputMode("update").format("console").start()
query2 = popular_counts.writeStream.outputMode("update").format("console").start()
query3 = suspicious_ips.writeStream.outputMode("update").format("console").start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
