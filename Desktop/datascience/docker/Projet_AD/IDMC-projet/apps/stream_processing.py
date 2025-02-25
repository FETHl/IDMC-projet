from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window

# Initialisation de Spark Streaming
spark = SparkSession.builder \
    .appName("StreamLogAnalysis") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27018/logs_db.stream_results") \
    .getOrCreate()

# Lecture des logs depuis Kafka
logs_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

# Décodage des messages Kafka
logs_stream_df = logs_stream_df.selectExpr("CAST(value AS STRING) as log")

# Parsing des logs
logs_stream_df = logs_stream_df.select(
    split(col("log"), " ")[0].alias("ip"),
    split(col("log"), " ")[3].alias("timestamp"),
    split(col("log"), " ")[5].alias("method"),
    split(col("log"), " ")[6].alias("url"),
    split(col("log"), " ")[8].alias("status_code"),
    split(col("log"), " ")[9].alias("size")
)

# Détection des erreurs (codes 404 et 500) en temps réel
errors_stream = logs_stream_df.filter(col("status_code").isin("404", "500")) \
                              .groupBy(window(col("timestamp"), "5 minutes"), "status_code") \
                              .agg(count("*").alias("count"))

# Produits en tendance (plus de 20 requêtes en 1 min)
trending_products = logs_stream_df.filter(col("url").like("/products/%")) \
                                  .groupBy(window(col("timestamp"), "1 minute"), "url") \
                                  .agg(count("*").alias("count")) \
                                  .filter(col("count") > 20)

# Surveillance des IP suspectes (plus de 50 requêtes en 5 min)
suspicious_ips = logs_stream_df.groupBy(window(col("timestamp"), "5 minutes"), "ip") \
                               .agg(count("*").alias("count")) \
                               .filter(col("count") > 50)

# Écriture des résultats dans MongoDB
query_errors = errors_stream.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "errors_stream") \
    .outputMode("update") \
    .start()

query_trending = trending_products.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "trending_products") \
    .outputMode("update") \
    .start()

query_suspicious_ips = suspicious_ips.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "suspicious_ips") \
    .outputMode("update") \
    .start()

query_errors.awaitTermination()
query_trending.awaitTermination()
query_suspicious_ips.awaitTermination()
