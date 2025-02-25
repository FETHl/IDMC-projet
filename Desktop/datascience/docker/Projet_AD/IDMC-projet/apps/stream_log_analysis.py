from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, split, window

# Initialisation de Spark Session
spark = SparkSession.builder \
    .appName("LogAnalysis") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27018/logs_db.results") \
    .getOrCreate()

# Chargement des logs depuis HDFS
logs_df = spark.read.text("hdfs://hdfs-namenode:9000/logs/access.log")

# Parsing des logs
logs_df = logs_df.select(
    split(col("value"), " ")[0].alias("ip"),
    split(col("value"), " ")[3].alias("timestamp"),
    split(col("value"), " ")[5].alias("method"),
    split(col("value"), " ")[6].alias("url"),
    split(col("value"), " ")[8].alias("status_code"),
    split(col("value"), " ")[9].alias("size")
)

# Produits les plus consultés
top_products = logs_df.filter(col("url").like("/products/%")) \
                      .groupBy("url") \
                      .agg(count("*").alias("count")) \
                      .orderBy(col("count").desc())

# Temps de réponse moyen par produit
time_response_avg = logs_df.filter(col("size").isNotNull()) \
                          .groupBy("url") \
                          .agg(avg(col("size")).alias("avg_response_time"))

# Détection des heures de pointe
peak_hours = logs_df.withColumn("hour", split(col("timestamp"), ":")[1]) \
                    .groupBy("hour") \
                    .agg(count("*").alias("count")) \
                    .orderBy(col("count").desc())

# Sauvegarde dans MongoDB
top_products.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "top_products").save()
time_response_avg.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "response_time").save()
peak_hours.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "peak_hours").save()

# STREAMING - Détection en temps réel
logs_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_stream_df = logs_stream_df.selectExpr("CAST(value AS STRING) as log")

logs_stream_df = logs_stream_df.select(
    split(col("log"), " ")[0].alias("ip"),
    split(col("log"), " ")[3].alias("timestamp"),
    split(col("log"), " ")[5].alias("method"),
    split(col("log"), " ")[6].alias("url"),
    split(col("log"), " ")[8].alias("status_code"),
    split(col("log"), " ")[9].alias("size")
)

# Détection des erreurs (404, 500)
errors_stream = logs_stream_df.filter(col("status_code").isin("404", "500")) \
                              .groupBy(window(col("timestamp"), "5 minutes"), "status_code") \
                              .agg(count("*").alias("count"))

# Détection de pics d'activité
peak_activity = logs_stream_df.groupBy(window(col("timestamp"), "1 minute")) \
                              .agg(count("*").alias("count")) \
                              .filter(col("count") > 100)

# Surveillance de la taille des réponses (temps de réponse élevé)
high_response_time = logs_stream_df.filter(col("size").cast("int") > 5000) \
                                  .groupBy(window(col("timestamp"), "1 minute"), "url") \
                                  .agg(count("*").alias("count"))

# Écriture en MongoDB
query_errors = errors_stream.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "errors_stream") \
    .outputMode("update") \
    .start()

query_peak = peak_activity.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "peak_activity") \
    .outputMode("update") \
    .start()

query_high_response_time = high_response_time.writeStream \
    .format("mongo") \
    .option("database", "logs_db") \
    .option("collection", "high_response_time") \
    .outputMode("update") \
    .start()

query_errors.awaitTermination()
query_peak.awaitTermination()
query_high_response_time.awaitTermination()
