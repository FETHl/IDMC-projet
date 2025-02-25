from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, split

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("BatchLogAnalysis") \
    .config("spark.mongodb.output.uri", "mongodb://mongo:27018/logs_db.results") \
    .getOrCreate()

# Chargement des logs depuis HDFS
logs_df = spark.read.text("hdfs://hdfs-namenode:9000/logs/access.log")

# Parsing des logs (Assume le format standard)
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

# Répartition des codes HTTP
http_status_distribution = logs_df.groupBy("status_code").agg(count("*").alias("count"))

# IP les plus actives
top_ips = logs_df.groupBy("ip").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10)

# Sauvegarde dans MongoDB
top_products.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "top_products").save()
http_status_distribution.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "http_status").save()
top_ips.write.format("mongo").mode("overwrite").option("database", "logs_db").option("collection", "top_ips").save()

spark.stop()
