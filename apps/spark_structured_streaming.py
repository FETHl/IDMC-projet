from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialisation de la session Spark
spark = SparkSession.builder.appName("StructuredStreamingKafkaMongoDB").config("spark.mongodb.output.uri", "mongodb://mongo:27017/logs.status_counts").getOrCreate()

# Lecture des logs depuis Kafka
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "logs").load()

# Conversion des données en string
logs_df = kafka_df.selectExpr("CAST(value AS STRING)")

# **Extraction des champs en utilisant split()**
parsed_logs = logs_df.withColumn("log_parts", split(col("value"), " ")).select(
        col("log_parts")[0].alias("ip"),  # Adresse IP
        regexp_extract(col("value"), r'\[(.*?)\]', 1).alias("timestamp"),  # Extraire la date entre []
        regexp_extract(col("value"), r'"(\w+) ', 1).alias("method"),  # Extraire le verbe HTTP (GET, POST, etc.)
        regexp_extract(col("value"), r'"(?:\w+) (.*?) HTTP', 1).alias("url"),  # Extraire l'URL demandée
        regexp_extract(col("value"), r'HTTP/\d.\d"', 0).alias("protocol"),  # Extraire le protocole HTTP
        col("log_parts")[8].cast("int").alias("status"),  # Code HTTP
        col("log_parts")[9].cast("int").alias("size")  # Taille de la réponse
    )

# Agrégation des logs par code HTTP
status_counts = parsed_logs.groupBy("status").count()

# Fonction pour écrire dans MongoDB (sans écraser)
def write_to_mongo(df, epoch_id):
    df.write.format("mongo").mode("append").option("replaceDocument", "false").save()

# Écriture des résultats dans MongoDB en streaming
query = status_counts.writeStream.outputMode("update").foreachBatch(write_to_mongo).start()

query.awaitTermination()
