from pyspark import SparkContext
import re
from datetime import datetime
import os
import json
import sys
import subprocess

# Vérifier et installer pymongo si nécessaire
try:
    import pymongo
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])
    import pymongo

# Initialiser Spark
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# Informations utilisateur et date
current_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
current_user = "MERYAVI"
print(f"\n=== Analyse des logs - {current_date} ===\nUtilisateur: {current_user}\n")

# Configuration MongoDB
mongo_uri = "mongodb://localhost:27018/"
mongo_db = "log_analysis"
use_mongodb = False

try:
    from pymongo import MongoClient
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.server_info()
    db = client[mongo_db]
    collections = {
        "products": db["products_views"],
        "http_codes": db["http_codes"],
        "ip_addresses": db["ip_addresses"],
        "categories": db["categories_response"],
        "stats": db["general_stats"],
    }
    for col in collections.values():
        col.delete_many({})  # Nettoyer les anciennes données
    use_mongodb = True
    print("Connexion MongoDB réussie")
except Exception as e:
    print(f"Connexion MongoDB échouée: {e}")

# Charger les logs depuis HDFS
log_file = "hdfs://hdfs-namenode:9000/logs/web_server.log"
logs_rdd = sc.textFile(log_file)

# Regex pour parser les logs
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*?" (\d+) (\d+)'

def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return {
            'ip': match.group(1),
            'timestamp': match.group(2),
            'method': match.group(3),
            'url': match.group(4),
            'status': int(match.group(5)),
            'response_size': int(match.group(6)),
            'product_id': re.search(r'/products/(\d+)', match.group(4)) or None,
            'category': re.search(r'/categories/(\w+)', match.group(4)) or "unknown",
        }
    return None

parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)

def save_to_mongodb(collection, data, analysis_type):
    if use_mongodb and data:
        try:
            collection.insert_many([{**item, "analysis_type": analysis_type, "analyzed_by": current_user, "analysis_date": current_date} for item in data])
            print(f"Données sauvegardées: {analysis_type}")
        except Exception as e:
            print(f"Erreur MongoDB ({analysis_type}): {e}")

# Analyser les produits les plus consultés
def analyze_most_viewed_products(rdd):
    top_products = (rdd.filter(lambda x: x['product_id'])
                      .map(lambda x: (x['product_id'], 1))
                      .reduceByKey(lambda a, b: a + b)
                      .takeOrdered(10, key=lambda x: -x[1]))
    save_to_mongodb(collections["products"], [{"product_id": p, "views": v} for p, v in top_products], "most_viewed_products")
    return top_products

# Analyser la répartition des codes HTTP
def analyze_http_codes(rdd):
    codes = rdd.map(lambda x: (x['status'], 1)).reduceByKey(lambda a, b: a + b).collect()
    save_to_mongodb(collections["http_codes"], [{"status_code": c, "count": n} for c, n in codes], "http_status_codes")
    return codes

# Analyser les IP les plus actives
def analyze_top_ip_addresses(rdd):
    top_ips = (rdd.map(lambda x: (x['ip'], 1))
                 .reduceByKey(lambda a, b: a + b)
                 .takeOrdered(10, key=lambda x: -x[1]))
    save_to_mongodb(collections["ip_addresses"], [{"ip": ip, "count": cnt} for ip, cnt in top_ips], "top_active_ips")
    return top_ips

# Taille moyenne des réponses par catégorie
def analyze_response_size_by_category(rdd):
    avg_size = (rdd.map(lambda x: (x['category'], (x['response_size'], 1)))
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                  .mapValues(lambda x: x[0] / x[1] if x[1] > 0 else 0)
                  .collect())
    save_to_mongodb(collections["categories"], [{"category": c, "avg_size": s} for c, s in avg_size], "category_response_sizes")
    return avg_size

# Exécuter les analyses
product_results = analyze_most_viewed_products(parsed_logs_rdd)
http_results = analyze_http_codes(parsed_logs_rdd)
ip_results = analyze_top_ip_addresses(parsed_logs_rdd)
category_results = analyze_response_size_by_category(parsed_logs_rdd)

total_requests = parsed_logs_rdd.count()
print(f"Nombre total de requêtes: {total_requests}")

general_stats = {
    "total_requests": total_requests,
    "top_urls": parsed_logs_rdd.map(lambda x: (x['url'], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5, key=lambda x: -x[1]),
    "http_methods": parsed_logs_rdd.map(lambda x: (x['method'], 1)).reduceByKey(lambda a, b: a + b).collect(),
    "analyzed_by": current_user,
    "analysis_date": current_date,
}
if use_mongodb:
    collections["stats"].insert_one(general_stats)

# Fermer les connexions
if use_mongodb:
    client.close()
sc.stop()
