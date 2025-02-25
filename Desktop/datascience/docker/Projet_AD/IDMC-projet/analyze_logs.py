'''
from pyspark import SparkContext
import re
from datetime import datetime

# Initialiser le contexte Spark
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# Charger les logs depuis HDFS
log_file = "hdfs://hdfs-namenode:9000/logs/web_server.log"
logs_rdd = sc.textFile(log_file)

# Afficher la date et l'utilisateur actuels
print("\n===========================================================================")
print(f"Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-02-10 07:05:13")
print("\n===========================================================================")

print(f"Current User's Login: FETHl")
print("\n===========================================================================")

print("\n")

# Définir le pattern regex pour parser les logs avec plus d'informations
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*?" (\d+) (\d+) ".*?" "(.*?)" (\d+)'

def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        # Extraire l'ID du produit de l'URL
        url = match.group(4)
        product_id = re.search(r'/products/(\d+)', url)
        product_id = product_id.group(1) if product_id else None
        
        # Extraire la catégorie du produit de l'URL
        category = re.search(r'/categories/(\w+)/', url)
        category = category.group(1) if category else "unknown"
        
        return {
            'ip': match.group(1),
            'timestamp': datetime.strptime(match.group(2), '%d/%b/%Y:%H:%M:%S %z'),
            'method': match.group(3),
            'url': url,
            'status': int(match.group(5)),
            'response_size': int(match.group(6)),
            'product_id': product_id,
            'category': category
        }
    return None

# Parser les logs
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)

# 1. Produits les plus consultés
def analyze_most_viewed_products(rdd, start_date, end_date):
    filtered_logs = rdd.filter(lambda x: start_date <= x['timestamp'] <= end_date and x['product_id'] is not None)
    product_views = filtered_logs.map(lambda x: (x['product_id'], 1)).reduceByKey(lambda a, b: a + b)
    top_products = product_views.takeOrdered(10, key=lambda x: -x[1])
    print("\n===========================================================================")
    print("\n================ Produits les plus consultés ==============================")
    print("\n===========================================================================")

    for product_id, views in top_products:
        print("\n===========================================================================")
        print(f"Produit ID: {product_id}, Nombre de vues: {views}")
        print("\n===========================================================================")


# 2. Répartition des codes HTTP
def analyze_http_codes(rdd):
    http_codes = rdd.map(lambda x: (x['status'], 1)).reduceByKey(lambda a, b: a + b)
    codes_distribution = http_codes.collect()
    print("\n==========================================================================")
    print("\n=================== Répartition des codes HTTP ===========================")
    print("\n===========================================================================")

    for code, count in codes_distribution:
        print("\n===========================================================================")
        print(f"Code HTTP {code}: {count} requêtes")
        print("\n===========================================================================")


# 3. Adresses IP les plus actives
def analyze_top_ip_addresses(rdd):
    ip_counts = rdd.map(lambda x: (x['ip'], 1)).reduceByKey(lambda a, b: a + b)
    top_ips = ip_counts.takeOrdered(10, key=lambda x: -x[1])
    print("\n===========================================================================")
    print("\n=== Top 10 des adresses IP les plus actives ===")
    print("\n===========================================================================")

    for ip, count in top_ips:
        print("\n===========================================================================")
        print(f"IP: {ip}, Nombre de requêtes: {count}")
        print("\n===========================================================================")


# 4. Temps de réponse moyen par catégorie
def analyze_response_size_by_category(rdd):
    category_sizes = rdd.map(lambda x: (x['category'], (x['response_size'], 1)))\
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
                       .mapValues(lambda x: x[0] / x[1])
    print("\n===========================================================================")
    print("\n=== Taille moyenne des réponses par catégorie ===")
    print("\n===========================================================================")

    for category, avg_size in category_sizes.collect():
        print("\n===========================================================================")
        print(f"Catégorie: {category}, Taille moyenne: {avg_size:.2f} bytes")
        print("\n===========================================================================")


# Exécuter toutes les analyses
print("\n===========================================================================")
print("\n====================== Début des analyses ===============================")
print("\n===========================================================================")


# Définir la période pour l'analyse des produits les plus consultés
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 2, 10)

analyze_most_viewed_products(parsed_logs_rdd, start_date, end_date)
analyze_http_codes(parsed_logs_rdd)
analyze_top_ip_addresses(parsed_logs_rdd)
analyze_response_size_by_category(parsed_logs_rdd)

# Statistiques générales originales
total_requests = parsed_logs_rdd.count()
print(f"\n=== Statistiques générales ===")
print("===========================================================================")
print(f"Nombre total de requêtes : {total_requests}")
print("===========================================================================")


# Top 5 des URLs les plus demandées
top_urls = parsed_logs_rdd.map(lambda x: (x['url'], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5, key=lambda x: -x[1])
print("\n===========================================================================")
print("\n================ Les 5 URLs les plus demandées ============================")
print("\n===========================================================================")

for url, count in top_urls:
    print(f"{url}: {count}")

# Nombre de requêtes par méthode HTTP
http_methods = parsed_logs_rdd.map(lambda x: (x['method'], 1)).reduceByKey(lambda a, b: a + b).collect()
print("\n===========================================================================")
print("\n============= Nombre de requêtes par méthode HTTP =========================")
print("\n===========================================================================")

for method, count in http_methods:
    print("\n===========================================================================")
    print(f"Méthode {method}: {count}")
    print("\n===========================================================================")


# Fermer le contexte Spark
sc.stop()

'''


'''
from pyspark import SparkContext
import re

# Initialiser le contexte Spark
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# Charger les logs depuis HDFS
log_file = "hdfs://namenode:9000/logs/web_server.log"
logs_rdd = sc.textFile(log_file)

# APicher les 10 premières lignes
print("Exemple de lignes du fichier de logs :")
for line in logs_rdd.take(10):
    print(line)

# Définir le pattern regex pour parser les logs
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'
# Fonction pour parser une ligne de log
def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return (match.group(1), # IP
                match.group(2), # Timestamp
                match.group(3), # HTTP Method
                match.group(4), # URL
                int(match.group(5)), # HTTP Status
                int(match.group(6)) # Response Size
                )
    else:
        return None
# Parser les logs
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)
# APicher 10 logs parsés
print("Exemple de logs parsés :")
for log in parsed_logs_rdd.take(10):
    print(log)

# nombre total des requetes
total_requests = parsed_logs_rdd.count()
print(f"Nombre total de requêtes : {total_requests}")

# Top 5 des URLs les plus demandées
top_urls = parsed_logs_rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5,key=lambda x: -x[1])
print("+--------------------------+")
print("Les 5 URLs les plus demandées :")
print("+--------------------------+")

for url, count in top_urls:
    print(f"{url}: {count}")
print("+--------------------------+")

# Top 3 des URLs les moins demandées
top_urls = parsed_logs_rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(3,key=lambda x: x[1])
print("Les 3 URLs les moins demandées :")
for url, count in top_urls:
    print(f"{url}: {count}")


# Nombre total des requetes
N_requet = parsed_logs_rdd.count()
print("+-----------------------------------+")
print("Nombre total des requetes:", N_requet)
print("+-----------------------------------+")




# Nombre de requetes par methode http
nbre_requets = parsed_logs_rdd.map(lambda x: (x[2], 1)).reduceByKey(lambda a, b: a + b).collect()

print("+--------------------------------------------+")
for url, count in nbre_requets:
    print(f" Nombre de requetes par method {url}: {count}")
print("+--------------------------------------------+")


# CAlcul la taille moyenne tes reponses 
moy_t_reponse = parsed_logs_rdd.map(lambda x: (x[5])).mean()
print("+--------------+++++++++++++++++++++++------------+")
print("la taille moyenne tes reponses  :",moy_t_reponse)
print("+-----------+++++++++++++++++++++++---------------+")

# Adresse IP la plus active 
moy_t_reponse = parsed_logs_rdd.map(lambda x: (x[0])).reduceByKey(lambda a, b: a + b).takeordered(1,key=lambda x: -x[1])
print("+--------------+++++++++++++++++++++++------------+")
for url, count in moy_t_reponse:
    print(f" Adresse IP la plus active {url}: {count}")
print("+-----------+++++++++++++++++++++++---------------+")



# Hadoop

import os
import tempfile

output_format_class = "org.apache.hadoop.mapred.TextOutputFormat"
input_format_class = "org.apache.hadoop.mapred.TextInputFormat"
key_class = "org.apache.hadoop.io.IntWritable"
value_class = "org.apache.hadoop.io.Text"

with tempfile.TemporaryDirectory() as d:
    path = os.path.join(d, "old_hadoop_file")

    # Write a temporary Hadoop file
    rdd = sc.parallelize([(1, ""), (1, "a"), (3, "x")])
    rdd.saveAsHadoopFile(path, output_format_class, key_class, value_class)

    # Load this Hadoop file as an RDD
    loaded = sc.hadoopFile(path, input_format_class, key_class, value_class)
    sorted(loaded.collect())

'''





from pyspark import SparkContext
import re
from datetime import datetime
from collections import defaultdict

print("\n===========================================================================")
print("Current User's Login: Mereym_FETHl\n")
print("\n===========================================================================")


# Initialiser le contexte Spark
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# Charger les logs depuis HDFS
log_file = "hdfs://hdfs-namenode:9000/logs/web_server.log"
logs_rdd = sc.textFile(log_file)

# Définir les catégories de produits
CATEGORIES = {
    'maquillage': ['lipstick', 'foundation', 'mascara', 'eyeliner'],
    'soins_peau': ['skincare/cream', 'skincare/sunscreen'],
    'soins_capillaires': ['hair/shampoo', 'hair/conditioner']
}

def get_product_category(url):
    for category, products in CATEGORIES.items():
        if any(product in url for product in products):
            return category
    return "autre"

def get_product_id(url):
    id_match = re.search(r'id=(\d+)', url)
    return id_match.group(1) if id_match else None

# Pattern regex amélioré pour parser les logs
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP/.*?" (\d+) (\d+)'

def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        ip = match.group(1)
        timestamp = match.group(2)
        method = match.group(3)
        url = match.group(4)
        status = int(match.group(5))
        response_size = int(match.group(6))
        
        return {
            'ip': ip,
            'timestamp': datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S +0000'),
            'method': method,
            'url': url,
            'status': status,
            'response_size': response_size,
            'product_id': get_product_id(url),
            'category': get_product_category(url)
        }
    return None

# Parser les logs
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)

# 1. Produits les plus consultés
print("\n===========================================================================")
print("\n=== Produits les plus consultés ===")
print("\n===========================================================================")

product_views = parsed_logs_rdd.filter(lambda x: x['product_id'] is not None)\
                              .map(lambda x: (x['product_id'], 1))\
                              .reduceByKey(lambda a, b: a + b)\
                              .collect()

for product_id, count in product_views:
    print(f"Produit ID {product_id} : {count} vues")
    print("\n===========================================================================")


# 2. Répartition des codes HTTP
print("\n===========================================================================")
print("\n=== Répartition des codes HTTP ===")
print("\n===========================================================================")

http_codes = parsed_logs_rdd.map(lambda x: (x['status'], 1))\
                           .reduceByKey(lambda a, b: a + b)\
                           .collect()

for code, count in http_codes:
    status_message = {
        200: "Succès",
        301: "Redirection",
        403: "Accès refusé",
        404: "Non trouvé",
        500: "Erreur serveur"
    }.get(code, "Autre")
    print(f"Code {code} ({status_message}): {count} requêtes")
    print("\n===========================================================================")

# 3. Top 10 adresses IP les plus actives
print("\n===========================================================================")
print("\n=== Top 10 adresses IP les plus actives ===")
print("\n===========================================================================")

top_ips = parsed_logs_rdd.map(lambda x: (x['ip'], 1))\
                        .reduceByKey(lambda a, b: a + b)\
                        .sortBy(lambda x: -x[1])\
                        .take(10)

for ip, count in top_ips:
    print(f"IP {ip}: {count} requêtes")
    print("\n===========================================================================")


# 4. Temps de réponse moyen par catégorie
print("\n===========================================================================")
print("\n=== Taille moyenne des réponses par catégorie ===")
print("\n===========================================================================")
category_sizes = parsed_logs_rdd.map(lambda x: (x['category'], (x['response_size'], 1)))\
                               .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
                               .mapValues(lambda x: x[0] / x[1])\
                               .collect()

for category, avg_size in category_sizes:
    print(f"Catégorie {category}: {avg_size:.2f} octets en moyenne")
    print("\n===========================================================================")


# Statistiques supplémentaires
print("\n===========================================================================")
print("\n=== Statistiques supplémentaires ===")
print("\n===========================================================================")


# Nombre total de requêtes
total_requests = parsed_logs_rdd.count()
print("\n===========================================================================")
print(f"Nombre total de requêtes: {total_requests}")
print("\n===========================================================================")


# Répartition des méthodes HTTP
http_methods = parsed_logs_rdd.map(lambda x: (x['method'], 1))\
                             .reduceByKey(lambda a, b: a + b)\
                             .collect()
print("\n===========================================================================")
print("\nRépartition des méthodes HTTP:")
print("\n===========================================================================")
for method, count in http_methods:
    print("\n===========================================================================")
    print(f"Méthode {method}: {count} requêtes")


# Afficher les URLs qui ont généré des erreurs (status >= 400)
print("\n===========================================================================")
print("\nURLs ayant généré des erreurs:")
print("\n===========================================================================")

error_urls = parsed_logs_rdd.filter(lambda x: x['status'] >= 400)\
                           .map(lambda x: (x['url'], x['status']))\
                           .collect().take(10)
for url, status in error_urls:
    print(f"URL: {url}, Code d'erreur: {status}, Catégorie: {get_product_category(url)}")
    print("\n===========================================================================")

sc.stop()