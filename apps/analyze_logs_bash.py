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