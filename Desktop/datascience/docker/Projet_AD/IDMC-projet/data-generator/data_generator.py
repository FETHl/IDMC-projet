import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers="kafka:9092")

# Listes de valeurs aléatoires
ips = ["192.168.1.1", "192.168.1.2", "192.168.1.3", "10.0.0.1", "10.0.0.2"]
methods = ["GET", "POST", "PUT", "DELETE"]
pages = ["/", "/home", "/login", "/register", "/dashboard", "/profile"]
statuses = [200, 201, 400, 403, 404, 500, 502, 503]
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Mobile/15E148",
]

def generate_log():
    """Génère un log Apache simulé"""
    ip = random.choice(ips)
    timestamp = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
    method = random.choice(methods)
    page = random.choice(pages)
    status = random.choice(statuses)
    response_size = random.randint(200, 5000)
    user_agent = random.choice(user_agents)

    log = f'{ip} - - [{timestamp}] "{method} {page} HTTP/1.1" {status} {response_size} "{user_agent}"'
    return log

# Boucle infinie pour générer les logs
while True:
    log = generate_log()
    producer.send("logs", log.encode("utf-8"))
    print(f"Sent log: {log}")
    time.sleep(random.uniform(0.5, 2.0))  # Envoi des logs avec un délai aléatoire entre 0.5s et 2s
